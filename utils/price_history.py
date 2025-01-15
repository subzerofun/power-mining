import os
import csv
import psycopg2
import argparse
import time
import random
from datetime import datetime, timedelta
from psycopg2.extras import DictCursor
import sys

# Add parent directory to path to import from utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.common import log_message, BLUE, RED, YELLOW, GREEN

# Debug levels
DEBUG_LEVEL = 1  # 0 = silent, 1 = critical/important, 2 = normal, 3 = verbose/detailed

class PriceHistoryManager:
    def __init__(self, db_in, db_history, randomize=False):
        self.db_in = db_in
        self.db_history = db_history
        self.commodity_map = self._load_commodity_map()
        self.randomize = randomize
        
        # Keep track of previous total DB size (in bytes) to calculate growth.
        self.prev_total_bytes = None

    def _load_commodity_map(self):
        """Load commodity mapping from CSV file"""
        commodity_map = {}
        try:
            with open('data/commodities_mining.csv', 'r') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader, 1):
                    commodity_map[row['name']] = i  # Use CSV id as SMALLINT
            return commodity_map
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to load commodity mapping: {e}", level=1)
            return None

    def check_table_exists(self):
        """Check if price history table exists"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_tables
                            WHERE schemaname = 'public'
                            AND tablename = 'price_history'
                        );
                    """)
                    return cur.fetchone()[0]
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to check table existence: {e}", level=1)
            return False

    def setup_database(self):
        """Initialize database schema in history database"""
        # Check if table already exists
        if self.check_table_exists():
            log_message(YELLOW, "SETUP", "Price history table already exists, skipping setup", level=1)
            return

        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    # Create commodity lookup table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS commodity_lookup (
                            id SMALLINT PRIMARY KEY,
                            name VARCHAR(50) UNIQUE
                        );
                    """)

                    # Use a PRIMARY KEY to allow ON CONFLICT DO NOTHING to skip exact duplicates
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS price_history (
                            timestamp TIMESTAMPTZ NOT NULL,
                            commodity_id SMALLINT,
                            station_id BIGINT,
                            price INTEGER,
                            demand INTEGER,
                            FOREIGN KEY (commodity_id) REFERENCES commodity_lookup(id),
                            CONSTRAINT pk_price_history PRIMARY KEY (timestamp, commodity_id, station_id)
                        );
                    """)

                    # Create hypertable with a 1-day chunk interval (so that once a day is past, it can compress)
                    cur.execute("""
                        SELECT create_hypertable('price_history', 'timestamp',
                            chunk_time_interval => INTERVAL '5 minutes',
                            if_not_exists => TRUE,
                            migrate_data => TRUE
                        );
                    """)

                    # Enable compression with the appropriate settings
                    cur.execute("""
                        ALTER TABLE price_history SET (
                            timescaledb.compress = true,
                            timescaledb.compress_segmentby = 'commodity_id, station_id',
                            timescaledb.compress_orderby = 'timestamp DESC'
                        );
                    """)

                    # Add a compression policy that compresses chunks older than 1 day
                    cur.execute("""
                        SELECT remove_compression_policy('price_history', if_exists => true);
                        SELECT add_compression_policy('price_history', 
                            compress_after => INTERVAL '5 minutes',
                            if_not_exists => true
                        );
                    """)

                    # Create or replace the snapshot function, without forcing immediate compression
                    cur.execute(f"""
                        CREATE OR REPLACE FUNCTION take_price_snapshot()
                        RETURNS INTEGER AS $$
                        DECLARE
                            rows_inserted INTEGER;
                            rand_percent FLOAT;
                        BEGIN
                            SELECT CASE WHEN EXISTS (
                                SELECT 1 
                                FROM pg_settings 
                                WHERE name = 'app.randomize_enabled' 
                                  AND setting = 'true'
                            ) THEN
                                5 + random() * 25  -- 5-30%
                            ELSE
                                0
                            END INTO rand_percent;

                            -- Create temp table with the CURRENT snapshot from db_in
                            CREATE TEMP TABLE temp_current_raw AS
                            SELECT 
                                NOW() as snapshot_ts,
                                cl.id as commodity_id,
                                sc.station_id,
                                sc.sell_price as price,
                                sc.demand as demand
                            FROM dblink('{self.db_in}',
                                'SELECT commodity_name, station_id, sell_price, demand 
                                 FROM station_commodities 
                                 WHERE sell_price > 0 
                                   AND demand >= 0'
                            ) AS sc(commodity_name VARCHAR, station_id BIGINT, sell_price INTEGER, demand INTEGER)
                            JOIN commodity_lookup cl ON cl.name = sc.commodity_name;

                            -- Randomize demands in a separate temp table
                            CREATE TEMP TABLE temp_current AS
                            SELECT 
                                snapshot_ts as timestamp,
                                commodity_id,
                                station_id,
                                price,
                                CASE 
                                  WHEN rand_percent > 0 AND random() < rand_percent/100 
                                       THEN floor(random() * 20001)::integer
                                  ELSE demand
                                END as demand
                            FROM temp_current_raw;

                            DROP TABLE temp_current_raw;

                            -- Identify only changed records by joining the last known values
                            WITH last_values AS (
                                SELECT DISTINCT ON (commodity_id, station_id)
                                    commodity_id, station_id, price, demand
                                FROM price_history
                                ORDER BY commodity_id, station_id, timestamp DESC
                            ), changes AS (
                                SELECT 
                                    tc.timestamp,
                                    tc.commodity_id,
                                    tc.station_id,
                                    tc.price,
                                    tc.demand
                                FROM temp_current tc
                                LEFT JOIN last_values lv ON 
                                    lv.commodity_id = tc.commodity_id AND 
                                    lv.station_id   = tc.station_id
                                WHERE 
                                    lv.commodity_id IS NULL
                                    OR lv.price != tc.price
                                    OR lv.demand != tc.demand
                            )
                            INSERT INTO price_history (timestamp, commodity_id, station_id, price, demand)
                            SELECT * FROM changes
                            ON CONFLICT DO NOTHING;

                            GET DIAGNOSTICS rows_inserted = ROW_COUNT;

                            DROP TABLE temp_current;
                            RETURN rows_inserted;
                        END;
                        $$ LANGUAGE plpgsql;
                    """)

                    # Enable randomization if requested
                    if self.randomize:
                        cur.execute("SELECT current_database();")
                        dbname = cur.fetchone()[0]
                        cur.execute(f"ALTER DATABASE {dbname} SET app.randomize_enabled TO true;")
                    else:
                        cur.execute("SELECT current_database();")
                        dbname = cur.fetchone()[0]
                        cur.execute(f"ALTER DATABASE {dbname} SET app.randomize_enabled TO false;")

                    # Create dblink extension for cross-database queries
                    cur.execute("CREATE EXTENSION IF NOT EXISTS dblink;")

                    # Create a job that runs take_price_snapshot() every hour
                    cur.execute("""
                        SELECT add_job(
                            'take_price_snapshot',
                            '1 hour',
                            initial_start => NOW()
                        );
                    """)

                    # Populate commodity lookup table
                    for name, id in self.commodity_map.items():
                        cur.execute("""
                            INSERT INTO commodity_lookup (id, name)
                            VALUES (%s, %s)
                            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
                        """, (id, name))

                    conn.commit()
                    log_message(GREEN, "SETUP", "History database schema initialized successfully", level=1)
                    
        except Exception as e:
            log_message(RED, "ERROR", f"Database setup failed: {e}", level=1)
            raise

    def get_current_job_interval(self):
        """Get current snapshot interval in minutes"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            schedule_interval,
                            next_start
                        FROM timescaledb_information.jobs
                        WHERE proc_name = 'take_price_snapshot'
                        LIMIT 1;
                    """)
                    result = cur.fetchone()
                    if result:
                        interval, next_start = result
                        minutes = None
                        # Modify get_current_job_interval() to handle e.g. '00:10:00'
                        if isinstance(interval, str):
                            # Check if it looks like "0:10:00" or "00:10:00"
                            # Timescale might store it as "00:10:00" or "0:10:00"
                            if ':' in interval:
                                # Convert "HH:MM:SS" to an integer minute count
                                hms = interval.split(':')
                                if len(hms) == 3:
                                    hours = int(hms[0])
                                    mins = int(hms[1])
                                    # We ignore seconds or parse them if needed
                                    minutes = hours * 60 + mins
                                else:
                                    minutes = None
                            elif 'hours' in interval:
                                # existing logic
                                minutes = int(interval.split()[0]) * 60
                            elif 'minutes' in interval:
                                minutes = int(interval.split()[0])
                            else:
                                minutes = None
                    return None
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get job interval: {e}", level=1)
            return None

    def update_job_interval(self, minutes):
        """Update snapshot job interval"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT job_id 
                        FROM timescaledb_information.jobs 
                        WHERE proc_name = 'take_price_snapshot'
                        LIMIT 1;
                    """)
                    job = cur.fetchone()
                    
                    if job:
                        cur.execute(f"""
                            SELECT alter_job(
                                {job[0]},
                                schedule_interval => INTERVAL '{minutes} minutes'
                            );
                        """)
                        log_message(GREEN, "INTERVAL", f"Updated snapshot interval to {minutes} minutes", level=1)
                    else:
                        cur.execute(f"""
                            SELECT add_job(
                                'take_price_snapshot',
                                INTERVAL '{minutes} minutes',
                                initial_start => NOW()
                            );
                        """)
                        log_message(GREEN, "INTERVAL", f"Created new snapshot job with interval {minutes} minutes", level=1)
                    
                    conn.commit()
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to update job interval: {e}", level=1)

    def stop_job(self):
        """Stop the price history recording job"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT job_id 
                        FROM timescaledb_information.jobs 
                        WHERE proc_name = 'take_price_snapshot'
                        LIMIT 1;
                    """)
                    job = cur.fetchone()
                    
                    if job:
                        cur.execute(f"SELECT delete_job({job[0]});")
                        log_message(GREEN, "STOP", "Price history recording job stopped", level=1)
                    else:
                        log_message(YELLOW, "STOP", "No active price history recording job found", level=1)
                    
                    conn.commit()
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to stop job: {e}", level=1)

    def randomize_last_snapshot(self):
        """Take the last snapshot and create a new one with randomized demands"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(timestamp) FROM price_history;")
                    last_timestamp = cur.fetchone()[0]
                    if not last_timestamp:
                        log_message(RED, "ERROR", "No previous snapshot found to randomize", level=1)
                        return 0

                    rand_percent = 5 + random.random() * 25  # 5-30%
                    log_message(BLUE, "DEBUG", f"Randomizing {rand_percent:.1f}% of records from last snapshot", level=2)
                    
                    # Create temp table with randomized data from last snapshot
                    cur.execute(f"""
                        CREATE TEMP TABLE temp_snapshot AS
                        SELECT 
                            NOW() as timestamp,
                            commodity_id,
                            station_id,
                            price,
                            CASE 
                                WHEN random() < {rand_percent / 100.0} THEN
                                    floor(random() * 20001)::integer
                                ELSE demand
                            END as demand,
                            demand as original_demand
                        FROM price_history
                        WHERE timestamp = %s;
                    """, (last_timestamp,))

                    # Count changes
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(*) FILTER (WHERE demand != original_demand) as changed,
                            MIN(demand) FILTER (WHERE demand != original_demand) as min_new_demand,
                            MAX(demand) FILTER (WHERE demand != original_demand) as max_new_demand,
                            AVG(ABS(demand - original_demand)) FILTER (WHERE demand != original_demand) as avg_change
                        FROM temp_snapshot;
                    """)
                    total, changed, min_demand, max_demand, avg_change = cur.fetchone()
                    log_message(RED, "RANDOM", 
                                f"Modified {changed:,} records ({changed/total*100:.1f}% of {total:,} total)\n"
                                f"New demands range: {min_demand:,} to {max_demand:,}\n"
                                f"Average change: {avg_change:,.0f} units", 
                                level=1)

                    # Insert as new snapshot
                    cur.execute("""
                        WITH inserted AS (
                            INSERT INTO price_history (timestamp, commodity_id, station_id, price, demand)
                            SELECT timestamp, commodity_id, station_id, price, demand
                            FROM temp_snapshot
                            ON CONFLICT DO NOTHING
                            RETURNING *
                        )
                        SELECT COUNT(*) FROM inserted;
                    """)
                    rows_inserted = cur.fetchone()[0]
                    cur.execute("DROP TABLE temp_snapshot;")
                    return rows_inserted

        except Exception as e:
            log_message(RED, "ERROR", f"Failed to randomize snapshot: {e}", level=1)
            return None

    def monitor_new_records(self):
        """Monitor new records being added"""
        last_timestamp = None
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        WITH snapshot_stats AS (
                            SELECT 
                                timestamp,
                                COUNT(*) as record_count,
                                pg_size_pretty(SUM(pg_column_size(price_history.*))) as raw_size,
                                SUM(pg_column_size(price_history.*)) as raw_bytes
                            FROM price_history
                            WHERE timestamp > NOW() - INTERVAL '5 minutes'
                            GROUP BY timestamp
                            ORDER BY timestamp DESC
                            LIMIT 1
                        ), size_stats AS (
                            SELECT * FROM hypertable_detailed_size('price_history')
                        )
                        SELECT 
                            s.timestamp,
                            s.record_count,
                            s.raw_size as snapshot_size,
                            pg_size_pretty(total_bytes) as total_size
                        FROM snapshot_stats s
                        CROSS JOIN size_stats;
                    """)
                    result = cur.fetchone()
                    if result:
                        timestamp, count, raw_size, total_size = result
                        if timestamp != last_timestamp:
                            log_message(GREEN, "SNAPSHOT", 
                                        f"New snapshot at {timestamp.strftime('%Y-%m-%d %H:%M:%S')}:\n"
                                        f"Records: {count:,}, Size: {raw_size}\n"
                                        f"Total database size: {total_size}", 
                                        level=1)
                            last_timestamp = timestamp
                            if self.randomize:
                                self.randomize_last_snapshot()
                    return result
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to monitor records: {e}", level=1)
            return None

    def get_data_timespan(self):
        """Get the current timespan of data in the database"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        WITH timespan AS (
                            SELECT 
                                MIN(timestamp) as oldest,
                                MAX(timestamp) as newest,
                                COUNT(*) as total_rows
                            FROM price_history
                        ), size_info AS (
                            SELECT * FROM hypertable_detailed_size('price_history')
                        )
                        SELECT 
                            t.oldest,
                            t.newest,
                            pg_size_pretty(s.total_bytes) as total_size,
                            pg_size_pretty(s.table_bytes + s.toast_bytes) as table_size,
                            pg_size_pretty(s.index_bytes) as index_size,
                            t.total_rows,
                            s.total_bytes
                        FROM timespan t
                        CROSS JOIN size_info s;
                    """)
                    result = cur.fetchone()
                    if result and result[0] and result[1]:
                        (oldest, newest, total_size, table_size, index_size, 
                         total_rows, raw_total_bytes) = result
                        timespan = newest - oldest
                        return {
                            'oldest': oldest,
                            'newest': newest,
                            'timespan': timespan,
                            'size': total_size,
                            'table_size': table_size,
                            'index_size': index_size,
                            'total_rows': total_rows,
                            'raw_total_bytes': raw_total_bytes
                        }
                    return None
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get data timespan: {e}", level=1)
            return None

    def get_price_history(self, station_id=None, commodity_name=None, start_time=None, hours=24):
        """Get price history for specified parameters
        
        Args:
            station_id (int, optional): Filter by specific station ID
            commodity_name (str, optional): Filter by commodity name
            start_time (datetime, optional): Start time for history (defaults to now - hours)
            hours (int, optional): Number of hours to look back (default 24)
            
        Returns:
            list of DictRows
        """
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    query = """
                        SELECT 
                            ph.timestamp,
                            cl.name as commodity_name,
                            ph.station_id,
                            ph.price,
                            ph.demand,
                            ph.price - LAG(ph.price) OVER (
                                PARTITION BY ph.station_id, ph.commodity_id 
                                ORDER BY ph.timestamp
                            ) as price_change,
                            ph.timestamp - LAG(ph.timestamp) OVER (
                                PARTITION BY ph.station_id, ph.commodity_id 
                                ORDER BY ph.timestamp
                            ) as time_since_last
                        FROM price_history ph
                        JOIN commodity_lookup cl ON cl.id = ph.commodity_id
                        WHERE ph.timestamp > %s
                    """
                    params = [start_time or (datetime.now() - timedelta(hours=hours))]
                    
                    if station_id:
                        query += " AND ph.station_id = %s"
                        params.append(station_id)
                    
                    if commodity_name:
                        query += " AND cl.name = %s"
                        params.append(commodity_name)
                    
                    query += " ORDER BY ph.timestamp DESC"
                    
                    cur.execute(query, params)
                    return cur.fetchall()
                    
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get price history: {e}", level=1)
            return None

    def test_connection(self):
        """Test connections to both databases"""
        success = True
        try:
            with psycopg2.connect(self.db_in) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                log_message(GREEN, "CONNECTION", "Successfully connected to input database", level=1)
        except Exception as e:
            log_message(RED, "CONNECTION", f"Failed to connect to input database: {e}", level=1)
            success = False

        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                log_message(GREEN, "CONNECTION", "Successfully connected to history database", level=1)
        except Exception as e:
            log_message(RED, "CONNECTION", f"Failed to connect to history database: {e}", level=1)
            success = False
            
        return success

    # -------------------------------------------------------------------------
    # NEW FUNCTION: Detailed logging of compression, chunk sizes, snapshot size
    # -------------------------------------------------------------------------
    def log_compression_stats(self, conn, snapshot_timestamp=None):
        """
        Log detailed info about:
          - Overall table size (compressed/uncompressed).
          - Each chunk's size + compression status.
          - Growth in MB since previous snapshot (if we tracked prev_total_bytes).
          - Optional: the snapshot just inserted (if snapshot_timestamp is provided).
        """
        with conn.cursor() as cur:
            # 1) Get overall table size (in bytes) from hypertable_detailed_size
            cur.execute("""
                SELECT total_bytes, table_bytes, index_bytes, toast_bytes
                FROM hypertable_detailed_size('price_history');
            """)
            row = cur.fetchone()
            if not row:
                return
            total_bytes, table_bytes, index_bytes, toast_bytes = row

            # 2) Print overall size
            total_size_pretty = self._pretty_size(total_bytes)
            table_size_pretty = self._pretty_size(table_bytes + toast_bytes)
            index_size_pretty = self._pretty_size(index_bytes)
            growth_mb = None

            # Compare with previous total bytes to see growth in MB
            if self.prev_total_bytes is not None:
                diff_bytes = total_bytes - self.prev_total_bytes
                growth_mb = diff_bytes / (1024 * 1024)
            
            self.prev_total_bytes = total_bytes  # Update for next time

            # Log the overall table size
            msg_lines = [
                f"Overall price_history size: {total_size_pretty}",
                f"  Table+Toast: {table_size_pretty}, Index: {index_size_pretty}",
            ]
            if growth_mb is not None:
                msg_lines.append(f"DB grew by ~{growth_mb:.2f} MB since last snapshot")

            log_message(BLUE, "COMPRESSION-STATS", "\n".join(msg_lines), level=1)

            # 3) Show chunk-level details
            cur.execute("""
                SELECT
                   chunk_name,
                   is_compressed,
                   pg_total_relation_size(chunk_schema || '.' || chunk_name) AS chunk_size
                FROM timescaledb_information.chunks
                WHERE hypertable_name = 'price_history'
                ORDER BY chunk_name;
            """)
            chunks = cur.fetchall()
            for chunk_name, is_cmp, csize in chunks:
                csize_pretty = self._pretty_size(csize)
                log_message(
                    BLUE, "CHUNK-INFO",
                    f"  {chunk_name}: {'COMPRESSED' if is_cmp else 'uncompressed'}, Size: {csize_pretty}",
                    level=2
                )

            # 4) If snapshot_timestamp is provided, show stats for just that snapshot
            if snapshot_timestamp:
                cur.execute("""
                    WITH snapshot_data AS (
                        SELECT
                            timestamp,
                            COUNT(*)  AS num_rows,
                            SUM(pg_column_size(price_history.*)) AS total_column_bytes
                        FROM price_history
                        WHERE timestamp = %s
                        GROUP BY timestamp
                    )
                    SELECT 
                        timestamp, num_rows, total_column_bytes
                    FROM snapshot_data;
                """, (snapshot_timestamp,))
                snap_res = cur.fetchone()
                if snap_res:
                    snap_ts, snap_rows, snap_raw = snap_res
                    snap_raw_pretty = self._pretty_size(snap_raw)
                    log_message(
                        BLUE, "SNAPSHOT-SIZE",
                        f"Snapshot at {snap_ts} => {snap_rows:,} rows, "
                        f"Raw total_column_size: {snap_raw_pretty}",
                        level=1
                    )

    def _pretty_size(self, size_in_bytes):
        """Helper to return a human-readable size."""
        if size_in_bytes < 1024:
            return f"{size_in_bytes} B"
        elif size_in_bytes < 1024 * 1024:
            return f"{size_in_bytes/1024:.2f} KB"
        elif size_in_bytes < 1024 * 1024 * 1024:
            return f"{size_in_bytes/(1024*1024):.2f} MB"
        else:
            return f"{size_in_bytes/(1024*1024*1024):.2f} GB"

    def take_snapshot_now(self):
        """Take an immediate snapshot, then show detailed compression stats."""
        try:
            with psycopg2.connect(self.db_history) as conn:
                # Before snapshot: record the old size for delta calculation
                #   (this is also stored in self.prev_total_bytes in case we want to do a "growth" log)
                before_snapshot_time = datetime.now()

                with conn.cursor() as cur:
                    if self.randomize:
                        # Similar logic, with randomization
                        log_message(BLUE, "DEBUG", "Creating immediate randomized snapshot...", level=2)

                        cur.execute("SELECT dblink_connect('temp_conn', %s);", (self.db_in,))
                        rand_percent = 5 + random.random() * 25  # 5-30%

                        cur.execute(f"""
                            CREATE TEMP TABLE temp_snapshot_raw AS
                            SELECT 
                                NOW() as snapshot_ts,
                                cl.id as commodity_id,
                                sc.station_id,
                                sc.sell_price as price,
                                sc.demand as demand
                            FROM dblink('temp_conn',
                                'SELECT commodity_name, station_id, sell_price, demand 
                                 FROM station_commodities 
                                 WHERE sell_price > 0 
                                   AND demand >= 0'
                            ) AS sc(commodity_name VARCHAR, station_id BIGINT, sell_price INTEGER, demand INTEGER)
                            JOIN commodity_lookup cl ON cl.name = sc.commodity_name;
                        """)

                        # Randomize demands
                        cur.execute(f"""
                            CREATE TEMP TABLE temp_snapshot AS
                            SELECT 
                                snapshot_ts as timestamp,
                                commodity_id,
                                station_id,
                                price,
                                CASE 
                                    WHEN random() < {rand_percent / 100.0} 
                                    THEN floor(random() * 20001)::integer
                                    ELSE demand
                                END as demand
                            FROM temp_snapshot_raw;
                            DROP TABLE temp_snapshot_raw;
                        """)

                        # Insert only changed records
                        cur.execute("""
                            WITH last_values AS (
                                SELECT DISTINCT ON (commodity_id, station_id)
                                    commodity_id, station_id, price, demand
                                FROM price_history
                                ORDER BY commodity_id, station_id, timestamp DESC
                            ), changes AS (
                                SELECT 
                                    ts.timestamp,
                                    ts.commodity_id,
                                    ts.station_id,
                                    ts.price,
                                    ts.demand
                                FROM temp_snapshot ts
                                LEFT JOIN last_values lv 
                                    ON lv.commodity_id = ts.commodity_id
                                   AND lv.station_id   = ts.station_id
                                WHERE 
                                    lv.commodity_id IS NULL
                                    OR lv.price != ts.price
                                    OR lv.demand != ts.demand
                            )
                            INSERT INTO price_history (timestamp, commodity_id, station_id, price, demand)
                            SELECT * FROM changes
                            ON CONFLICT DO NOTHING
                            RETURNING *;
                        """)
                        inserted = cur.fetchall()
                        rows_inserted = len(inserted)

                        cur.execute("DROP TABLE temp_snapshot;")
                        cur.execute("SELECT dblink_disconnect('temp_conn');")

                        log_message(GREEN, "SNAPSHOT", 
                                    f"Immediate randomized snapshot taken: {rows_inserted} records inserted", 
                                    level=1)
                        
                        # Log detailed stats (pass the latest snapshot timestamp if you want snapshot-level stats)
                        if rows_inserted > 0:
                            # We can guess the snapshot_timestamp from the first inserted row
                            # or you can store "NOW()" before insertion. Let's do it from inserted[0][0] (timestamp).
                            last_snapshot_time = inserted[0][0]
                            conn.commit()
                            self.log_compression_stats(conn, snapshot_timestamp=last_snapshot_time)
                        else:
                            conn.commit()
                            self.log_compression_stats(conn)

                        return rows_inserted

                    else:
                        # Normal snapshot
                        cur.execute("SELECT take_price_snapshot();")
                        result = cur.fetchone()
                        rows_inserted = result[0] if result else 0
                        if rows_inserted == 0:
                            log_message(YELLOW, "WARNING", 
                                        "No new records inserted - possibly no data changed", 
                                        level=1)
                        else:
                            log_message(GREEN, "SNAPSHOT", 
                                        f"Immediate snapshot taken: {rows_inserted} records inserted", 
                                        level=1)

                        # Log stats (we can find the newly inserted timestamp by querying the max timestamp)
                        cur.execute("""
                            SELECT MAX(timestamp) FROM price_history
                            WHERE timestamp > (NOW() - INTERVAL '5 minutes');
                        """)
                        last_ts = cur.fetchone()[0]
                        conn.commit()

                        # Log chunk sizes, overall size, etc.
                        if last_ts:
                            self.log_compression_stats(conn, snapshot_timestamp=last_ts)
                        else:
                            # No new snapshot timestamp found, just do overall stats
                            self.log_compression_stats(conn)

                        return rows_inserted
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to take immediate snapshot: {e}", level=1)
            return None

def main():
    parser = argparse.ArgumentParser(description='Price history management')
    parser.add_argument('--db-in', type=str, required=True,
                       help='PostgreSQL URL for input database')
    parser.add_argument('--db-history', type=str, required=True,
                       help='PostgreSQL URL for history database (with TimescaleDB)')
    parser.add_argument('--setup-only', action='store_true',
                       help='Only setup database schema if it does not exist')
    parser.add_argument('--interval', type=int,
                       help='Set snapshot interval in minutes (e.g., 60 for hourly)')
    parser.add_argument('--stop', action='store_true',
                       help='Stop the price history recording job')
    parser.add_argument('--monitor', action='store_true',
                       help='Monitor new records being added')
    parser.add_argument('--start-early', action='store_true',
                       help='Take an immediate snapshot before waiting for the scheduled interval')
    parser.add_argument('--rand', action='store_true',
                       help='Randomize a percentage of commodity demands between 0-20000 for testing')
    args = parser.parse_args()
    
    manager = PriceHistoryManager(args.db_in, args.db_history, randomize=args.rand)
    
    try:
        # Test connections first
        if not manager.test_connection():
            log_message(RED, "FATAL", "Database connection test failed", level=1)
            sys.exit(1)

        # Handle stop request
        if args.stop:
            manager.stop_job()
            return

        # Setup if requested and table doesn't exist
        if args.setup_only:
            manager.setup_database()

        # Take immediate snapshot if requested
        if args.start_early:
            manager.take_snapshot_now()

        # Show current data timespan
        timespan = manager.get_data_timespan()
        if timespan:
            log_message(BLUE, "INFO", "Current data timespan:", level=1)
            log_message(BLUE, "INFO", f"  Oldest record: {timespan['oldest']}", level=1)
            log_message(BLUE, "INFO", f"  Newest record: {timespan['newest']}", level=1)
            log_message(BLUE, "INFO", f"  Total duration: {timespan['timespan']}", level=1)
            log_message(BLUE, "INFO", f"  Database size: {timespan['size']}", level=1)
            log_message(BLUE, "INFO", f"  Total records: {timespan['total_rows']:,}", level=1)

            # Also store the total bytes in manager for future "growth" calculation
            manager.prev_total_bytes = timespan['raw_total_bytes']

        # Show current snapshot interval and status
        job_info = manager.get_current_job_interval()
        if job_info:
            log_message(BLUE, "INFO", f"Current snapshot interval: {job_info['minutes']} minutes", level=1)
            log_message(BLUE, "INFO", f"Next snapshot scheduled: {job_info['next_start']}", level=1)
            log_message(BLUE, "INFO", f"Last run status: {job_info['status']}", level=1)
            log_message(BLUE, "INFO", f"Last successful run: {job_info['last_success']}", level=1)

        # Update interval if requested
        if args.interval:
            if not job_info or job_info['minutes'] != args.interval:
                manager.update_job_interval(args.interval)
            else:
                log_message(BLUE, "INFO", f"Snapshot interval already set to {args.interval} minutes", level=1)

        # Monitor mode
        if args.monitor:
            log_message(BLUE, "MONITOR", "Starting snapshot monitor (Ctrl+C to stop)...", level=1)
            try:
                while True:
                    manager.monitor_new_records()
                    time.sleep(30)  # Check every 30 seconds
            except KeyboardInterrupt:
                log_message(YELLOW, "MONITOR", "Monitoring stopped by user", level=1)

    except Exception as e:
        log_message(RED, "FATAL", f"Service failed: {e}", level=1)
        sys.exit(1)

if __name__ == "__main__":
    main()

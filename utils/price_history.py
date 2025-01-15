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
                    
                    # Create price history table with optimized types
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS price_history (
                            timestamp TIMESTAMPTZ NOT NULL,
                            commodity_id SMALLINT,
                            station_id BIGINT,
                            price INTEGER,
                            demand INTEGER,
                            FOREIGN KEY (commodity_id) REFERENCES commodity_lookup(id)
                        );
                    """)
                    
                    # First create hypertable
                    cur.execute("""
                        SELECT create_hypertable('price_history', 'timestamp',
                            chunk_time_interval => INTERVAL '1 hour',
                            if_not_exists => TRUE,
                            migrate_data => TRUE
                        );
                    """)
                    
                    # Enable compression with correct settings
                    cur.execute("""
                        ALTER TABLE price_history SET (
                            timescaledb.compress,
                            timescaledb.compress_segmentby = 'commodity_id,station_id',
                            timescaledb.compress_orderby = 'timestamp DESC'
                        );
                    """)
                    
                    # Remove any existing compression policy and add new one for immediate compression
                    cur.execute("""
                        SELECT remove_compression_policy('price_history', if_exists => true);
                        SELECT add_compression_policy('price_history', 
                            compress_after => INTERVAL '1 millisecond',
                            if_not_exists => true
                        );
                    """)

                    # Create snapshot function with proper change detection
                    cur.execute(f"""
                        CREATE OR REPLACE FUNCTION take_price_snapshot()
                        RETURNS INTEGER AS $$
                        DECLARE
                            rows_inserted INTEGER;
                            rand_percent FLOAT;
                        BEGIN
                            -- Generate random percentage between 5-30 if randomization is enabled
                            SELECT CASE WHEN EXISTS (
                                SELECT 1 FROM pg_settings WHERE name = 'app.randomize_enabled' AND setting = 'true'
                            ) THEN
                                5 + random() * 25  -- 5-30%
                            ELSE
                                0
                            END INTO rand_percent;

                            -- Create temp table with latest data
                            CREATE TEMP TABLE temp_current AS
                            SELECT 
                                NOW() as timestamp,
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

                            -- Insert only records that have actually changed
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
                                    CASE 
                                        WHEN rand_percent > 0 AND random() < rand_percent/100 THEN
                                            floor(random() * 20001)  -- Random value between 0-20000
                                        ELSE tc.demand
                                    END as demand
                                FROM temp_current tc
                                LEFT JOIN last_values lv ON 
                                    lv.commodity_id = tc.commodity_id AND 
                                    lv.station_id = tc.station_id
                                WHERE 
                                    lv.commodity_id IS NULL OR  -- New record
                                    lv.price != tc.price OR     -- Price changed
                                    lv.demand != tc.demand      -- Demand changed
                            )
                            INSERT INTO price_history (timestamp, commodity_id, station_id, price, demand)
                            SELECT * FROM changes;

                            GET DIAGNOSTICS rows_inserted = ROW_COUNT;
                            
                            DROP TABLE temp_current;
                            
                            -- Force immediate compression of the chunk (usually fails if chunk is open)
                            PERFORM compress_chunk(i.chunk_schema || '.' || i.chunk_name)
                            FROM (
                                SELECT chunk_schema, chunk_name
                                FROM timescaledb_information.chunks
                                WHERE hypertable_name = 'price_history'
                                ORDER BY range_end DESC
                                LIMIT 1
                            ) i;

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
                    
                    # Create job for snapshots (default 1 hour)
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

    # CHANGE: parse the timescale interval, including 'HH:MM:SS' logic
    def parse_interval_str(self, interval_str):
        """Parse TimescaleDB interval (e.g. '1:00:00', '1 hour', '10 minutes') to integer minutes."""
        interval_str = interval_str.strip().lower()
        if ':' in interval_str:
            # e.g. '01:00:00' => 60
            parts = interval_str.split(':')
            if len(parts) == 3:
                try:
                    hh = int(parts[0])
                    mm = int(parts[1])
                    # ignoring seconds
                    return hh * 60 + mm
                except:
                    return None
        elif 'hour' in interval_str:
            # e.g. '1 hour', '2 hours'
            try:
                parts = interval_str.split()
                # '1 hour' => parts[0] = '1'
                hrs = int(parts[0])
                return hrs * 60
            except:
                return None
        elif 'minute' in interval_str:
            # e.g. '10 minutes', '1 minute'
            try:
                parts = interval_str.split()
                mins = int(parts[0])
                return mins
            except:
                return None
        return None

    def get_current_job_interval(self):
        """Get current snapshot interval in minutes."""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    # CHANGE: gather all jobs, no LIMIT 1
                    cur.execute("""
                        SELECT job_id, schedule_interval, next_start
                        FROM timescaledb_information.jobs
                        WHERE proc_name = 'take_price_snapshot'
                        ORDER BY job_id;
                    """)
                    jobs = cur.fetchall()
                    if not jobs:
                        return None
                    
                    # We'll just consider the first job
                    job_id, schedule_interval, next_start = jobs[0]
                    interval_str = str(schedule_interval)
                    minutes = self.parse_interval_str(interval_str)

                    return {
                        'minutes': minutes,
                        'next_start': next_start,
                        'status': 'Unknown',
                        'last_success': None,
                        'interval_str': interval_str,
                        'job_id': job_id
                    }
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get job interval: {e}", level=1)
            return None

    def update_job_interval(self, minutes):
        """Update snapshot job interval"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    # CHANGE: gather all jobs, no LIMIT 1
                    cur.execute("""
                        SELECT job_id 
                        FROM timescaledb_information.jobs 
                        WHERE proc_name = 'take_price_snapshot'
                        ORDER BY job_id;
                    """)
                    jobs = cur.fetchall()
                    
                    if jobs:
                        job_id = jobs[0][0]
                        cur.execute(f"""
                            SELECT alter_job(
                                {job_id},
                                schedule_interval => INTERVAL '{minutes} minutes'
                            );
                        """)
                        log_message(GREEN, "INTERVAL", f"Updated snapshot interval to {minutes} minutes", level=1)
                    else:
                        # Create new job if doesn't exist
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
                    # CHANGE: remove LIMIT 1, delete all matching jobs
                    cur.execute("""
                        SELECT job_id 
                        FROM timescaledb_information.jobs 
                        WHERE proc_name = 'take_price_snapshot';
                    """)
                    jobs = cur.fetchall()
                    
                    if jobs:
                        for (jid,) in jobs:
                            cur.execute(f"SELECT delete_job({jid});")
                        log_message(GREEN, "STOP", f"Stopped {len(jobs)} job(s).", level=1)
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
                    # Get the latest snapshot timestamp
                    cur.execute("""
                        SELECT MAX(timestamp) FROM price_history;
                    """)
                    last_timestamp = cur.fetchone()[0]
                    if not last_timestamp:
                        log_message(RED, "ERROR", "No previous snapshot found to randomize", level=1)
                        return 0

                    # Create temp table with randomized data from last snapshot
                    rand_percent = 5 + random.random() * 25  # 5-30%
                    log_message(BLUE, "DEBUG", f"Randomizing {rand_percent:.1f}% of records from last snapshot", level=2)
                    
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

                    # Count and show changes
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
                              f"Average change: {avg_change:,.0f} units", level=1)

                    # Insert as new snapshot
                    cur.execute("""
                        WITH inserted AS (
                            INSERT INTO price_history (timestamp, commodity_id, station_id, price, demand)
                            SELECT timestamp, commodity_id, station_id, price, demand
                            FROM temp_snapshot
                            RETURNING *
                        )
                        SELECT COUNT(*) FROM inserted;
                    """)
                    rows_inserted = cur.fetchone()[0]
                    
                    # Clean up
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
                        if timestamp != last_timestamp:  # Only show if it's a new snapshot
                            log_message(GREEN, "SNAPSHOT", 
                                      f"New snapshot at {timestamp.strftime('%Y-%m-%d %H:%M:%S')}:\n"
                                      f"Records: {count:,}, Size: {raw_size}\n"
                                      f"Total database size: {total_size}", level=1)
                            last_timestamp = timestamp
                            
                            # If randomization is enabled, create a new randomized snapshot
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
                            t.total_rows
                        FROM timespan t
                        CROSS JOIN size_info s;
                    """)
                    result = cur.fetchone()
                    if result and result[0] and result[1]:
                        oldest, newest, total_size, table_size, index_size, total_rows = result
                        timespan = newest - oldest
                        return {
                            'oldest': oldest,
                            'newest': newest,
                            'timespan': timespan,
                            'size': total_size,
                            'table_size': table_size,
                            'index_size': index_size,
                            'total_rows': total_rows
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
            list: List of dictionaries containing price history records with fields:
                 timestamp, commodity_name, station_id, price, demand
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
                            -- Calculate price changes
                            ph.price - LAG(ph.price) OVER (
                                PARTITION BY ph.station_id, ph.commodity_id 
                                ORDER BY ph.timestamp
                            ) as price_change,
                            -- Calculate time since last update
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

    def take_snapshot_now(self):
        """Take an immediate snapshot"""
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    if self.randomize:
                        try:
                            # First verify we can read from input database
                            log_message(BLUE, "DEBUG", "Checking input database connection...", level=2)
                            try:
                                cur.execute("SELECT dblink_connect('temp_conn', %s);", (self.db_in,))
                                log_message(BLUE, "DEBUG", "Connected to input database", level=2)
                                
                                # Create a temporary table for the modified data
                                log_message(BLUE, "DEBUG", "Creating temporary table with randomized demands...", level=2)
                                rand_percent = 5 + random.random() * 25  # 5-30%
                                cur.execute(f"""
                                    CREATE TEMP TABLE temp_snapshot AS
                                    SELECT 
                                        NOW() as timestamp,
                                        cl.id as commodity_id,
                                        sc.station_id,
                                        sc.sell_price as price,
                                        CASE 
                                            WHEN random() < {rand_percent / 100.0} THEN  -- Random % chance to modify
                                                floor(random() * 20001)::integer  -- Random value between 0-20000
                                            ELSE sc.demand
                                        END as demand
                                    FROM dblink('temp_conn',
                                        'SELECT commodity_name, station_id, sell_price, demand 
                                         FROM station_commodities 
                                         WHERE sell_price > 0 
                                         AND demand >= 0'
                                    ) AS sc(commodity_name VARCHAR, station_id BIGINT, sell_price INTEGER, demand INTEGER)
                                    JOIN commodity_lookup cl ON cl.name = sc.commodity_name;
                                """)
                                log_message(BLUE, "DEBUG", "Temporary table created successfully", level=2)

                                # Count randomized records
                                cur.execute("""
                                    SELECT 
                                        COUNT(*) as total,
                                        COUNT(*) FILTER (WHERE demand <= 20000 AND demand >= 0) as randomized
                                    FROM temp_snapshot;
                                """)
                                total, randomized = cur.fetchone()
                                log_message(RED, "RANDOM", f"Randomized {randomized:,} records ({rand_percent:.1f}% of {total:,} total)", level=1)

                                # Insert from temp table and get count
                                log_message(BLUE, "DEBUG", "Inserting randomized records...", level=2)
                                try:
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
                                    result = cur.fetchone()
                                    log_message(BLUE, "DEBUG", f"Insert result: {result}", level=2)
                                    rows_inserted = result[0] if result else 0
                                    log_message(BLUE, "DEBUG", f"Rows inserted: {rows_inserted}", level=2)
                                except Exception as e:
                                    log_message(RED, "ERROR", f"Failed during insert: {e}", level=1)
                                    raise
                                
                                # Clean up
                                log_message(BLUE, "DEBUG", "Cleaning up temporary table...", level=2)
                                try:
                                    cur.execute("DROP TABLE temp_snapshot;")
                                    log_message(BLUE, "DEBUG", "Temporary table dropped successfully", level=2)
                                except Exception as e:
                                    log_message(RED, "ERROR", f"Failed to drop temp table: {e}", level=1)
                                    raise
                                
                                return rows_inserted
                                 
                            except Exception as e:
                                log_message(RED, "ERROR", f"Failed to create temp table: {e}", level=1)
                                raise
                            finally:
                                # Clean up the connection
                                cur.execute("SELECT dblink_disconnect('temp_conn');")
                            
                            # Insert from temp table and get count
                            log_message(BLUE, "DEBUG", "Inserting randomized records...", level=2)
                            try:
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
                                result = cur.fetchone()
                                log_message(BLUE, "DEBUG", f"Insert result: {result}", level=2)
                                rows_inserted = result[0] if result else 0
                                log_message(BLUE, "DEBUG", f"Rows inserted: {rows_inserted}", level=2)
                            except Exception as e:
                                log_message(RED, "ERROR", f"Failed during insert: {e}", level=1)
                                raise
                            
                            # Clean up
                            log_message(BLUE, "DEBUG", "Cleaning up temporary table...", level=2)
                            try:
                                cur.execute("DROP TABLE temp_snapshot;")
                                log_message(BLUE, "DEBUG", "Temporary table dropped successfully", level=2)
                            except Exception as e:
                                log_message(RED, "ERROR", f"Failed to drop temp table: {e}", level=1)
                                raise
                            
                            return rows_inserted
                            
                        except Exception as e:
                            log_message(RED, "ERROR", f"Detailed error in randomization branch: {e}", level=1)
                            return None
                    else:
                        # Take normal snapshot
                        log_message(BLUE, "DEBUG", "Taking normal snapshot...", level=2)
                        
                        # First verify we can read from input database
                        cur.execute("""
                            SELECT COUNT(*) FROM dblink(%s,
                                'SELECT 1 FROM station_commodities 
                                 WHERE sell_price > 0 AND demand >= 0'
                            ) AS t(dummy INTEGER);
                        """, (self.db_in,))
                        check = cur.fetchone()
                        if not check or check[0] == 0:
                            log_message(RED, "ERROR", "No valid records found in input database", level=1)
                            return 0
                            
                        # Now take the snapshot
                        cur.execute("SELECT take_price_snapshot();")
                        result = cur.fetchone()
                        rows_inserted = result[0] if result else 0
                        
                        if rows_inserted == 0:
                            log_message(YELLOW, "WARNING", "No new records inserted - might be duplicates within same minute", level=1)
                    
                    log_message(GREEN, "SNAPSHOT", f"Immediate snapshot taken: {rows_inserted} records inserted", level=1)
                    
                    # Compress all uncompressed chunks
                    cur.execute("""
                        SELECT compress_chunk(chunk_schema || '.' || chunk_name)
                        FROM timescaledb_information.chunks
                        WHERE hypertable_name = 'price_history'
                        AND NOT is_compressed;
                    """)
                    conn.commit()
                    
                    # Verify compression
                    cur.execute("""
                        WITH size_stats AS (
                            SELECT 
                                (SELECT COUNT(*) FROM timescaledb_information.chunks 
                                 WHERE hypertable_name = 'price_history') as total_chunks,
                                (SELECT COUNT(*) FROM timescaledb_information.chunks 
                                 WHERE hypertable_name = 'price_history' AND is_compressed) as compressed_chunks,
                                hypertable_detailed_size('price_history') as sizes
                            FROM (SELECT 1) t
                        )
                        SELECT 
                            total_chunks,
                            compressed_chunks,
                            pg_size_pretty(sizes.total_bytes) as total_size,
                            pg_size_pretty(sizes.table_bytes + sizes.toast_bytes) as table_size,
                            pg_size_pretty(sizes.index_bytes) as index_size
                        FROM size_stats, LATERAL (SELECT * FROM hypertable_detailed_size('price_history')) sizes;
                    """)
                    stats = cur.fetchone()
                    if stats:
                        total_chunks, compressed_chunks, total_size, table_size, index_size = stats
                        log_message(GREEN, "COMPRESSION", 
                                  f"Chunks: {compressed_chunks}/{total_chunks} compressed, "
                                  f"Total size: {total_size}, "
                                  f"Table size: {table_size}, "
                                  f"Index size: {index_size}", level=1)
                    
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
                       help='Randomize 20%% of commodity demands between 0-20000 for testing')
    args = parser.parse_args()
    
    manager = PriceHistoryManager(args.db_in, args.db_history, randomize=args.rand)
    
    try:
        # 1) Test connections first
        if not manager.test_connection():
            log_message(RED, "FATAL", "Database connection test failed", level=1)
            sys.exit(1)

        # 2) Stop job if requested
        if args.stop:
            manager.stop_job()
            return

        # 3) Setup if requested and table doesn't exist
        if args.setup_only:
            manager.setup_database()

        # CHANGE: Move the interval update BEFORE retrieving current job info.
        #    This ensures that if you do --interval 10, we actually set it
        #    before printing "Current snapshot interval".
        if args.interval:
            manager.update_job_interval(args.interval)

        # Now get current job interval *after* the update
        job_info = manager.get_current_job_interval()
        if job_info:
            mins = job_info['minutes']
            interval_str = job_info['interval_str']
            log_message(BLUE, "INFO", 
                        f"Current snapshot interval: {mins} minutes (raw interval: '{interval_str}')", 
                        level=1)
            log_message(BLUE, "INFO", f"Next snapshot scheduled: {job_info['next_start']}", level=1)
            log_message(BLUE, "INFO", f"Last run status: {job_info['status']}", level=1)
            log_message(BLUE, "INFO", f"Last successful run: {job_info['last_success']}", level=1)
        else:
            log_message(YELLOW, "INFO", "No take_price_snapshot job found at all.", level=1)

        # 4) If requested, take immediate snapshot
        if args.start_early:
            rows_inserted = manager.take_snapshot_now()
            log_message(BLUE, "INFO", f"Snapshot inserted rows: {rows_inserted}", level=1)

        # 5) Show current data timespan
        timespan = manager.get_data_timespan()
        if timespan:
            log_message(BLUE, "INFO", f"Current data timespan:", level=1)
            log_message(BLUE, "INFO", f"  Oldest record: {timespan['oldest']}", level=1)
            log_message(BLUE, "INFO", f"  Newest record: {timespan['newest']}", level=1)
            log_message(BLUE, "INFO", f"  Total duration: {timespan['timespan']}", level=1)
            log_message(BLUE, "INFO", f"  Database size: {timespan['size']}", level=1)
            log_message(BLUE, "INFO", f"  Total records: {timespan['total_rows']:,}", level=1)

        # 6) Monitor mode
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

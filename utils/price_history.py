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

DEBUG_LEVEL = 1  # 0 = silent, 1 = critical, 2 = normal, 3 = verbose

class PriceHistoryManager:
    def __init__(self, db_in, db_history, randomize=False):
        self.db_in = db_in
        self.db_history = db_history
        self.commodity_map = self._load_commodity_map()
        self.randomize = randomize
        self.last_logged_timestamp = None

    def _load_commodity_map(self):
        commodity_map = {}
        try:
            with open('data/commodities_mining.csv', 'r') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader, 1):
                    commodity_map[row['name']] = i
            return commodity_map
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to load commodity mapping: {e}", level=1)
            return None

    def check_table_exists(self):
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
        if self.check_table_exists():
            log_message(YELLOW, "SETUP", "Price history table already exists, skipping setup", level=1)
            return
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS commodity_lookup (
                            id SMALLINT PRIMARY KEY,
                            name VARCHAR(50) UNIQUE
                        );
                    """)
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
                    cur.execute("""
                        SELECT create_hypertable('price_history', 'timestamp',
                            chunk_time_interval => INTERVAL '1 hour',
                            if_not_exists => TRUE,
                            migrate_data => TRUE
                        );
                    """)
                    cur.execute("""
                        ALTER TABLE price_history SET (
                            timescaledb.compress,
                            timescaledb.compress_segmentby = 'commodity_id,station_id',
                            timescaledb.compress_orderby = 'timestamp DESC'
                        );
                    """)
                    cur.execute("""
                        SELECT remove_compression_policy('price_history', if_exists => true);
                        SELECT add_compression_policy('price_history',
                            compress_after => INTERVAL '1 millisecond',
                            if_not_exists => true
                        );
                    """)
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
                                5 + random() * 25
                            ELSE
                                0
                            END INTO rand_percent;

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
                                        WHEN rand_percent > 0 AND random() < rand_percent/100
                                        THEN floor(random() * 20001)
                                        ELSE tc.demand
                                    END as demand
                                FROM temp_current tc
                                LEFT JOIN last_values lv
                                  ON lv.commodity_id = tc.commodity_id
                                 AND lv.station_id   = tc.station_id
                                WHERE
                                    lv.commodity_id IS NULL
                                 OR lv.price != tc.price
                                 OR lv.demand != tc.demand
                            )
                            INSERT INTO price_history (timestamp, commodity_id, station_id, price, demand)
                            SELECT * FROM changes;

                            GET DIAGNOSTICS rows_inserted = ROW_COUNT;
                            DROP TABLE temp_current;

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
                    if self.randomize:
                        cur.execute("SELECT current_database();")
                        dbname = cur.fetchone()[0]
                        cur.execute(f"ALTER DATABASE {dbname} SET app.randomize_enabled TO true;")
                    else:
                        cur.execute("SELECT current_database();")
                        dbname = cur.fetchone()[0]
                        cur.execute(f"ALTER DATABASE {dbname} SET app.randomize_enabled TO false;")
                    cur.execute("CREATE EXTENSION IF NOT EXISTS dblink;")
                    cur.execute("""
                        SELECT add_job(
                            'take_price_snapshot',
                            '1 hour',
                            initial_start => NOW()
                        );
                    """)
                    for name, cid in self.commodity_map.items():
                        cur.execute("""
                            INSERT INTO commodity_lookup (id, name)
                            VALUES (%s, %s)
                            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
                        """, (cid, name))
                    conn.commit()
                    log_message(GREEN, "SETUP", "History database schema initialized successfully", level=1)
        except Exception as e:
            log_message(RED, "ERROR", f"Database setup failed: {e}", level=1)
            raise

    def _parse_interval_str(self, interval_str):
        interval_str = interval_str.strip().lower()
        if ':' in interval_str:
            parts = interval_str.split(':')
            if len(parts) == 3:
                try:
                    hh = int(parts[0])
                    mm = int(parts[1])
                    return hh * 60 + mm
                except:
                    return None
        elif 'hour' in interval_str:
            try:
                hr = int(interval_str.split()[0])
                return hr * 60
            except:
                return None
        elif 'minute' in interval_str:
            try:
                m = int(interval_str.split()[0])
                return m
            except:
                return None
        return None

    def get_current_job_interval(self):
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT job_id, schedule_interval, next_start
                        FROM timescaledb_information.jobs
                        WHERE proc_name = 'take_price_snapshot'
                        ORDER BY job_id;
                    """)
                    rows = cur.fetchall()
                    if not rows:
                        return None
                    job_id, schedule_interval, next_start = rows[0]
                    interval_str = str(schedule_interval)
                    mins = self._parse_interval_str(interval_str)
                    return {
                        'job_id': job_id,
                        'minutes': mins,
                        'interval_str': interval_str,
                        'next_start': next_start,
                        'status': 'Unknown',
                        'last_success': None
                    }
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get job interval: {e}", level=1)
            return None

    def update_job_interval(self, minutes):
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT job_id
                        FROM timescaledb_information.jobs
                        WHERE proc_name = 'take_price_snapshot'
                        ORDER BY job_id;
                    """)
                    rows = cur.fetchall()
                    if rows:
                        job_id = rows[0][0]
                        cur.execute(f"""
                            SELECT alter_job({job_id}, schedule_interval => INTERVAL '{minutes} minutes');
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
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT job_id
                        FROM timescaledb_information.jobs
                        WHERE proc_name = 'take_price_snapshot';
                    """)
                    rows = cur.fetchall()
                    if rows:
                        for (jid,) in rows:
                            cur.execute(f"SELECT delete_job({jid});")
                        log_message(GREEN, "STOP", f"Stopped {len(rows)} job(s).", level=1)
                    else:
                        log_message(YELLOW, "STOP", "No active price history recording job found", level=1)
                    conn.commit()
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to stop job: {e}", level=1)

    def randomize_last_snapshot(self):
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(timestamp) FROM price_history;")
                    last_ts = cur.fetchone()[0]
                    if not last_ts:
                        log_message(RED, "ERROR", "No previous snapshot found to randomize", level=1)
                        return 0
                    rand_percent = 5 + random.random() * 25
                    log_message(BLUE, "DEBUG", f"Randomizing {rand_percent:.1f}% of records from last snapshot", level=2)
                    cur.execute(f"""
                        CREATE TEMP TABLE temp_snapshot AS
                        SELECT
                            NOW() as timestamp,
                            commodity_id,
                            station_id,
                            price,
                            CASE
                                WHEN random() < {rand_percent / 100.0}
                                THEN floor(random() * 20001)::integer
                                ELSE demand
                            END as demand,
                            demand as original_demand
                        FROM price_history
                        WHERE timestamp = %s;
                    """, (last_ts,))
                    cur.execute("""
                        SELECT
                            COUNT(*) as total,
                            COUNT(*) FILTER (WHERE demand != original_demand) as changed,
                            MIN(demand) FILTER (WHERE demand != original_demand) as min_d,
                            MAX(demand) FILTER (WHERE demand != original_demand) as max_d,
                            AVG(ABS(demand - original_demand)) FILTER (WHERE demand != original_demand) as avg_d
                        FROM temp_snapshot;
                    """)
                    total, changed, min_d, max_d, avg_d = cur.fetchone()
                    log_message(RED, "RANDOM",
                                f"Modified {changed:,} records ({changed/total*100:.1f}% of {total:,} total)\n"
                                f"New demands range: {min_d:,} to {max_d:,}\n"
                                f"Average change: {avg_d:,.0f} units",
                                level=1)
                    cur.execute("""
                        WITH inserted AS (
                            INSERT INTO price_history(timestamp, commodity_id, station_id, price, demand)
                            SELECT timestamp, commodity_id, station_id, price, demand
                            FROM temp_snapshot
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
                            s.raw_size,
                            pg_size_pretty(total_bytes) as total_size
                        FROM snapshot_stats s
                        CROSS JOIN size_stats;
                    """)
                    row = cur.fetchone()
                    if row:
                        ts, count, snap_size, total_db_size = row
                        if ts and ts != self.last_logged_timestamp:
                            log_message(GREEN, "SNAPSHOT",
                                        f"New snapshot at {ts}:\n"
                                        f"Records: {count:,}, Size: {snap_size}\n"
                                        f"Total database size: {total_db_size}",
                                        level=1)
                            self.last_logged_timestamp = ts
                    return row
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to monitor records: {e}", level=1)
            return None

    def get_data_timespan(self):
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
                            pg_size_pretty(s.total_bytes),
                            pg_size_pretty(s.table_bytes + s.toast_bytes),
                            pg_size_pretty(s.index_bytes),
                            t.total_rows
                        FROM timespan t
                        CROSS JOIN size_info s;
                    """)
                    r = cur.fetchone()
                    if r and r[0] and r[1]:
                        oldest, newest, total_size, table_size, index_size, total_rows = r
                        return {
                            'oldest': oldest,
                            'newest': newest,
                            'timespan': (newest - oldest),
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
        try:
            with psycopg2.connect(self.db_history) as conn:
                with conn.cursor() as cur:
                    if self.randomize:
                        try:
                            cur.execute("SELECT dblink_connect('temp_conn', %s);", (self.db_in,))
                            rand_percent = 5 + random.random() * 25
                            cur.execute(f"""
                                CREATE TEMP TABLE temp_snapshot AS
                                SELECT
                                    NOW() as timestamp,
                                    cl.id as commodity_id,
                                    sc.station_id,
                                    sc.sell_price as price,
                                    CASE
                                        WHEN random() < {rand_percent / 100.0}
                                        THEN floor(random() * 20001)::integer
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
                            cur.execute("""
                                WITH inserted AS (
                                    INSERT INTO price_history(timestamp, commodity_id, station_id, price, demand)
                                    SELECT timestamp, commodity_id, station_id, price, demand
                                    FROM temp_snapshot
                                    ON CONFLICT DO NOTHING
                                    RETURNING *
                                )
                                SELECT COUNT(*) FROM inserted;
                            """)
                            inserted = cur.fetchone()[0]
                            cur.execute("DROP TABLE temp_snapshot;")
                            cur.execute("SELECT dblink_disconnect('temp_conn');")
                            return inserted
                        except Exception as e:
                            return None
                    else:
                        cur.execute("SELECT take_price_snapshot();")
                        row = cur.fetchone()
                        rows_inserted = row[0] if row else 0
                        return rows_inserted
        except Exception as e:
            return None

def main():
    parser = argparse.ArgumentParser(description='Price history management')
    parser.add_argument('--db-in', type=str, required=True,
                       help='PostgreSQL URL for input database')
    parser.add_argument('--db-history', type=str, required=True,
                       help='PostgreSQL URL for history database (TimescaleDB)')
    parser.add_argument('--setup-only', action='store_true',
                       help='Only setup database schema if it does not exist')
    parser.add_argument('--interval', type=int,
                       help='Set snapshot interval in minutes')
    parser.add_argument('--stop', action='store_true',
                       help='Stop the price history recording job')
    parser.add_argument('--monitor', action='store_true',
                       help='Monitor new records')
    parser.add_argument('--start-early', action='store_true',
                       help='Take an immediate snapshot now')
    parser.add_argument('--rand', action='store_true',
                       help='Randomize demands for testing')
    args = parser.parse_args()

    manager = PriceHistoryManager(args.db_in, args.db_history, randomize=args.rand)

    try:
        if not manager.test_connection():
            log_message(RED, "FATAL", "Database connection test failed", level=1)
            sys.exit(1)

        if args.stop:
            manager.stop_job()
            return

        if args.setup_only:
            manager.setup_database()

        if args.interval:
            manager.update_job_interval(args.interval)

        job_info = manager.get_current_job_interval()
        if job_info:
            mins = job_info['minutes']
            int_str = job_info['interval_str']
            log_message(BLUE, "INFO",
                        f"Current snapshot interval: {mins} minutes (raw: '{int_str}')",
                        level=1)
            log_message(BLUE, "INFO",
                        f"Next snapshot scheduled: {job_info['next_start']}",
                        level=1)
            log_message(BLUE, "INFO",
                        f"Last run status: {job_info['status']}",
                        level=1)
            log_message(BLUE, "INFO",
                        f"Last successful run: {job_info['last_success']}",
                        level=1)
        else:
            log_message(YELLOW, "INFO", "No take_price_snapshot job found at all.", level=1)

        if args.start_early:
            manager.take_snapshot_now()

        timespan = manager.get_data_timespan()
        if timespan:
            log_message(BLUE, "INFO", "Current data timespan:", level=1)
            log_message(BLUE, "INFO", f"  Oldest record: {timespan['oldest']}", level=1)
            log_message(BLUE, "INFO", f"  Newest record: {timespan['newest']}", level=1)
            log_message(BLUE, "INFO", f"  Total duration: {timespan['timespan']}", level=1)
            log_message(BLUE, "INFO", f"  Database size: {timespan['size']}", level=1)
            log_message(BLUE, "INFO", f"  Total records: {timespan['total_rows']:,}", level=1)

        if args.monitor:
            log_message(BLUE, "MONITOR", "Starting monitor (Ctrl+C to stop)...", level=1)
            try:
                while True:
                    manager.monitor_new_records()
                    time.sleep(30)
            except KeyboardInterrupt:
                log_message(YELLOW, "MONITOR", "Monitoring stopped by user", level=1)

    except Exception as e:
        log_message(RED, "FATAL", f"Service failed: {e}", level=1)
        sys.exit(1)

if __name__ == "__main__":
    main()

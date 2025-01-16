import os
import sys
import csv
import psycopg2
import argparse
from datetime import datetime, timedelta
from psycopg2.extras import DictCursor
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.common import log_message, BLUE, RED, YELLOW, GREEN

class HistoryManager:
    def __init__(self, db_in, db_history):
        self.db_in = db_in
        self.db_history = db_history
        self.commodity_map = self._load_commodity_map()
        self._test_connection()

    def _load_commodity_map(self):
        commodity_map = {}
        try:
            with open('data/commodities_valuable.csv', 'r') as f:
                reader = csv.DictReader(f)
                for idx, row in enumerate(reader):
                    commodity_map[row['name']] = idx
            return commodity_map
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to load commodity mapping: {e}", level=1)
            return None

    def _test_connection(self):
        try:
            with psycopg2.connect(self.db_in) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
            with psycopg2.connect(self.db_history) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception as e:
            log_message(RED, "ERROR", f"Database connection failed: {e}")
            sys.exit(1)

    def setup(self):
        try:
            with psycopg2.connect(self.db_history) as conn, conn.cursor() as cur:
                # Create tables and hypertable
                cur.execute("""
                    DROP TABLE IF EXISTS commodity_lookup CASCADE;
                    CREATE TABLE commodity_lookup (
                        id SMALLINT PRIMARY KEY,
                        name VARCHAR(50) UNIQUE
                    );
                    
                    CREATE TABLE IF NOT EXISTS price_history (
                        timestamp TIMESTAMPTZ NOT NULL,
                        commodity_id SMALLINT,
                        station_id BIGINT,
                        price INTEGER,
                        demand INTEGER,
                        FOREIGN KEY (commodity_id) REFERENCES commodity_lookup(id)
                    );
                """)
                
                # Create hypertable with larger chunks for better compression
                cur.execute("""
                    SELECT create_hypertable('price_history', 'timestamp',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    );
                """)
                
                # Setup compression with optimal settings
                cur.execute("""
                    ALTER TABLE price_history SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'commodity_id,station_id',
                        timescaledb.compress_orderby = 'timestamp DESC',
                        timescaledb.compress_chunk_time_interval = '1 day'
                    );
                """)
                
                # Add retention policy (7 days)
                cur.execute("""
                    SELECT remove_retention_policy('price_history', if_exists => true);
                    SELECT add_retention_policy('price_history', INTERVAL '7 days');
                """)
                
                # Add compression policy
                cur.execute("""
                    SELECT remove_compression_policy('price_history', if_exists => true);
                    SELECT add_compression_policy('price_history',
                        compress_after => INTERVAL '1 hour',
                        if_not_exists => true
                    );
                """)

                # Create dblink extension if not exists
                cur.execute("CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;")

                # Load commodity mappings from CSV
                for name, cid in self.commodity_map.items():
                    cur.execute("""
                        INSERT INTO commodity_lookup (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (cid, name))
                
                # Create snapshot function with hardcoded connection
                cur.execute(f"""
                    DROP FUNCTION IF EXISTS public.take_price_snapshot();
                    
                    CREATE OR REPLACE FUNCTION public.take_price_snapshot(job_id int DEFAULT NULL, config jsonb DEFAULT NULL)
                    RETURNS INTEGER
                    LANGUAGE plpgsql
                    VOLATILE
                    PARALLEL UNSAFE
                    SECURITY DEFINER
                    SET search_path = ''
                    AS $$
                    DECLARE
                        rows_inserted INTEGER;
                    BEGIN
                        -- Take snapshot
                        WITH snapshot AS (
                            INSERT INTO public.price_history (timestamp, commodity_id, station_id, price, demand)
                            SELECT
                                NOW(),
                                m.commodity_id,
                                sc.station_id,
                                sc.sell_price,
                                sc.demand
                            FROM public.dblink('{self.db_in}',
                                'SELECT commodity_name, station_id, sell_price, demand
                                 FROM station_commodities
                                 WHERE sell_price > 0 AND demand >= 0'
                            ) AS sc(commodity_name VARCHAR, station_id BIGINT, sell_price INTEGER, demand INTEGER)
                            JOIN (VALUES {','.join(f"('{name}',{idx})" for name, idx in self.commodity_map.items())})
                                AS m(name,commodity_id) ON m.name = sc.commodity_name
                            RETURNING 1
                        )
                        SELECT COUNT(*) INTO rows_inserted FROM snapshot;

                        -- Try to compress the latest chunk
                        BEGIN
                            PERFORM _timescaledb_functions.compress_chunk(
                                chunk => (
                                    SELECT format('%I.%I', chunk_schema, chunk_name)::regclass
                                    FROM timescaledb_information.chunks
                                    WHERE hypertable_name = 'price_history'
                                    AND NOT is_compressed
                                    ORDER BY range_end DESC
                                    LIMIT 1
                                )
                            );
                        EXCEPTION WHEN OTHERS THEN
                            -- Ignore compression errors
                            NULL;
                        END;

                        RETURN COALESCE(rows_inserted, 0);
                    END;
                    $$;

                    -- Grant execute permission
                    REVOKE ALL ON FUNCTION public.take_price_snapshot(int, jsonb) FROM PUBLIC;
                    GRANT EXECUTE ON FUNCTION public.take_price_snapshot(int, jsonb) TO PUBLIC;

                    -- Ensure owner is the same as the connection user
                    ALTER FUNCTION public.take_price_snapshot(int, jsonb) OWNER TO CURRENT_USER;

                    -- Add explicit comment
                    COMMENT ON FUNCTION public.take_price_snapshot(int, jsonb) IS 'Takes a snapshot of current prices and compresses old data';
                """)
                
                conn.commit()
                log_message(GREEN, "SETUP", "Database initialized successfully")
        except Exception as e:
            log_message(RED, "ERROR", f"Setup failed: {e}")
            sys.exit(1)

    def update_interval(self, minutes):
        """Start taking snapshots every X minutes"""
        try:
            log_message(GREEN, "START", f"Starting snapshots every {minutes} minutes")
            while True:
                # Take snapshot and get stats
                rows = self.take_snapshot()
                self.monitor()
                
                # Sleep for the interval
                time.sleep(minutes * 60)
        except KeyboardInterrupt:
            log_message(YELLOW, "STOP", "Snapshot loop stopped by user")
        except Exception as e:
            log_message(RED, "ERROR", f"Snapshot loop failed: {e}")

    def stop(self):
        """This is now a no-op since we're not using TimescaleDB jobs"""
        log_message(YELLOW, "STOP", "No background jobs to stop - use Ctrl+C to stop the snapshot loop")

    def take_snapshot(self):
        try:
            with psycopg2.connect(self.db_history) as conn, conn.cursor() as cur:
                cur.execute("SELECT take_price_snapshot();")
                rows = cur.fetchone()[0]
                log_message(GREEN, "SNAPSHOT", f"Inserted {rows} records")
                return rows
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to take snapshot: {e}")
            return 0

    def monitor(self):
        try:
            with psycopg2.connect(self.db_history) as conn, conn.cursor() as cur:
                cur.execute("""
                    WITH stats AS (
                        SELECT
                            (SELECT COUNT(*) FROM price_history) as total_rows,
                            (SELECT MAX(timestamp) FROM price_history) as latest_ts,
                            COUNT(*) as total_chunks,
                            COUNT(*) FILTER (WHERE is_compressed) as compressed_chunks
                        FROM timescaledb_information.chunks
                        WHERE hypertable_schema = 'public' 
                        AND hypertable_name = 'price_history'
                    ),
                    compression_stats AS (
                        SELECT
                            SUM(before_compression_table_bytes) as before_table_bytes,
                            SUM(before_compression_index_bytes) as before_index_bytes,
                            SUM(before_compression_toast_bytes) as before_toast_bytes,
                            SUM(before_compression_total_bytes) as before_total_bytes,
                            SUM(after_compression_table_bytes) as after_table_bytes,
                            SUM(after_compression_index_bytes) as after_index_bytes,
                            SUM(after_compression_toast_bytes) as after_toast_bytes,
                            SUM(after_compression_total_bytes) as after_total_bytes
                        FROM chunk_compression_stats('price_history')
                    ),
                    latest_chunk_stats AS (
                        SELECT 
                            c.is_compressed as compression_status,
                            cs.before_compression_table_bytes,
                            cs.before_compression_index_bytes,
                            cs.before_compression_toast_bytes,
                            cs.before_compression_total_bytes,
                            cs.after_compression_table_bytes,
                            cs.after_compression_index_bytes,
                            cs.after_compression_toast_bytes,
                            cs.after_compression_total_bytes
                        FROM timescaledb_information.chunks c
                        LEFT JOIN chunk_compression_stats('price_history') cs 
                            ON cs.chunk_schema = c.chunk_schema 
                            AND cs.chunk_name = c.chunk_name
                        WHERE c.hypertable_schema = 'public'
                        AND c.hypertable_name = 'price_history'
                        ORDER BY c.range_end DESC
                        LIMIT 1
                    ),
                    current_size AS (
                        SELECT
                            table_bytes,
                            index_bytes,
                            toast_bytes,
                            total_bytes
                        FROM hypertable_detailed_size('price_history')
                    )
                    SELECT
                        s.total_rows,
                        s.latest_ts,
                        s.total_chunks,
                        s.compressed_chunks,
                        pg_size_pretty(cs.before_table_bytes) as before_table_size,
                        pg_size_pretty(cs.before_index_bytes) as before_index_size,
                        pg_size_pretty(cs.before_toast_bytes) as before_toast_size,
                        pg_size_pretty(cs.before_total_bytes) as before_total_size,
                        pg_size_pretty(cs.after_table_bytes) as after_table_size,
                        pg_size_pretty(cs.after_index_bytes) as after_index_size,
                        pg_size_pretty(cs.after_toast_bytes) as after_toast_size,
                        pg_size_pretty(cs.after_total_bytes) as after_total_size,
                        CASE 
                            WHEN cs.before_total_bytes > 0 
                            THEN round((cs.after_total_bytes::numeric / cs.before_total_bytes::numeric * 100)::numeric, 2)
                            ELSE NULL
                        END as compression_ratio,
                        CASE 
                            WHEN lc.compression_status THEN 'Compressed'
                            ELSE 'Uncompressed'
                        END as latest_chunk_status,
                        pg_size_pretty(lc.before_compression_total_bytes) as latest_chunk_before,
                        pg_size_pretty(lc.after_compression_total_bytes) as latest_chunk_after,
                        CASE 
                            WHEN lc.before_compression_total_bytes > 0 
                            THEN round((lc.after_compression_total_bytes::numeric / lc.before_compression_total_bytes::numeric * 100)::numeric, 2)
                            ELSE NULL
                        END as latest_chunk_ratio,
                        pg_size_pretty(c.table_bytes) as current_table_size,
                        pg_size_pretty(c.index_bytes) as current_index_size,
                        pg_size_pretty(c.toast_bytes) as current_toast_size,
                        pg_size_pretty(c.total_bytes) as current_total_size
                    FROM stats s
                    CROSS JOIN compression_stats cs
                    LEFT JOIN latest_chunk_stats lc ON true
                    CROSS JOIN current_size c;
                """)
                stats = cur.fetchone()
                if stats:
                    log_message(BLUE, "STATS", f"""
Database Statistics:
-------------------
Total Records: {stats[0]:,}
Latest Snapshot: {stats[1]}

Chunk Information:
-----------------
Total Chunks: {stats[2]} (Compressed: {stats[3]})
Latest Chunk Status: {stats[13] or 'Uncompressed'}

Compression Overview:
-------------------
Before Compression:
  - Table: {stats[4]}
  - Index: {stats[5]}
  - Toast: {stats[6]}
  - Total: {stats[7]}

After Compression:
  - Table: {stats[8]}
  - Index: {stats[9]}
  - Toast: {stats[10]}
  - Total: {stats[11]}

Overall Compression Ratio: {stats[12]}%

Current Database Size:
--------------------
Table Size: {stats[17]}
Index Size: {stats[18]}
Toast Size: {stats[19]}
Total Size: {stats[20]}

Latest Chunk:
------------
Before Compression: {stats[14] or 'N/A'}
After Compression: {stats[15] or 'N/A'}
Compression Ratio: {stats[16] or 'N/A'}%
""")
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get stats: {e}")

    def compress(self):
        try:
            with psycopg2.connect(self.db_history) as conn, conn.cursor() as cur:
                # Get current sizes
                cur.execute("""
                    SELECT
                        chunk_schema || '.' || chunk_name as chunk,
                        pg_size_pretty(before_compression_total_bytes) as before_size,
                        pg_size_pretty(after_compression_total_bytes) as after_size,
                        round(100 - (after_compression_total_bytes::numeric / 
                            NULLIF(before_compression_total_bytes, 0) * 100), 2) as savings
                    FROM chunk_compression_stats('price_history')
                    ORDER BY chunk;
                """)
                before_stats = cur.fetchall()
                
                # Compress all uncompressed chunks
                cur.execute("""
                    SELECT compress_chunk(
                        format('%I.%I', chunk_schema, chunk_name)::regclass
                    )
                    FROM timescaledb_information.chunks
                    WHERE hypertable_name = 'price_history'
                    AND NOT is_compressed;
                """)
                
                # Get new sizes
                cur.execute("""
                    SELECT
                        chunk_schema || '.' || chunk_name as chunk,
                        pg_size_pretty(before_compression_total_bytes) as before_size,
                        pg_size_pretty(after_compression_total_bytes) as after_size,
                        round(100 - (after_compression_total_bytes::numeric / 
                            NULLIF(before_compression_total_bytes, 0) * 100), 2) as savings
                    FROM chunk_compression_stats('price_history')
                    ORDER BY chunk;
                """)
                after_stats = cur.fetchall()
                
                # Report changes
                log_message(BLUE, "COMPRESSION", "Chunk compression stats:")
                for chunk in after_stats:
                    log_message(BLUE, chunk[0], 
                              f"Before: {chunk[1]}, After: {chunk[2]}, Savings: {chunk[3]}%")
                
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to compress chunks: {e}")

    def get_data_timespan(self):
        """Get information about the data timespan and database sizes."""
        try:
            with psycopg2.connect(self.db_history) as conn, conn.cursor() as cur:
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
                        t.total_rows,
                        s.total_bytes,
                        s.table_bytes + s.toast_bytes as data_bytes,
                        s.index_bytes
                    FROM timespan t
                    CROSS JOIN size_info s;
                """)
                r = cur.fetchone()
                if r and r[0] and r[1]:
                    return {
                        'oldest': r[0],
                        'newest': r[1],
                        'timespan': (r[1] - r[0]),
                        'total_size': r[2],
                        'data_size': r[3],
                        'index_size': r[4],
                        'total_rows': r[5],
                        'total_bytes': r[6],
                        'data_bytes': r[7],
                        'index_bytes': r[8]
                    }
                return None
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get data timespan: {e}")
            return None

    def get_price_history(self, station_id=None, commodity_name=None, start_time=None, hours=24):
        """Get price history for specified station and/or commodity.
        
        Args:
            station_id: Optional station ID to filter by
            commodity_name: Optional commodity name to filter by
            start_time: Optional start time, defaults to now - hours
            hours: Number of hours to look back (default 24)
            
        Returns:
            List of price history records with price changes and time deltas
        """
        try:
            with psycopg2.connect(self.db_history) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
                query = """
                    WITH history AS (
                        SELECT
                            ph.timestamp,
                            cl.name as commodity_name,
                            ph.station_id,
                            ph.price,
                            ph.demand,
                            LAG(ph.price) OVER w as prev_price,
                            LAG(ph.timestamp) OVER w as prev_timestamp
                        FROM price_history ph
                        JOIN commodity_lookup cl ON cl.id = ph.commodity_id
                        WHERE ph.timestamp > %s
                        WINDOW w AS (
                            PARTITION BY ph.station_id, ph.commodity_id
                            ORDER BY ph.timestamp
                        )
                    )
                    SELECT
                        timestamp,
                        commodity_name,
                        station_id,
                        price,
                        demand,
                        price - prev_price as price_change,
                        timestamp - prev_timestamp as time_since_last
                    FROM history
                    WHERE 1=1
                """
                params = [start_time or (datetime.now() - timedelta(hours=hours))]
                
                if station_id:
                    query += " AND station_id = %s"
                    params.append(station_id)
                if commodity_name:
                    query += " AND commodity_name = %s"
                    params.append(commodity_name)
                
                query += " ORDER BY timestamp DESC"
                
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to get price history: {e}")
            return None

    def recompress(self):
        """Recompress all chunks by decompressing and compressing them."""
        conn = None
        try:
            conn = psycopg2.connect(self.db_history)
            conn.autocommit = True
            
            with conn.cursor() as cur:
                # Get list of compressed chunks first
                cur.execute("""
                    SELECT 
                        c.chunk_schema, 
                        c.chunk_name,
                        pg_size_pretty(cs.before_compression_total_bytes) as before_size,
                        pg_size_pretty(cs.after_compression_total_bytes) as after_size
                    FROM timescaledb_information.chunks c
                    LEFT JOIN chunk_compression_stats('price_history') cs 
                        ON cs.chunk_schema = c.chunk_schema 
                        AND cs.chunk_name = c.chunk_name
                    WHERE c.hypertable_name = 'price_history'
                    AND c.is_compressed = true
                    ORDER BY c.range_end;
                """)
                chunks = cur.fetchall()
                total_chunks = len(chunks)
                
                if total_chunks == 0:
                    log_message(YELLOW, "RECOMPRESSION", "No compressed chunks found to recompress")
                    return
                
                log_message(BLUE, "RECOMPRESSION", f"Starting recompression of {total_chunks} chunks...")
                
                # Process each chunk individually
                for i, (schema, name, before_size, after_size) in enumerate(chunks, 1):
                    chunk_id = f"{schema}.{name}"
                    log_message(BLUE, "PROGRESS", f"Processing chunk {i}/{total_chunks}: {chunk_id} (Currently: {after_size} compressed from {before_size})")
                    
                    try:
                        # First decompress
                        cur.execute(f"""
                            SELECT decompress_chunk('{chunk_id}'::regclass);
                        """)
                        
                        # Get uncompressed size from chunk_size
                        cur.execute(f"""
                            SELECT pg_size_pretty(pg_relation_size('{chunk_id}'::regclass))
                        """)
                        uncompressed_size = cur.fetchone()[0]
                        log_message(GREEN, "PROGRESS", f"Decompressed chunk {i}/{total_chunks} (Size: {uncompressed_size})")
                        
                        # Then compress
                        cur.execute(f"""
                            SELECT compress_chunk('{chunk_id}'::regclass);
                        """)
                        
                        # Get new compression stats
                        cur.execute(f"""
                            SELECT 
                                pg_size_pretty(before_compression_total_bytes) as before_size,
                                pg_size_pretty(after_compression_total_bytes) as after_size,
                                round(100 - (after_compression_total_bytes::numeric / 
                                    NULLIF(before_compression_total_bytes, 0) * 100), 2) as savings
                            FROM chunk_compression_stats('price_history')
                            WHERE chunk_schema = %s AND chunk_name = %s;
                        """, (schema, name))
                        stats = cur.fetchone()
                        if stats:
                            log_message(GREEN, "SUCCESS", 
                                f"Recompressed chunk {i}/{total_chunks} "
                                f"(Before: {stats[0]}, After: {stats[1]}, Savings: {stats[2]}%)")
                        
                    except Exception as chunk_error:
                        log_message(RED, "WARNING", f"Failed to process chunk {chunk_id}: {chunk_error}")
                        continue
                
                # Show final stats
                log_message(GREEN, "COMPLETE", f"Finished processing {total_chunks} chunks")
                self.monitor()
                
        except KeyboardInterrupt:
            log_message(YELLOW, "INTERRUPTED", "Recompression interrupted by user")
        except Exception as e:
            log_message(RED, "ERROR", f"Failed to recompress chunks: {e}")
            self.monitor()
        finally:
            if conn:
                conn.close()
            sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description='Price history management')
    parser.add_argument('--db-in', required=True, help='Input database URL')
    parser.add_argument('--db-history', required=True, help='History database URL')
    parser.add_argument('--setup', action='store_true', help='Setup database schema')
    parser.add_argument('--interval', type=int, help='Take snapshots every X minutes')
    parser.add_argument('--monitor', action='store_true', help='Monitor snapshots')
    parser.add_argument('--early', action='store_true', help='Take immediate snapshot')
    parser.add_argument('--compress', action='store_true', help='Force compression')
    parser.add_argument('--recompress', action='store_true', help='Recompress all chunks')
    parser.add_argument('--get-history', action='store_true', help='Get price history')
    parser.add_argument('--station', type=int, help='Station ID for price history')
    parser.add_argument('--commodity', help='Commodity name for price history')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back (default: 24)')

    args = parser.parse_args()
    manager = HistoryManager(args.db_in, args.db_history)

    # Handle each operation and exit immediately after
    if args.setup:
        manager.setup()
        sys.exit(0)
    if args.early:
        manager.take_snapshot()
        manager.monitor()
        sys.exit(0)
    if args.compress:
        manager.compress()
        sys.exit(0)
    if args.recompress:
        manager.recompress()
        sys.exit(0)
    if args.get_history:
        history = manager.get_price_history(
            station_id=args.station,
            commodity_name=args.commodity,
            hours=args.hours
        )
        if history:
            print("\nPrice History:")
            print("-" * 80)
            print(f"{'Timestamp':<25} {'Price':<10} {'Demand':<10} {'Change':<10} {'Time Since Last'}")
            print("-" * 80)
            for record in history:
                time_since = str(record['time_since_last']).split('.')[0] if record['time_since_last'] else 'N/A'
                print(f"{record['timestamp']!s:<25} {record['price']:<10} {record['demand']:<10} "
                      f"{record['price_change'] if record['price_change'] is not None else 'N/A':<10} {time_since}")
        sys.exit(0)
    if args.interval:
        manager.update_interval(args.interval)
        sys.exit(0)
    elif args.monitor:
        try:
            while True:
                manager.monitor()
                time.sleep(30)
        except KeyboardInterrupt:
            sys.exit(0)
    
    # If no operation specified, show help
    parser.print_help()
    sys.exit(1)

if __name__ == "__main__":
    main()

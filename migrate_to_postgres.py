import sqlite3
import psycopg2
from psycopg2.extras import DictCursor, execute_values
import os
import sys
from tqdm import tqdm

def create_postgres_schema(pg_conn):
    """Create the PostgreSQL database schema with all necessary constraints"""
    with pg_conn.cursor() as cur:
        # Drop existing tables if they exist
        cur.execute("""
            DROP TABLE IF EXISTS station_commodities CASCADE;
            DROP TABLE IF EXISTS stations CASCADE;
            DROP TABLE IF EXISTS mineral_signals CASCADE;
            DROP TABLE IF EXISTS systems CASCADE;
        """)
        
        # Create systems table
        cur.execute("""
            CREATE TABLE systems (
                id64 BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                x DOUBLE PRECISION,
                y DOUBLE PRECISION,
                z DOUBLE PRECISION,
                controlling_power TEXT,
                power_state TEXT,
                powers_acquiring JSONB,
                distance_from_sol DOUBLE PRECISION,
                CONSTRAINT unique_system_name UNIQUE (name)
            )
        """)
        
        # Create mineral_signals table with NULL allowed for appropriate fields
        cur.execute("""
            CREATE TABLE mineral_signals (
                system_id64 BIGINT NOT NULL,
                body_name TEXT NOT NULL,
                ring_name TEXT NOT NULL,
                ring_type TEXT NOT NULL,
                mineral_type TEXT,
                signal_count INTEGER,
                reserve_level TEXT,
                CONSTRAINT pk_mineral_signals PRIMARY KEY (system_id64, body_name, ring_name, ring_type),
                CONSTRAINT fk_mineral_signals_system FOREIGN KEY (system_id64) 
                    REFERENCES systems(id64) ON DELETE CASCADE
            )
        """)
        
        # Create stations table with NULL allowed for appropriate fields
        cur.execute("""
            CREATE TABLE stations (
                system_id64 BIGINT NOT NULL,
                station_id BIGINT,
                body TEXT,
                station_name TEXT NOT NULL,
                station_type TEXT,
                primary_economy TEXT,
                distance_to_arrival DOUBLE PRECISION,
                landing_pad_size TEXT,
                update_time TIMESTAMP,
                CONSTRAINT pk_stations PRIMARY KEY (system_id64, station_name),
                CONSTRAINT fk_stations_system FOREIGN KEY (system_id64) 
                    REFERENCES systems(id64) ON DELETE CASCADE
            )
        """)
        
        # Create station_commodities table with NULL allowed for appropriate fields
        cur.execute("""
            CREATE TABLE station_commodities (
                system_id64 BIGINT NOT NULL,
                station_id BIGINT,
                station_name TEXT NOT NULL,
                commodity_name TEXT NOT NULL,
                sell_price INTEGER,
                demand INTEGER,
                CONSTRAINT pk_station_commodities PRIMARY KEY (system_id64, station_name, commodity_name),
                CONSTRAINT fk_station_commodities FOREIGN KEY (system_id64, station_name) 
                    REFERENCES stations(system_id64, station_name) ON DELETE CASCADE
            )
        """)
        
        # Create indexes for performance
        cur.execute("""
            CREATE INDEX idx_systems_name ON systems(name);
            CREATE INDEX idx_systems_coords ON systems(x, y, z);
            CREATE INDEX idx_systems_powers_acquiring ON systems USING GIN (powers_acquiring);
            CREATE INDEX idx_mineral_signals_type ON mineral_signals(mineral_type);
            CREATE INDEX idx_station_commodities_price ON station_commodities(commodity_name, sell_price DESC);
        """)
        
        pg_conn.commit()

def migrate_data(sqlite_path, pg_conn):
    """Migrate data from SQLite to PostgreSQL"""
    # Connect to SQLite database with binary mode
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.text_factory = bytes
    sqlite_cur = sqlite_conn.cursor()
    
    def clean_text(value):
        """Clean and decode text values, handling encoding issues"""
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    return value.decode('latin1')
                except UnicodeDecodeError:
                    return value.decode('utf-8', errors='replace')
        return value

    def clean_row(row):
        """Clean all text values in a row"""
        return tuple(clean_text(value) if isinstance(value, bytes) else value for value in row)
    
    def deduplicate_rows(rows, key_indices):
        """Deduplicate rows based on key columns, keeping the last occurrence"""
        seen = {}
        for row in rows:
            key = tuple(row[i] for i in key_indices)
            seen[key] = row
        return list(seen.values())
    
    try:
        # Migrate systems
        print("Migrating systems table...")
        sqlite_cur.execute("SELECT id64, name, x, y, z, controlling_power, power_state, powers_acquiring, distance_from_sol FROM systems")
        systems = [clean_row(row) for row in sqlite_cur.fetchall()]
        # Deduplicate on id64 (index 0)
        systems = deduplicate_rows(systems, [0])
        
        with pg_conn.cursor() as pg_cur:
            execute_values(
                pg_cur,
                """
                INSERT INTO systems (id64, name, x, y, z, controlling_power, power_state, powers_acquiring, distance_from_sol) 
                VALUES %s 
                ON CONFLICT (id64) DO UPDATE SET 
                    name = EXCLUDED.name,
                    x = EXCLUDED.x,
                    y = EXCLUDED.y,
                    z = EXCLUDED.z,
                    controlling_power = EXCLUDED.controlling_power,
                    power_state = EXCLUDED.power_state,
                    powers_acquiring = EXCLUDED.powers_acquiring,
                    distance_from_sol = EXCLUDED.distance_from_sol
                """,
                systems,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
        
        # Migrate mineral_signals
        print("Migrating mineral_signals table...")
        sqlite_cur.execute("SELECT system_id64, body_name, ring_name, ring_type, mineral_type, signal_count, reserve_level FROM mineral_signals")
        signals = [clean_row(row) for row in sqlite_cur.fetchall()]
        # Deduplicate on primary key columns (system_id64, body_name, ring_name, ring_type)
        signals = deduplicate_rows(signals, [0, 1, 2, 3])
        
        with pg_conn.cursor() as pg_cur:
            execute_values(
                pg_cur,
                """
                INSERT INTO mineral_signals 
                (system_id64, body_name, ring_name, ring_type, mineral_type, signal_count, reserve_level) 
                VALUES %s 
                ON CONFLICT (system_id64, body_name, ring_name, ring_type) DO UPDATE SET 
                    mineral_type = EXCLUDED.mineral_type,
                    signal_count = EXCLUDED.signal_count,
                    reserve_level = EXCLUDED.reserve_level
                """,
                signals,
                template="(%s, %s, %s, %s, %s, %s, %s)"
            )
        
        # Migrate stations
        print("Migrating stations table...")
        sqlite_cur.execute("SELECT system_id64, station_name, station_type, distance_to_arrival, landing_pad_size, update_time FROM stations")
        stations = [clean_row(row) for row in sqlite_cur.fetchall()]
        # Deduplicate on primary key columns (system_id64, station_name)
        stations = deduplicate_rows(stations, [0, 1])
        
        with pg_conn.cursor() as pg_cur:
            execute_values(
                pg_cur,
                """
                INSERT INTO stations 
                (system_id64, station_name, station_type, distance_to_arrival, landing_pad_size, update_time) 
                VALUES %s 
                ON CONFLICT (system_id64, station_name) DO UPDATE SET 
                    station_type = EXCLUDED.station_type,
                    distance_to_arrival = EXCLUDED.distance_to_arrival,
                    landing_pad_size = EXCLUDED.landing_pad_size,
                    update_time = EXCLUDED.update_time
                """,
                stations,
                template="(%s, %s, %s, %s, %s, %s)"
            )
        
        # Migrate station_commodities
        print("Migrating station_commodities table...")
        sqlite_cur.execute("""
            SELECT sc.system_id64, sc.station_name, sc.commodity_name, sc.sell_price, sc.demand 
            FROM station_commodities sc
            INNER JOIN stations s ON sc.system_id64 = s.system_id64 AND sc.station_name = s.station_name
        """)
        commodities = [clean_row(row) for row in sqlite_cur.fetchall()]
        # Deduplicate on primary key columns (system_id64, station_name, commodity_name)
        commodities = deduplicate_rows(commodities, [0, 1, 2])
        
        with pg_conn.cursor() as pg_cur:
            execute_values(
                pg_cur,
                """
                INSERT INTO station_commodities 
                (system_id64, station_name, commodity_name, sell_price, demand) 
                VALUES %s 
                ON CONFLICT (system_id64, station_name, commodity_name) DO UPDATE SET 
                    sell_price = EXCLUDED.sell_price,
                    demand = EXCLUDED.demand
                """,
                commodities,
                template="(%s, %s, %s, %s, %s)"
            )
        
        pg_conn.commit()
        print("Data migration completed successfully!")
        
    finally:
        sqlite_conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python migrate_to_postgres.py <sqlite_db_path>")
        sys.exit(1)

    sqlite_path = sys.argv[1]
    if not os.path.exists(sqlite_path):
        print(f"SQLite database not found: {sqlite_path}")
        sys.exit(1)

    # Get PostgreSQL connection details from environment
    pg_url = os.getenv('DATABASE_URL', 'postgresql://postgres:elephant9999!@localhost:5432/power_mining')
    
    try:
        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(pg_url)
        
        print("Creating PostgreSQL schema...")
        create_postgres_schema(pg_conn)
        
        print("Starting data migration...")
        migrate_data(sqlite_path, pg_conn)
        
        print("Migration completed successfully!")
        
    except Exception as e:
        print(f"Error during migration: {str(e)}")
        sys.exit(1)
    finally:
        if 'pg_conn' in locals():
            pg_conn.close() 
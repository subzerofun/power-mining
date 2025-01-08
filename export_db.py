import os
import subprocess
from datetime import datetime

def export_database():
    # Get database URL from environment
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:elephant9999!@localhost:5432/power_mining')
    
    # Create dumps directory if it doesn't exist
    if not os.path.exists('dumps'):
        os.makedirs('dumps')
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dump_file = f'dumps/power_mining_{timestamp}.sql'
    
    try:
        # Parse connection details from URL
        db_parts = db_url.replace('postgresql://', '').split('@')
        user_pass = db_parts[0].split(':')
        host_db = db_parts[1].split('/')
        
        username = user_pass[0]
        password = user_pass[1]
        host = host_db[0].split(':')[0]
        port = host_db[0].split(':')[1] if ':' in host_db[0] else '5432'
        database = host_db[1]
        
        # Set PGPASSWORD environment variable
        os.environ['PGPASSWORD'] = password
        
        # Find pg_dump in default PostgreSQL installation directory
        pg_dump_paths = [
            r'C:\Program Files\PostgreSQL\17\bin\pg_dump.exe',
            r'C:\Program Files\PostgreSQL\16\bin\pg_dump.exe'
        ]
        
        pg_dump_path = None
        for path in pg_dump_paths:
            if os.path.exists(path):
                pg_dump_path = path
                break
                
        if not pg_dump_path:
            raise FileNotFoundError("Could not find pg_dump.exe in standard PostgreSQL installation directories")
        
        # Create dump command with full path
        dump_cmd = [
            pg_dump_path,
            '-h', host,
            '-p', port,
            '-U', username,
            '-d', database,
            '-F', 'p',  # plain text format
            '-f', dump_file,
            '--no-owner',  # skip ownership commands
            '--no-privileges'  # skip privilege commands
        ]
        
        # Execute dump
        print(f"Exporting database to {dump_file}...")
        subprocess.run(dump_cmd, check=True)
        print("Export completed successfully!")
        
        # Create import instructions
        with open('dumps/IMPORT_INSTRUCTIONS.txt', 'w') as f:
            f.write("""
PostgreSQL Database Import Instructions:

1. Local Import:
   psql -U your_user -d power_mining -f dump_file.sql

2. Appliku Import:
   a. Go to your Appliku dashboard
   b. Select your project
   c. Go to the Database section
   d. Choose "Import Database"
   e. Upload the .sql file
   f. Click "Import"

Note: Make sure your DATABASE_URL environment variable is set correctly in Appliku.
""")
        
        return dump_file
        
    except Exception as e:
        print(f"Error during export: {str(e)}")
        return None

if __name__ == "__main__":
    export_database() 
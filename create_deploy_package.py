import os
import shutil
import zipfile
from datetime import datetime

def create_deploy_package():
    # Create timestamp for the package
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    deploy_dir = f'deploy_package_{timestamp}'
    
    # Create deploy directory
    os.makedirs(deploy_dir, exist_ok=True)
    
    # Files to include
    files_to_copy = [
        'server_web.py',
        'gunicorn_config.py',
        'requirements.txt',
        'mining_data.py',
        'res_data.py',
        'update_live_web.py',
        # Add any other necessary files
    ]
    
    # Copy files
    print("Copying files...")
    for file in files_to_copy:
        if os.path.exists(file):
            shutil.copy2(file, deploy_dir)
    
    # Create Procfile for Appliku
    print("Creating Procfile...")
    with open(os.path.join(deploy_dir, 'Procfile'), 'w') as f:
        f.write("""web: gunicorn -c gunicorn_config.py server_web:create_app()
updater: python update_live_web.py --auto
""")
    
    # Create runtime.txt
    print("Creating runtime.txt...")
    with open(os.path.join(deploy_dir, 'runtime.txt'), 'w') as f:
        f.write('python-3.11.5')
    
    # Create deployment instructions
    print("Creating deployment instructions...")
    with open(os.path.join(deploy_dir, 'DEPLOY_INSTRUCTIONS.txt'), 'w') as f:
        f.write("""Appliku Deployment Instructions:

1. Database Setup:
   - Create a PostgreSQL database in Appliku
   - Note down the DATABASE_URL

2. Environment Variables:
   Set the following in Appliku:
   - DATABASE_URL=<your-postgres-url>
   - ENABLE_LIVE_UPDATE=true
   - WEBSOCKET_HOST=0.0.0.0
   - WEBSOCKET_PORT=8765

3. Deployment:
   - Upload this package to Appliku
   - Deploy using the provided Procfile
   - Verify both web and updater services are running

4. Database Migration:
   - Use migrate_to_postgres.py to migrate your data
   - Or import the provided database dump

Note: Make sure both web and updater services are enabled in Appliku.
""")
    
    # Create zip file
    zip_filename = f'{deploy_dir}.zip'
    print(f"Creating zip file {zip_filename}...")
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(deploy_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, deploy_dir)
                zipf.write(file_path, arcname)
    
    # Cleanup
    shutil.rmtree(deploy_dir)
    
    print(f"\nDeployment package created: {zip_filename}")
    print("Follow the instructions in DEPLOY_INSTRUCTIONS.txt inside the zip file.")

if __name__ == "__main__":
    create_deploy_package() 
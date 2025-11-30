import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                print(f"PostgreSQL is ready on host {host}.")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error details: {e}")
            retries += 1
            print(f"PostgreSQL is not ready on host {host}. Attempt {retries}/{max_retries}. Retrying in {delay_seconds} seconds...")
            time.sleep(delay_seconds)
    print(f"PostgreSQL is not ready on host {host} after {max_retries} attempts.")
    return False

source_config = {
    'dbname': 'source_db',
    'user': 'source_user',
    'password': 'source_password',
    'host': 'source_postgres',
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'destination_user',
    'password': 'destination_password',
    'host': 'destination_postgres',
}
print("Waiting for source PostgreSQL...")
if not wait_for_postgres(source_config['host']):
    exit(1)

print("Waiting for destination PostgreSQL...")
if not wait_for_postgres(destination_config['host']):
    exit(1)

print("Starting ELT process...")

# Commande pg_dump pour exporter la base de données source
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD=source_config['password'])
subprocess.run(dump_command, env=subprocess_env, check=True)

# Commande pour charger les données dans la destination
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql',
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])
subprocess.run(load_command, env=subprocess_env, check=True)

print("ELT process completed successfully.")
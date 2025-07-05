import os
import yaml
import snowflake.connector

# Path to dbt profiles.yml
DBT_PROFILES_PATH = os.path.expanduser('~/.dbt/profiles.yml')
DBT_PROFILE_NAME = 'dev'  # Updated to match your profiles.yml
DBT_TARGET_NAME = 'dev'   # Target name remains 'dev'

# Read dbt profiles.yml
with open(DBT_PROFILES_PATH, 'r') as f:
    profiles = yaml.safe_load(f)

profile = profiles[DBT_PROFILE_NAME]
target = profile['outputs'][DBT_TARGET_NAME]

# Extract credentials
user = target['user']
password = target['password']
account = target['account']
database = target['database']
schema = target['schema']
warehouse = target['warehouse']
role = target.get('role')

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    database=database,
    schema=schema,
    warehouse=warehouse,
    role=role
)

query = '''
select
  route,
  total_co2_tonnes
from mart_route_analysis
order by total_co2_tonnes desc nulls last
limit 1;
'''

try:
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        if result and result[0] is not None and result[1] is not None:
            print(f"Route with highest CO2 emissions: {result[0]} ({result[1]:,.2f} tonnes)")
        elif result and result[0] is not None:
            print(f"Route with highest CO2 emissions: {result[0]} (CO2 value is null)")
        else:
            print("No data found.")
finally:
    conn.close() 
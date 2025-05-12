from influxdb import InfluxDBClient
import csv
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from dataclasses import dataclass
import os
import time

import dotenv

dotenv.load_dotenv()


# InfluxDB connection details
INFLUXDB_CONFIG = {
    'host': '192.168.0.108',
    'port': 8086,
    'username': os.getenv('INFLUXDB_USERNAME'),
    'password': os.getenv('INFLUXDB_PASSWORD'),
    'database': 'homeassistant'
}

print(os.getenv('INFLUXDB_USERNAME'))
# Default output directory
DEFAULT_OUTPUT_DIR = '/mnt/c/Users/patri/OneDrive/Dokumenty/bakalarka/USECASE1_tepelne_cerpadla/data/sarze_4/'

START_DATE = datetime(2025, 3, 15)
END_DATE = datetime(2025, 4, 23)

print(START_DATE)
print(END_DATE)
@dataclass
class SensorConfig:
    """Configuration for a single sensor."""
    name: str
    measurement: str
    description: str
    days: int = 5  # Default number of days to fetch
    output_dir: str = DEFAULT_OUTPUT_DIR  # Default output directory
    ignore: bool = False  # Whether to ignore this sensor

# Heat pump temperature suffixes and descriptions
HEATPUMP_IDS = range(4)                             # 0, 1, 2, 3
TEMP_SUFFIXES = [#{"suffix":"tcj", "description":"teplota kondenzátoru","measurement":"°C"},
                 
                #{"suffix":"tl2", "description":"teplota na spodním výparníku","measurement":"°C"},

                #{"suffix":"td", "description":"teplota na výtlaku","measurement":"°C"},
                   
                {"suffix":"tl", "description":"teplota na horním výparníku","measurement":"°C"}
                 
                ]


# Configuration for all sensors
#days only valid if START_DATE is not set
SENSOR_CONFIGS = [
    SensorConfig(
        name='xcc_venkovni_teplota_nefiltrovana',
        measurement='°C',
        description='TCE unfiltered outdoor temperature',
        output_dir=DEFAULT_OUTPUT_DIR + 'teplota/',
        days=5,
        ignore=True
    ),
    SensorConfig(
        name='gw1100a_outdoor_temperature',
        measurement='°C',
        description='JOM outdoor temperature',
        output_dir=DEFAULT_OUTPUT_DIR + 'teplota/',
        days=5,
        ignore=True
    ),
    SensorConfig(
        name='xcc_venkovni_teplota',
        measurement='°C',
        description='XCC filtered outdoor temperature',
        output_dir=DEFAULT_OUTPUT_DIR + 'teplota/',
        days=5,
        ignore=True
    ),
    SensorConfig(
        name='gw1100a_indoor_temperature',
        measurement='°C',
        description='JOM indoor temperature',
        output_dir=DEFAULT_OUTPUT_DIR + 'teplota/',
        days=5,
        ignore=True
    ),
] + [
    # Heat pump temperature sensors generated from suffixes and IDs
    SensorConfig(
        name=f"xcc_tcstav{hp}_{sfx['suffix']}",
        measurement=sfx['measurement'],
        description=f"XCC heat‑pump {hp} – {sfx['description']}",
        output_dir=DEFAULT_OUTPUT_DIR + f"tepelna_cerpadla/{sfx['suffix']}/",
        days=5,
        ignore=False
    )
    for hp in HEATPUMP_IDS
    for sfx in TEMP_SUFFIXES
] + [
    # Heat pump temperature sensors generated from suffixes and IDs
    SensorConfig(
        name=f"xcc_tcstav{hp}_{sfx['suffix']}",
        measurement=sfx['measurement'],
        description=f"XCC heat‑pump {hp} – {sfx['description']}",
        output_dir=DEFAULT_OUTPUT_DIR + f"tepelna_cerpadla/{sfx['suffix']}/",
        days=5,
        ignore=True
    )
    for hp in range(4)
    for sfx in [{"suffix":"fanh", "description":"rychlost vetraku","measurement":"RPM"},{"suffix":"prikon","description":"výkon, čerpadla","measurement":"kW"}]
]



def connect_to_influxdb():
    """Establish and return an InfluxDB connection."""
    return InfluxDBClient(**INFLUXDB_CONFIG)

def fetch_sensor_data(sensor: str, days: int, measurement: str) -> List[Dict]:
    """Fetch raw sensor data from InfluxDB for the given timeframe."""
    client = connect_to_influxdb()
    if END_DATE:
        end_time = END_DATE
    else:
        end_time = datetime.now(timezone.utc)
    if START_DATE:
        start_time = START_DATE
    else:
        start_time = end_time - timedelta(days=days)
    
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    query = f'''
        SELECT "value" AS "value"
        FROM "autogen"."{measurement}"  
        WHERE time > '{start_time_str}' AND time < '{end_time_str}'
        AND "entity_id"='{sensor}'
    '''
    
    result = client.query(query)
    client.close()
    return list(result.get_points())

def export_to_csv(data: List[Dict], file_path: str) -> None:
    """Export fetched data to a CSV file."""
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['time', 'value'])  # Header
        for point in data:
            writer.writerow([point['time'], point['value']])
    
    print(f'Number of data points retrieved: {len(data)}')
    print(f'Data successfully exported to {file_path}')

def download_sensor(config: SensorConfig) -> None:
    """Download data for a single sensor based on its configuration."""
    if config.ignore:
        print(f"\nSkipping {config.description} ({config.name}) - marked as ignored")
        return
        
    file_path = os.path.join(config.output_dir, f"{config.name}.csv")
    print(f"\nDownloading data for {config.description} ({config.name})")
    data = fetch_sensor_data(config.name, config.days, config.measurement)
    export_to_csv(data, file_path)

def download_all_sensors() -> None:
    """Download data for all configured sensors."""
    for config in SENSOR_CONFIGS:
        if config.ignore:
            print(f"\nSkipping {config.description} ({config.name}) - marked as ignored")
            continue
        else:
            download_sensor(config)        
            time.sleep(7)
        

def find_sensor_measurement(sensor_name: str) -> Optional[str]:
    """Find which measurement contains data for a specific sensor."""
    client = connect_to_influxdb()
    
    # First get all measurements
    measurements_query = "SHOW MEASUREMENTS"
    measurements = list(client.query(measurements_query).get_points())
    
    print(f"Searching for sensor: {sensor_name}")
    print("Checking measurements:")
    
    for measurement in measurements:
        measurement_name = measurement['name']
        # Check if the sensor exists in this measurement
        query = f'''
            SELECT count("value") 
            FROM "{measurement_name}" 
            WHERE "entity_id"='{sensor_name}'
        '''
        result = list(client.query(query).get_points())
        if result and result[0]['count'] > 0:
            print(f"Found in measurement: {measurement_name}")
            print(f"Number of data points: {result[0]['count']}")
            return measurement_name
    
    print("Sensor not found in any measurement")
    return None

if __name__ == "__main__":
    # Download data for all configured sensors
    download_all_sensors()
    
    # Example of finding a specific sensor's measurement
    # sensor_name = "xcc_venkovni_teplota_nefiltrovana"
    # find_sensor_measurement(sensor_name)
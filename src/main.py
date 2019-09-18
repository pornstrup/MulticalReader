import os
import datetime
import traceback
import schedule
import PyKamstrup.kamstrup as kamstrup
from influxdb import InfluxDBClient


DEVICE_PORT = os.environ['DEVICE_PORT']
BAUD = int(os.environ['BAUD'])

SCHEDULE = os.environ['SCHEDULE']

INFLUX_SERVER = os.environ['INFLUX_SERVER']
INFLUX_PORT = int(os.environ['INFLUX_PORT'])
INFLUX_USERNAME = os.environ['INFLUX_USERNAME']
INFLUX_PASSWORD = os.environ['INFLUX_PASSWORD'] 
INFLUX_DB = os.environ['INFLUX_DB']


def scan_and_store(timestamp):
    
    registers = kamstrup.kamstrup_MC403_var 

    with kamstrup.kamstrup(serial_port=DEVICE_PORT, baud=BAUD) as device:

        db_client = InfluxDBClient(INFLUX_SERVER, INFLUX_PORT, INFLUX_USERNAME, INFLUX_PASSWORD, INFLUX_DB)

        for register in registers:
            try:
                value, unit = device.readvar(register)

                if value != None:
                    name = registers[register]
                    time = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    reading = [{
                        "measurement": name,
                        "time": time,
                        "fields": {"value": value, "unit": unit}
                    }]
                    
                    db_client.write_points(reading)
                    
                    print(reading)
                    
                    
            except KeyboardInterrupt:
                raise
            except Exception:
                traceback.print_exc()
                pass

        db_client.close()
        

    
if __name__ == "__main__":    
    schedule.schedule_work(SCHEDULE, scan_and_store)

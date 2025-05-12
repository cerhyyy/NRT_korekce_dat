from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import json, datetime
from pyflink.common import WatermarkStrategy, Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.state import StateTtlConfig
from pyflink.datastream.functions import CoProcessFunction, RuntimeContext, KeyedCoProcessFunction, MapFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows
import sklearn
import xgboost
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
import time

import logging, os
#os.environ["PYFLINK_CLIENT_LOG_LEVEL"] = "DEBUG"
#logging.basicConfig(level=logging.DEBUG)




env = StreamExecutionEnvironment.get_execution_environment()
#env.enable_checkpointing(30_000)            # 30 s exactly‑once checkpoints
# Add the JAR file 
env.add_jars("file:///mnt/c/Users/patri/OneDrive/Dokumenty/bakalarka/USECASE1_tepelne_cerpadla/flink_connectors/flink-sql-connector-kafka-3.3.0-1.20.jar")


# Set parallelism to 1 for easier debugging
env.set_parallelism(6)

kafka_props = {'bootstrap.servers':'localhost:19092','group.id':'flink_demo'}
def parse(s):
    try:
        data = json.loads(s)
        # Parse timestamp, handling all timezone formats
        time_str = data['time']
        
        # Check if timestamp already has timezone information
        if 'Z' in time_str or '+' in time_str or '-' in time_str:
            # If it has timezone info, parse it directly
            timestamp = datetime.datetime.fromisoformat(time_str)
        else:
            # If no timezone info, assume UTC and add it
            timestamp = datetime.datetime.fromisoformat(time_str + '+00:00')
            
        # Convert to UTC if not already in UTC
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(datetime.timezone.utc)
            
        value = float(data['value'])
        source = data.get('source', '')
        processing_time_ms = float(data.get('processing_time_ms', 0.0))  
        time_at_arrival_ms = time.perf_counter()
        return timestamp, value, source, processing_time_ms, time_at_arrival_ms
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Error parsing message: {e}, message: {s[:100]}...")
        # Return default values or re-raise based on your error handling strategy
        raise

output_schema = Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.FLOAT()])
ds_temp_nefiltr = env.add_source(FlinkKafkaConsumer('temperature', SimpleStringSchema(), kafka_props)) \
          .map(parse, output_type=output_schema)

ds_temp_meteo = env.add_source(FlinkKafkaConsumer('meteo_temperature', SimpleStringSchema(), kafka_props)) \
          .map(parse, output_type=output_schema)




class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        timestamp_ms = value[0]  # Convert to microseconds
        return timestamp_ms

wm = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)) \
    .with_timestamp_assigner(EventTimestampAssigner()).with_idleness(Duration.of_minutes(3))

wm2 = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)) \
    .with_timestamp_assigner(EventTimestampAssigner()).with_idleness(Duration.of_minutes(3))

ds_temp_nefiltr = ds_temp_nefiltr.assign_timestamps_and_watermarks(wm)
ds_temp_meteo = ds_temp_meteo.assign_timestamps_and_watermarks(wm2)


combined_ds = ds_temp_nefiltr.union(ds_temp_meteo)






import json


def to_json(t):
    """
    t = (event_ts, temperature, temperature_meteo, processing_time, processing_time_meteo)
    """

    processing_time = time.perf_counter() - t[3]
    return json.dumps({
        "time": t[0].isoformat(timespec="milliseconds"),
        "temperature": t[1],
        "source": t[2],
        "processing_time": processing_time,
        "time_at_arrival": t[4],
        "start_time": t[3]    },
    allow_nan=False
    )

json_stream = combined_ds.map(to_json, output_type=Types.STRING())
json_stream.print()


from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

kafka_sink = (
    KafkaSink.builder()
             .set_bootstrap_servers("localhost:19092")
             .set_record_serializer(
                 KafkaRecordSerializationSchema.builder()
                     .set_topic("con_w_meteom2")
                     .set_value_serialization_schema(SimpleStringSchema())
                     .build()
             )
             .set_property("transaction.timeout.ms", "60000") 
             .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
             .build()
)



json_stream.sink_to(kafka_sink)



print("Starting execution")
env.execute("DEBUG")

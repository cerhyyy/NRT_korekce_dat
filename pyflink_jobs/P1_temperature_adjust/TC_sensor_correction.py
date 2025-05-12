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
env.set_parallelism(1)

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






class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        timestamp_ms = value[0]  # Convert to microseconds
        return timestamp_ms

wm = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)) \
    .with_timestamp_assigner(EventTimestampAssigner()).with_idleness(Duration.of_minutes(3))
ds_temp_nefiltr = ds_temp_nefiltr.assign_timestamps_and_watermarks(wm)



# Key by timestamp for deduplication
keyed_temp_nefiltr = ds_temp_nefiltr.key_by(lambda x: x[0])  # key by timestamp




















######################### processing ###################################


##### DEDUPLICATE #####

class DeduplicateFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("seen", Types.BOOLEAN())

        # Set TTL for state to avoid memory issues with stale data
        ttl_config = StateTtlConfig \
            .new_builder(Time.minutes(1)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()
        
        descriptor.enable_time_to_live(ttl_config)

        self.seen = runtime_context.get_state(descriptor)
        
    def process_element(self, value, ctx):
        if self.seen.value() is None or not self.seen.value():
            self.seen.update(True)
            yield value
        else:
            #print(f"the message on {value[0]} was deduplicated")
            return

dedup_temp_nefiltr = keyed_temp_nefiltr.process(DeduplicateFunction(), output_type=output_schema)


keyed_dedup_temp_nefiltr = dedup_temp_nefiltr.key_by(lambda _: 0)          # same "dummy" key for all








##### RATE CALCULATOR for temperature from TC #####

class TemperatureRateCalculatorTC(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        # Store the previous temperature reading for rate calculation
        descriptor_prev_temp = ValueStateDescriptor(
            "previous_temperature", 
            Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT()])
        )
        self.prev_temp = ctx.get_state(descriptor_prev_temp)

        # Initialize counter with default value of 0
        counter_descriptor = ValueStateDescriptor("counter", Types.FLOAT())
        self.counter = ctx.get_state(counter_descriptor)

        
        # Set TTL for state to avoid memory issues with stale data
        ttl_config = StateTtlConfig \
            .new_builder(Time.minutes(10)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()
        
        descriptor_prev_temp.enable_time_to_live(ttl_config)
        counter_descriptor.enable_time_to_live(ttl_config)
        
        # Initialize counter if not already set
        self.counter.update(0.0)
        print(f"counter_teplota_TC: {self.counter.value()}")

    def process_element(self, element, ctx):
        # Unpack the current temperature reading
        current_time, current_temp, source, processing_time, time_at_arrival_ms = element
        
        # Get the previous temperature reading
        prev_reading = self.prev_temp.value()
        
        # Calculate rate of change if we have a previous reading
        if prev_reading is not None:
            prev_time, prev_temp = prev_reading
            
            # Calculate time difference in seconds (using absolute value)
            time_diff_seconds = abs((current_time - prev_time).total_seconds())
            
            # Avoid division by zero
            if time_diff_seconds > 0:
                # Calculate rate of change in degrees per minute
                rate_of_change = (current_temp - prev_temp) / (time_diff_seconds / 60)
                
                # Only yield if rate of change is within acceptable range
                if abs(rate_of_change) <= 1.6:
                    # Update state with current reading
                    self.prev_temp.update((current_time, current_temp))
                    # Return the original element with the rate of change appended
                    yield (current_time, current_temp, source, processing_time,time_at_arrival_ms)
                else:
                    # Safely increment counter
                    current_count = self.counter.value() or 0.0
                    self.counter.update(current_count + 1.0)
                    print(f"counter: {self.counter.value()}, timestamp: {current_time}")
                    yield (current_time, prev_temp, source, processing_time,time_at_arrival_ms)
                    
            else:
                # Update state with current reading
                self.prev_temp.update((current_time, current_temp))
                yield (current_time, current_temp, source, processing_time,time_at_arrival_ms)
        else:
            # First reading, no rate of change calculation possible
            self.prev_temp.update((current_time, current_temp))
            yield (current_time, current_temp, source, processing_time,time_at_arrival_ms)

    def close(self):
        counter_value = self.counter.value()
        if counter_value is not None:
            print(f"counter: {counter_value}")

# Apply the rate calculator to the temperature stream
temp_with_rate_nefiltr = keyed_dedup_temp_nefiltr.process(
    TemperatureRateCalculatorTC(),
    output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.FLOAT()])
)




temp_with_rate_nefiltr.print()


############### SINK #############
# Kafka sink



import json


def to_json(t):
    """
    t = (event_ts, temperature, temperature_meteo, processing_time, processing_time_meteo)
    """
    current_time = time.perf_counter()
    processing_time = current_time - t[3]  

    return json.dumps({
        "time": t[0].isoformat(timespec="milliseconds"),
        "temperature": t[1],
        "processing_time": processing_time,
        "start_time": t[3]
    },
    allow_nan=False
    )

json_stream = temp_with_rate_nefiltr.map(to_json, output_type=Types.STRING())
#json_stream.print()


from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

kafka_sink = (
    KafkaSink.builder()
             .set_bootstrap_servers("localhost:19092")
             .set_record_serializer(
                 KafkaRecordSerializationSchema.builder()
                     .set_topic("con_w_meteom8")
                     .set_value_serialization_schema(SimpleStringSchema())
                     .build()
             )
             .set_property("transaction.timeout.ms", "60000") 
             .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
             .build()
)



json_stream.sink_to(kafka_sink)

























print("start...")

env.execute("A‑with‑avg‑B")

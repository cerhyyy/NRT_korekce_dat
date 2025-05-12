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

ds_temp_meteo = env.add_source(FlinkKafkaConsumer('meteo_temperature', SimpleStringSchema(), kafka_props)) \
          .map(parse, output_type=output_schema)




class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        timestamp_ms = int(value[0].timestamp()) 
        return timestamp_ms

wm = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)) \
    .with_timestamp_assigner(EventTimestampAssigner()).with_idleness(Duration.of_seconds(3))

ds_temp_nefiltr = ds_temp_nefiltr.assign_timestamps_and_watermarks(wm)
ds_temp_meteo = ds_temp_meteo.assign_timestamps_and_watermarks(wm)



# Key by timestamp for deduplication
keyed_temp_nefiltr = ds_temp_nefiltr.key_by(lambda x: x[0])  # key by timestamp
keyed_temp_meteo = ds_temp_meteo.key_by(lambda x: x[0])      # key by timestamp




















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
            print(f"the message on {value[0]} was deduplicated")
            return

dedup_temp_nefiltr = keyed_temp_nefiltr.process(DeduplicateFunction(), output_type=output_schema)
dedup_temp_meteo = keyed_temp_meteo.process(DeduplicateFunction(), output_type=output_schema)


keyed_dedup_temp_nefiltr = dedup_temp_nefiltr.key_by(lambda _: 0)          # same "dummy" key for all
keyed_dedup_temp_meteo = dedup_temp_meteo.key_by(lambda _: 1)





##### RATE CALCULATOR #####

class TemperatureRateCalculator(KeyedProcessFunction):
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
        print(f"counter: {self.counter.value()}")

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
temp_with_rate = keyed_dedup_temp_meteo.process(
    TemperatureRateCalculator(),
    output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.FLOAT()])
)













#temp_with_rate.print()
# Re-key the stream for the join with meteo data
keyed_temp_with_rate = temp_with_rate.key_by(lambda _: 0)






ALLOWED_SKEW_SEC =  5                         
ALLOWED_SKEW_MS  = ALLOWED_SKEW_SEC * 1000

ALLOWED_SKEW_SEC_METEO = 20 * 60

class WaitAndJoin(CoProcessFunction):
    def open(self, ctx):
        record_schema = Types.TUPLE([
            Types.SQL_TIMESTAMP(),  # ts
            Types.FLOAT(),          # value
            Types.STRING(),         # source
            Types.FLOAT()           # processing_time_ms
        ])


        meteo_schema = Types.TUPLE([
            Types.SQL_TIMESTAMP(),  # ts
            Types.FLOAT(),          # value
            Types.STRING(),         # source
            Types.FLOAT(),
            Types.FLOAT()])

        # ① buffer device records until their timer fires
        self.device_buf = ctx.get_map_state(
            MapStateDescriptor("device_buf",
                               Types.LONG(),          # key = ts as epoch‑ms
                               record_schema))

        # ② keep *latest* meteo record for the same key
        self.latest_meteo = ctx.get_list_state(
            ListStateDescriptor("latest_meteo", meteo_schema))

        # TTL so RocksDB doesn't grow unbounded
        ttl = (StateTtlConfig
               .new_builder(Time.minutes(1))
               .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
               .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
               .build())
        self.device_buf.enable_time_to_live(ttl)
        self.latest_meteo.enable_time_to_live(ttl)

    # -------- left stream = device temperature ---------------------------
    def process_element1(self, device, ctx):

        ts_ms = int(device[0].timestamp() * 1000)
        self.device_buf.put(ts_ms, device)
        # wait full skew before deciding
        ctx.timer_service().register_event_time_timer(ts_ms)

    def process_element2(self, meteo, ctx):




        meteo_time_at_arrival_ms = meteo[4] #system pyflink job
        meteo_time_at_arrival_ms = time.perf_counter() - meteo_time_at_arrival_ms #time of arrival in waitandjoin
        meteo = (meteo[0], meteo[1], meteo[2], meteo[3], meteo_time_at_arrival_ms)
        self.latest_meteo.add(meteo)  
    def on_timer(self, timer_ts, ctx):
       
        
        
        device_ts_ms = timer_ts
        device = self.device_buf.get(device_ts_ms) 
        if device is None:  #také zde probíhá deduplikace
            return                        

        self.device_buf.remove(device_ts_ms)

        meteo_records = list(self.latest_meteo.get())
        #print("meteo records: ", len(meteo_records))
        if meteo_records:
            # Filter records that are older than or equal to device timestamp
            valid_meteo_records = [m for m in meteo_records if m[0] <= device[0]]
            future_meteo_records = [m for m in meteo_records if m[0] > device[0]]
            
            # Sort valid records by timestamp
            valid_meteo_records.sort(key=lambda x: x[0])
            
            # Clear the state
            self.latest_meteo.clear()
            
            # Add future records to state
            # Check if future_meteo_records is not empty before adding to state
            if future_meteo_records:
                self.latest_meteo.add_all(future_meteo_records)
            
            # Add the most recent valid record to state if it exists
            if valid_meteo_records:
                self.latest_meteo.add(valid_meteo_records[-1])
            #print(f"len(valid_meteo_records): {len(valid_meteo_records)}  ")
            
            #print(f"valid_meteo_records: {valid_meteo_records}")
            if valid_meteo_records:
                # Get the most recent one (last in the list since they're ordered by timestamp)
                meteo = valid_meteo_records[-1]

                #print(f" difference: {abs((device[0] - meteo[0]).total_seconds())}")
                if abs((device[0] - meteo[0]).total_seconds()) <= ALLOWED_SKEW_SEC_METEO:
                    yield(
                        device[0], device[1], meteo[1],  # ts, dev_temp, meteo_temp
                        device[3], meteo[3],abs((device[0] - meteo[0]).total_seconds()),meteo[4]            # processing latencies
                    )
                else:
                    #print(f"not joined because of time difference {abs((device[0] - meteo[0]).total_seconds())}")
                    yield(
                        device[0], device[1], device[1], device[3], 0.0,abs((device[0] - meteo[0]).total_seconds()),meteo[4]
                    )
            else:
                #print(f"no valid meteo records")
                yield(
                    device[0], device[1], device[1], device[3], 0.0,0.0,0.0
                )
        else:
            #print(f"no meteo records")
            yield(
                device[0], device[1], device[1], device[3], 0.0,0.0,0.0
            )



joined_temp_nefiltr = keyed_dedup_temp_nefiltr.connect(keyed_temp_with_rate).process(WaitAndJoin(),
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
joined_temp_nefiltr.print()



############### SINK #############
# Kafka sink



import json


def to_json(t):
    """
    t = (event_ts, temperature, temperature_meteo, processing_time, processing_time_meteo)
    """
    current_time = time.perf_counter()
    processing_time = current_time - t[3]  
    processing_time_meteo = current_time - t[4]

    return json.dumps({
        "time": t[0].isoformat(timespec="milliseconds"),
        "temperature": t[1],
        "suggested_temperature": t[2],
        "processing_time": processing_time,
        "processing_time_meteo": processing_time_meteo,
        "time_difference": t[5],
        "processing_of_meteo_stage": t[6],
        "start_time": t[3]
    },
    allow_nan=False
    )

json_stream = joined_temp_nefiltr.map(to_json, output_type=Types.STRING())
#json_stream.print()


from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

kafka_sink = (
    KafkaSink.builder()
             .set_bootstrap_servers("localhost:19092")
             .set_record_serializer(
                 KafkaRecordSerializationSchema.builder()
                     .set_topic("con_w_meteom")
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

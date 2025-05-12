from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import json, datetime
from pyflink.common import WatermarkStrategy, Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.state import StateTtlConfig
from pyflink.datastream.functions import CoProcessFunction, RuntimeContext, KeyedCoProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
import sklearn
import xgboost
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
import time

# before env.execute()
import logging, os
os.environ["PYFLINK_CLIENT_LOG_LEVEL"] = "DEBUG"
logging.basicConfig(level=logging.DEBUG)




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
        additional_value = float(data.get('processing_time_ms', 0.0))  # Default to 0.0 if not present
        return timestamp, value, source, additional_value
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Error parsing message: {e}, message: {s[:100]}...")
        # Return default values or re-raise based on your error handling strategy
        raise


output_schema = Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.STRING(), Types.FLOAT()])
ds_a = env.add_source(FlinkKafkaConsumer('temperature', SimpleStringSchema(), kafka_props)) \
          .map(parse, output_type=output_schema)

ds_prikon = env.add_source(FlinkKafkaConsumer('prikon', SimpleStringSchema(), kafka_props)).map(parse, output_type=output_schema)
ds_fanh = env.add_source(FlinkKafkaConsumer('fanh', SimpleStringSchema(), kafka_props)).map(parse, output_type=output_schema)
ds_tl = env.add_source(FlinkKafkaConsumer('tl', SimpleStringSchema(), kafka_props)).map(parse, output_type=output_schema)




class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):

        timestamp_ms = int(value[0].timestamp() * 1000)  # Convert to microseconds
        return timestamp_ms

wm = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(EventTimestampAssigner()).with_idleness(Duration.of_seconds(300))
ds_a = ds_a.assign_timestamps_and_watermarks(wm)
ds_prikon = ds_prikon.assign_timestamps_and_watermarks(wm)
ds_fanh = ds_fanh.assign_timestamps_and_watermarks(wm)
ds_tl = ds_tl.assign_timestamps_and_watermarks(wm)


keyed_a = ds_a.key_by(lambda _: 0)          # same "dummy" key for all

keyed_prikon = ds_prikon.key_by(lambda _: 0)
keyed_fanh = ds_fanh.key_by(lambda _: 0)
keyed_tl = ds_tl.key_by(lambda _: 0)
print("checkpoint 1")






class LatestBJoin(CoProcessFunction):
    def open(self, ctx: RuntimeContext):
        schema = Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.STRING(), Types.FLOAT()])
        # Create descriptor for B records state
        descriptor_tc0 = ListStateDescriptor("b_records_tc0", schema)
        descriptor_tc1 = ListStateDescriptor("b_records_tc1", schema)
        descriptor_tc2 = ListStateDescriptor("b_records_tc2", schema)
        descriptor_tc3 = ListStateDescriptor("b_records_tc3", schema)
        descriptor_source = ValueStateDescriptor("source", Types.STRING())
        descriptor_last_tl = ValueStateDescriptor("last_tl", Types.FLOAT())

        # Apply TTL configuration to the state descriptors
        ttl_config = StateTtlConfig \
            .new_builder(Time.minutes(1)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()

        descriptor_tc0.enable_time_to_live(ttl_config)
        descriptor_tc1.enable_time_to_live(ttl_config)
        descriptor_tc2.enable_time_to_live(ttl_config)
        descriptor_tc3.enable_time_to_live(ttl_config)

        self.b_records_tc0 = ctx.get_list_state(descriptor_tc0)
        self.b_records_tc1 = ctx.get_list_state(descriptor_tc1)
        self.b_records_tc2 = ctx.get_list_state(descriptor_tc2)
        self.b_records_tc3 = ctx.get_list_state(descriptor_tc3)
        self.source = ctx.get_state(descriptor_source)
        self.last_tl = ctx.get_state(descriptor_last_tl)

    def get_b_rate_of_change(self, b_records, window_start):
        # Collect B values from the ListState
        b_values = []
        valid_b_records = []

        b_records_list = list(b_records.get())
    
        # Iterate directly through the ListState (it's already iterable)
        for b in b_records_list:
            if b is not None and b[0] is not None and b[1] is not None and b[0] >= window_start:  # Keep records within 5-minute window
                valid_b_records.append(b)
                b_values.append(b[1])  # Extract just the value part

        # Update state with only valid records
        if self.source.value() is not None and "tl" in self.source.value() and len(b_records_list) > 0:
            last_b_record = b_records_list[-1][1]
            if last_b_record is not None:
                self.last_tl.update(last_b_record)
        b_records.update(valid_b_records)
    
        # calculate rate of change
        if b_values and len(b_values) > 1:
            # Get time difference in seconds using timedelta
            time_diff = valid_b_records[-1][0] - valid_b_records[0][0]
            time_diff_seconds = abs(time_diff.total_seconds())
            if time_diff_seconds > 0:
                rate_of_change = (b_values[-1] - b_values[0]) / time_diff_seconds
                return rate_of_change, b_values[0]
        
        # Return default values if no valid records or calculation possible
        if self.source.value() is not None and "tl" in self.source.value():
            last_tl_value = self.last_tl.value()
            if last_tl_value is not None:
                return 0.0, last_tl_value
        return 0.0, 0.0




    def process_element1(self, a, ctx):
        try:
            # Get current time for calculating the time window
            current_time = a[0]
            window_start = current_time - datetime.timedelta(minutes=5)


            # Get rate of change and valid records for each state
            rate_tc0, value_tc0 = self.get_b_rate_of_change(self.b_records_tc0, window_start)
            rate_tc1, value_tc1 = self.get_b_rate_of_change(self.b_records_tc1, window_start)
            rate_tc2, value_tc2 = self.get_b_rate_of_change(self.b_records_tc2, window_start)
            rate_tc3, value_tc3 = self.get_b_rate_of_change(self.b_records_tc3, window_start)
            


                
            yield (a[0], a[1], 
                   rate_tc0,
                   rate_tc1,
                   rate_tc2,
                   rate_tc3,
                   value_tc0,
                   value_tc1,
                   value_tc2,
                   value_tc3,
                   a[3])  # Pass through the processing time
        except Exception as e:
            print(f"Error in process_element1: {e}")
            yield (a[0], a[1], 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, a[3])
    def process_element2(self, b, ctx):
        # Update source only if it's not already set (first run)
        if not self.source.value():
            self.source.update(b[2])
        
        # Add new B record to the appropriate state based on source
        source = b[2]
        if 'tcstav0' in source:
            self.b_records_tc0.add_all([b])
        elif 'tcstav1' in source:
            self.b_records_tc1.add_all([b])
        elif 'tcstav2' in source:
            self.b_records_tc2.add_all([b])
        elif 'tcstav3' in source:
            self.b_records_tc3.add_all([b])
        else:
            print(f"Warning: Unknown source identifier: {source}")

print("checkpoint 2")





joined_prikon = keyed_a.connect(keyed_prikon).process(LatestBJoin(),
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))

#joined_prikon.print()


joined_tl = keyed_a.connect(keyed_tl).process(LatestBJoin(),
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
#joined_tl.print()



joined_fanh = keyed_a.connect(keyed_fanh).process(LatestBJoin(),
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
#joined_fanh.print()















class InnerJoin(KeyedCoProcessFunction):

    def open(self, ctx):
        # MapState<side, valueTuple>
        desc = MapStateDescriptor("buffer", Types.SQL_TIMESTAMP(), 
                                Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), 
                                           Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
                                           Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))

        # Set time to live for state entries
        self.ttl_config = StateTtlConfig \
            .new_builder(Time.minutes(20)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .disable_cleanup_in_background() \
            .build()
        
        # Apply TTL config to the state descriptor
        desc.enable_time_to_live(self.ttl_config)
        self.buffer = ctx.get_map_state(desc)
        print("InnerJoin opened successfully")

    # stream A
    def process_element1(self, left, ctx):
        #print(f"Processing left element with timestamp: {left[0]}")
        if self.buffer.contains(left[0]):
            right = self.buffer.get(left[0])
            #print(f"Found matching right element for timestamp: {left[0]}")
            yield self._merge(left, right)
            self.buffer.remove(left[0])
        else:
            #print(f"No matching right element found for timestamp: {left[0]}, buffering left element")
            self.buffer.put(left[0], left)
            event_ts = ctx.timestamp()
            cleanup_ts = event_ts + 5*60*1000 #20 minutes
            ctx.timer_service().register_event_time_timer(cleanup_ts)

    # stream B
    def process_element2(self, right, ctx):
        #print(f"Processing right element with timestamp: {right[0]}")
        if self.buffer.contains(right[0]):
            left = self.buffer.get(right[0])
            #print(f"Found matching left element for timestamp: {right[0]}")
            yield self._merge(left, right)
            self.buffer.remove(right[0])
        else:
            #print(f"No matching left element found for timestamp: {right[0]}, buffering right element")
            self.buffer.put(right[0], right)
            event_ts = ctx.timestamp()
            cleanup_ts = event_ts + 5*60*1000 #20 minutes
            ctx.timer_service().register_event_time_timer(cleanup_ts)

    def on_timer(self, ts, ctx):
        key = ctx.get_current_key()
        
        
        # Remove from both buffers if they exist
        if self.buffer.contains(key):
            print(f"Cleaning up unmatched record with timestamp: {key}")
            self.buffer.remove(key)
        return []


    @staticmethod
    def _merge(a, b):
        #print(f"Merging records with timestamp: {(a[0], a[1], *a[2:10], *b[2:10],a[10])}")
        return (a[0], a[1], *a[2:10], *b[2:10],a[10])





# First join prikon and tl
joined_pt = joined_prikon.connect(joined_tl).key_by(lambda x: x[0], lambda x: x[0], key_type=Types.SQL_TIMESTAMP()) \
           .process(InnerJoin(),
                    output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))


#joined_pt.print()



class SecondInnerJoin(KeyedCoProcessFunction):

    def open(self, ctx):
        # MapState<side, valueTuple>
        left_big = MapStateDescriptor("left_big", Types.SQL_TIMESTAMP(), 
                                Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(),
                                             Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
                                             Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
                                             Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
                                             Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
        
        right_small = MapStateDescriptor("right_small", Types.SQL_TIMESTAMP(), 
                                Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(),
                                             Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
                                             Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))

        self.left_big = ctx.get_map_state(left_big)
        self.right_small = ctx.get_map_state(right_small)

    # stream A
    def process_element1(self, left, ctx):
        if self.right_small.contains(left[0]):
            right = self.right_small.get(left[0])
            yield self._merge(left, right)
            self.right_small.remove(left[0])
        else:
            self.left_big.put(left[0], left)
            event_ts = ctx.timestamp()
            cleanup_ts = event_ts + 5*60*1000 #5 minutes
            ctx.timer_service().register_event_time_timer(cleanup_ts)

    # stream B
    def process_element2(self, right, ctx):
        if self.left_big.contains(right[0]):
            left = self.left_big.get(right[0])
            yield self._merge(left, right)
            self.left_big.remove(right[0])
        else:
            self.right_small.put(right[0], right)
            event_ts = ctx.timestamp()
            cleanup_ts = event_ts + 5*60*1000
            ctx.timer_service().register_event_time_timer(cleanup_ts)

    def on_timer(self, ts, ctx):
        key = ctx.get_current_key()
        # Remove from both buffers if they exist
        if self.left_big.contains(key):
            self.left_big.remove(key)
        if self.right_small.contains(key):
            self.right_small.remove(key)
        return []

    @staticmethod
    def _merge(a, b):
        # Keep all values except the last one (processing time) from both streams
        #print(f"processing time: {a[18]}")
        return (a[0], a[1], *a[2:18], *b[4:10], a[18])

# Then join the result with fanh
joined_all = joined_pt.connect(joined_fanh).key_by(lambda x: x[0], lambda x: x[0], key_type=Types.SQL_TIMESTAMP()) \
           .process(SecondInnerJoin(),             
                    output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))

#joined_all.print()



"""


############### Model adjusting the temperature###########



from pyflink.datastream.functions import KeyedProcessFunction


class AdjustTemperature(KeyedProcessFunction):
    def __init__(self):
        self.pipe = None
        self.model_loaded = False

    def open(self, ctx):
        try:
            from joblib import load
            self.pipe = load("../model/models/sdeltaT_xgb.joblib")
            self.model_loaded = True
            print("Model loaded successfully")
        except Exception as e:
            print(f"Error loading model: {e}")
            self.model_loaded = False

    def process_element(self, t, ctx):
        try:
            if not self.model_loaded:
                print("Model not loaded, returning original values")
                yield t
                return

            feats = pd.DataFrame(
            [{
            "temperature": t[1],
            "fanh_tc0": t[2],
            "fanh_tc1": t[3],
            "fanh_tc2": t[4],
            "fanh_tc3": t[5],
            "prikon_tc0": t[6],
            "prikon_tc1": t[7],
            "prikon_tc2": t[8],
            "prikon_tc3": t[9]
            }])
            
            delta = float(self.pipe.predict(feats)[0])
            #print(f"Temperature: {t[1]}, Delta: {delta}")
            yield (t[0], t[1] + delta, *t[2:10], t[10])
        except Exception as exc:
            print(f"Error in model prediction: {exc}")
            logging.warning("Model fallback: %s", exc, exc_info=True)
            yield t



adjusted_ds = joined_pt.key_by(lambda x: x[0]).process(AdjustTemperature(), 
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
"""
"""

#### Model in external service ####

import requests, json, apache_beam as beam
from concurrent.futures import ThreadPoolExecutor

from pyflink.datastream.functions import MapFunction

class AdjustTemperature(MapFunction):
    def open(self, ctx):
        self.pool = ThreadPoolExecutor(max_workers=8)
        self.session = requests.Session()
        self.url = "http://localhost:8000/predict"

    def map(self, row):
        future = self.pool.submit(
            self.session.post, self.url,
            json={"values": list(row[1:10])}, timeout=2.0
        )
        try:
            r = future.result()                 # returns quickly if pool is idle
            r.raise_for_status()
            delta = float(r.json()["prediction"])
            return (row[0], row[1] + delta, *row[2:])
        except Exception as e:
            print("[WARN] fallback:", e)
            return row


adjusted_ds = joined_pt.map(AdjustTemperature(), output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))

"""



############### SINK #############
# Kafka sink



import json


def to_json(t):
    """
    t = (event_ts, temperature,
         tc0_prikon, tc1_prikon, tc2_prikon, tc3_prikon,
         tc0_fanh , tc1_fanh , tc2_fanh , tc3_fanh)
    """
    current_time = time.perf_counter()
    processing_time = current_time - t[24]  # t[7] is the processing_time_ms from the message
    #print(f"Processing time: {processing_time}, t[24]: {t[24]}, current_time: {current_time}")
    
    return json.dumps({
        "time": t[0].isoformat(timespec="milliseconds"),
        "temperature": t[1],
        # Prikon rates and values
        "prikon_tc0_rate": t[2],
        "prikon_tc1_rate": t[3],
        "prikon_tc2_rate": t[4],
        "prikon_tc3_rate": t[5],
        "prikon_tc0_value": t[6],
        "prikon_tc1_value": t[7],
        "prikon_tc2_value": t[8],
        "prikon_tc3_value": t[9],
        # TL rates and values
        "tl_tc0_rate": t[10],
        "tl_tc1_rate": t[11],
        "tl_tc2_rate": t[12],
        "tl_tc3_rate": t[13],
        "tl_tc0_value": t[14],
        "tl_tc1_value": t[15],
        "tl_tc2_value": t[16],
        "tl_tc3_value": t[17],
        # Fanh rates (only two)
        "fanh_tc0_rate": t[18],
        "fanh_tc1_rate": t[19],
        "fanh_tc0_value": t[20],
        "fanh_tc1_value": t[21],
        "fanh_tc2_value": t[22],
        "fanh_tc3_value": t[23],
        "processing_time": processing_time
    },
    allow_nan=False
    )

json_stream = joined_all.map(to_json, output_type=Types.STRING())
json_stream.print()


from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

kafka_sink = (
    KafkaSink.builder()
             .set_bootstrap_servers("localhost:19092")
             .set_record_serializer(
                 KafkaRecordSerializationSchema.builder()
                     .set_topic("good_night")
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

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




class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):

        timestamp_ms = int(value[0].timestamp() * 1000)  # Convert to microseconds
        return timestamp_ms

wm = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(EventTimestampAssigner()).with_idleness(Duration.of_seconds(300))
ds_a = ds_a.assign_timestamps_and_watermarks(wm)
ds_prikon = ds_prikon.assign_timestamps_and_watermarks(wm)
ds_fanh = ds_fanh.assign_timestamps_and_watermarks(wm)


keyed_a = ds_a.key_by(lambda _: 0)          # same "dummy" key for all

keyed_prikon = ds_prikon.key_by(lambda _: 0)
keyed_fanh = ds_fanh.key_by(lambda _: 0)
print("checkpoint 1")

class LatestBJoin(CoProcessFunction):
    def open(self, ctx: RuntimeContext):
        schema = Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.STRING(), Types.FLOAT()])
        # Create descriptor for B records state
        descriptor_tc0 = ListStateDescriptor("b_records_tc0", schema)
        descriptor_tc1 = ListStateDescriptor("b_records_tc1", schema)
        descriptor_tc2 = ListStateDescriptor("b_records_tc2", schema)
        descriptor_tc3 = ListStateDescriptor("b_records_tc3", schema)

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

    def get_b_values_avg(self, b_records, window_start):
    # Collect B values from the ListState
        b_values = []
        valid_b_records = []
    


        # Iterate directly through the ListState (it's already iterable)
        for b in b_records.get():
            if b[0] >= window_start:  # Keep records within 5-minute window
                valid_b_records.append(b)
                b_values.append(b[1])  # Extract just the value part

        # Update state with only valid records
        b_records.update(valid_b_records)
    
    # Calculate average if there are valid B records
        if b_values:
            avg_b = sum(b_values) / len(b_values)
            return avg_b
        else:
            return 0.0




    def process_element1(self, a, ctx):
        try:
            # Get current time for calculating the time window
            current_time = a[0]
            window_start = current_time - datetime.timedelta(minutes=5)
                
            yield (a[0], a[1], 
                   self.get_b_values_avg(self.b_records_tc0, window_start),
                   self.get_b_values_avg(self.b_records_tc1, window_start),
                   self.get_b_values_avg(self.b_records_tc2, window_start),
                   self.get_b_values_avg(self.b_records_tc3, window_start),
                   a[3])  # Pass through the processing time
        except Exception as e:
            print(f"Error in process_element1: {e}")
            yield (a[0], a[1], 0.0, 0.0, 0.0, 0.0, a[3])
    def process_element2(self, b, ctx):
        
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

class latest_fanh_join(CoProcessFunction):
    def open(self, ctx: RuntimeContext):
        schema = Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.STRING()])
        descriptor_tc0 = ValueStateDescriptor("b_records_tc0", schema)
        descriptor_tc1 = ValueStateDescriptor("b_records_tc1", schema)
        descriptor_tc2 = ValueStateDescriptor("b_records_tc2", schema)
        descriptor_tc3 = ValueStateDescriptor("b_records_tc3", schema)

        self.tc0 = ctx.get_state(descriptor_tc0)
        self.tc1 = ctx.get_state(descriptor_tc1)
        self.tc2 = ctx.get_state(descriptor_tc2)
        self.tc3 = ctx.get_state(descriptor_tc3)

        self.tc0.update((datetime.datetime(1970, 1, 1),0.0,"tcstav0"))
        self.tc1.update((datetime.datetime(1970, 1, 1),0.0,"tcstav1"))
        self.tc2.update((datetime.datetime(1970, 1, 1),0.0,"tcstav2"))
        self.tc3.update((datetime.datetime(1970, 1, 1),0.0,"tcstav3"))



    def process_element1(self, a, ctx):
        tc0_val = self.tc0.value() or (None, 0.0, None)
        tc1_val = self.tc1.value() or (None, 0.0, None)
        tc2_val = self.tc2.value() or (None, 0.0, None)
        tc3_val = self.tc3.value() or (None, 0.0, None)

        yield (a[0], a[1], tc0_val[1], tc1_val[1], tc2_val[1], tc3_val[1], a[3])

    def process_element2(self, b, ctx):
        source = b[2]
        if 'tcstav0' in source:
            self.tc0.update(b)
        elif 'tcstav1' in source:
            self.tc1.update(b)
        elif 'tcstav2' in source:
            self.tc2.update(b)
        elif 'tcstav3' in source:
            self.tc3.update(b)
        else:
            print(f"Warning: Unknown source identifier: {source}")














joined_prikon = keyed_a.connect(keyed_prikon).process(LatestBJoin(),
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
#joined_prikon.print()



joined_fanh = keyed_a.connect(keyed_fanh).process(latest_fanh_join(),
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
#joined_fanh.print()





class InnerJoin(KeyedCoProcessFunction):

    def open(self, ctx):
        try:
            # MapState<side, valueTuple>
            desc = MapStateDescriptor("buffer", Types.SQL_TIMESTAMP(), 
                                    Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), 
                                               Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
            self.buffer = ctx.get_map_state(desc)
        except Exception as e:
            print(f"Error in InnerJoin.open: {e}")
            raise

    # stream A
    def process_element1(self, left, ctx):
        try:
            if self.buffer.contains(left[0]):
                right = self.buffer.get(left[0])
                yield self._merge(left, right)
                self.buffer.remove(left[0])
            else:
                self.buffer.put(left[0], left)
                event_ts = ctx.timestamp()
                cleanup_ts = event_ts + 5*60*1000 #5 minutes
                ctx.timer_service().register_event_time_timer(cleanup_ts)
        except Exception as e:
            print(f"Error in process_element1: {e}")
            yield left

    # stream B
    def process_element2(self, right, ctx):
        try:
            if self.buffer.contains(right[0]):
                left = self.buffer.get(right[0])
                yield self._merge(left, right)
                self.buffer.remove(right[0])
            else:
                self.buffer.put(right[0], right)
                event_ts = ctx.timestamp()
                cleanup_ts = event_ts + 5*60*1000
                ctx.timer_service().register_event_time_timer(cleanup_ts)
        except Exception as e:
            print(f"Error in process_element2: {e}")
            yield right

    def on_timer(self, ts, ctx):
        try:
            key = ctx.get_current_key()
            self.buffer.remove(key)
            return []
        except Exception as e:
            print(f"Error in on_timer: {e}")
            return []

    @staticmethod
    def _merge(a, b):
        try:
            # Keep the processing time from the first stream (a)
            return (a[0], a[1], *a[2:6], *b[2:6], a[6])
        except Exception as e:
            print(f"Error in _merge: {e}")
            return a

joined_fp = joined_fanh.connect(joined_prikon).key_by(lambda x: x[0],lambda x: x[0],  key_type=Types.SQL_TIMESTAMP()) \
           .process(InnerJoin(),
                    output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))

joined_fp.print()






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
            yield (t[0], t[1] + delta, *t[2:10], t[10],delta,t[1])
        except Exception as exc:
            print(f"Error in model prediction: {exc}")
            logging.warning("Model fallback: %s", exc, exc_info=True)
            yield t



adjusted_ds = joined_fp.key_by(lambda x: x[0]).process(AdjustTemperature(), 
          output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))
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


adjusted_ds = joined_fp.map(AdjustTemperature(), output_type=Types.TUPLE([Types.SQL_TIMESTAMP(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()]))

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
    processing_time = current_time - t[10] 
    
    return json.dumps({
        "time": t[0].isoformat(timespec="milliseconds"),
        "temperature": t[1],
        "fanh_tc0": t[2],
        "fanh_tc1": t[3],
        "fanh_tc2": t[4],
        "fanh_tc3": t[5],
        "prikon_tc0": t[6],
        "prikon_tc1": t[7],
        "prikon_tc2": t[8],
        "prikon_tc3": t[9],
        "processing_time": processing_time,
        "delta": t[11],
        "original_temperature": t[12]
    },
    allow_nan=False
    )

json_stream = adjusted_ds.map(to_json, output_type=Types.STRING())
json_stream.print()


from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

kafka_sink = (
    KafkaSink.builder()
             .set_bootstrap_servers("localhost:19092")
             .set_record_serializer(
                 KafkaRecordSerializationSchema.builder()
                     .set_topic("joined_fp")
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

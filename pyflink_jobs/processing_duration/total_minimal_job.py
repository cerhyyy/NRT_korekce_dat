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


ds_temp_nefiltr = env.add_source(FlinkKafkaConsumer('temperature', SimpleStringSchema(), kafka_props)) \
          

ds_temp_meteo = env.add_source(FlinkKafkaConsumer('meteo_temperature', SimpleStringSchema(), kafka_props)) \
          







combined_ds = ds_temp_nefiltr.union(ds_temp_meteo)





combined_ds.print()


from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

kafka_sink = (
    KafkaSink.builder()
             .set_bootstrap_servers("localhost:19092")
             .set_record_serializer(
                 KafkaRecordSerializationSchema.builder()
                     .set_topic("con_w_meteom3")
                     .set_value_serialization_schema(SimpleStringSchema())
                     .build()
             )
             .set_property("transaction.timeout.ms", "60000") 
             .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
             .build()
)



combined_ds.sink_to(kafka_sink)



print("Starting execution")
env.execute("DEBUG")

from pyflink.datastream.connectors import FlinkKafkaConsumer, RollingPolicy,StreamingFileSink,OutputFileConfig
from pyflink.common.serialization import JsonRowDeserializationSchema,SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream.execution_mode import RuntimeExecutionMode

# Teste do job usando DataStream API
def job():
    env = StreamExecutionEnvironment.get_execution_environment()
    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
    # env.add_jars("file:///path/to/flink-sql-connector-kafka.jar")
    env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(1000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW([]).build()
    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics='A_RAIABD-TB_CANAL_VENDA',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '10.1.165.35:9092,10.1.165.36:9092,10.1.165.37:9092',
        'group.id': 'test_group'})

    ds = env.add_source(kafka_consumer)

    # Saída
    #output_path = 's3://rd-datalake-dev-temp/spark_dev/flink/output/'
    output_path = 's3://kubernets-flink-poc/pyflink-json/'
    file_sink = StreamingFileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder()
        #.with_part_suffix(".json")
        .with_part_suffix(".parquet")
        .build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy(part_size=5*1024*1024,rollover_interval=10*1000,inactivity_interval=10*1000)) \
        .build()
    ds.add_sink(file_sink)
    env.execute("tb_canal_venda")

if __name__ == '__main__':
    job()
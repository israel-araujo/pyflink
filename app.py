from pyflink.datastream.connectors import FlinkKafkaConsumer,FileSink,OutputFileConfig
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types

# Teste do job usando DataStream API
def job():
    env = StreamExecutionEnvironment.get_execution_environment()
    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
    #env.add_jars("file:///path/to/flink-sql-connector-kafka.jar")

    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info=Types.ROW([Types.MAP(key_type_info=Types.STRING(),value_type_info=Types.STRING())])).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='A_RAIABD-TB_CANAL_VENDA',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '10.1.165.35:9092,10.1.165.36:9092,10.1.165.37:9092',
        'group.id': 'test_group'})

    ds = env.add_source(kafka_consumer)

    # Saída
    output_path = 's3://rd-datalake-dev-temp/spark_dev/flink/output/'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder() \
        .with_part_prefix('pre') \
        .with_part_suffix('suf').build()) \
        .build()
    ds.print()
    ds.sink_to(file_sink)
    env.execute()

if __name__ == '__main__':
    job()
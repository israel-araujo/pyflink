from pyflink.datastream.connectors import FlinkKafkaConsumer, RollingPolicy,StreamingFileSink,OutputFileConfig
from pyflink.common.serialization import JsonRowDeserializationSchema,SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.table import *


# Teste do job usando DataStream API

def job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(1000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    t_env = StreamTableEnvironment.create(env)
    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics='A_RAIABD-TB_CANAL_VENDA',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '10.1.165.35:9092,10.1.165.36:9092,10.1.165.37:9092',
        'group.id': 'test_group'}
        )
    ds = env.add_source(kafka_consumer)
    t_env.execute_sql('''
                    CREATE TABLE sync (
                        cd_canal_venda INT,
                        ds_canal_venda STRING
                    ) WITH (
                        'connector' = 'filesystem',
                        'path' = 's3://kubernets-flink-poc/pyflink-json/',
                        'format' = 'json'
                    )''')
    table = t_env.from_data_stream(ds)
    table_result = table.execute_insert("sync")
    env.execute("tb_canal_venda")


if __name__ == '__main__':
    job()

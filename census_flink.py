from flink_ml_tensorflow.tensorflow_TFConfig import TFConfig
from flink_ml_tensorflow.tensorflow_on_flink_mlconf import MLCONSTANTS
from flink_ml_tensorflow.tensorflow_on_flink_table import train
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableConfig, StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_config = TableConfig()
t_env = StreamTableEnvironment.create(env)

statement_set = t_env.create_statement_set()
t_env.get_config().get_configuration().set_boolean('python.fn-execution.memory.managed', True)

t_env.execute_sql('''
            create table stream_source (
                age varchar,
                workclass varchar,
                fnlwgt varchar,
                education varchar,
                education_num varchar,
                marital_status varchar,
                occupation varchar,
                relationship varchar,
                race varchar,
                gender varchar,
                capital_gain varchar,
                capital_loss varchar,
                hours_per_week varchar,
                native_country varchar,
                income_bracket varchar
            ) with (
                'connector' = 'kafka',
                'topic' = 'census_train_input',
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'stream_train_source',
                'format' = 'csv',
                'scan.startup.mode' = 'latest-offset'
            )
        ''')

work_num = 2
ps_num = 1
python_file = 'census_distribute.py'
func = 'tf_on_flink_stream'
prop = {MLCONSTANTS.PYTHON_VERSION: '',
        MLCONSTANTS.ENCODING_CLASS: 'com.alibaba.flink.ml.operator.coding.RowCSVCoding',
        MLCONSTANTS.DECODING_CLASS: 'com.alibaba.flink.ml.operator.coding.RowCSVCoding',
        'sys:csv_encode_types': 'STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING,STRING',
        MLCONSTANTS.CONFIG_STORAGE_TYPE: MLCONSTANTS.STORAGE_ZOOKEEPER,
        MLCONSTANTS.CONFIG_ZOOKEEPER_CONNECT_STR: 'localhost:2181',
        MLCONSTANTS.CONFIG_ZOOKEEPER_BASE_PATH: '/census',
        MLCONSTANTS.REMOTE_CODE_ZIP_FILE: "file:///tmp/code.zip"}
env_path = None
input_tb = t_env.from_path('stream_source')
output_schema = None

tf_config = TFConfig(work_num, ps_num, prop, python_file, func, env_path)

train(env, t_env, statement_set, input_tb, tf_config, output_schema)

statement_set.execute()

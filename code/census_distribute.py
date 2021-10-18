import json
import logging
import os
from datetime import datetime

import tensorflow as tf

from census_dataset import build_model_columns, input_fn, flink_input_fn

logger = logging.getLogger('tensorflow')
formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s  %(threadName)s - %(message)s")
for h in logger.handlers:
    h.setFormatter(formatter)
tf.logging.set_verbosity(tf.logging.INFO)

flags = tf.flags
flags.DEFINE_string('train_data', '/tmp/census_data/adult.data',
                    'path of train data')
flags.DEFINE_string('eval_data', '/tmp/census_data/adult.evaluate', 'path of eval data')
flags.DEFINE_string('model_dir', '/tmp/census_model',
                    'Directory where all models are saved')
flags.DEFINE_string('model_version', datetime.now().isoformat(),
                    'Model version')
flags.DEFINE_integer('batch_size', 128, 'Batch size.')
flags.DEFINE_integer('num_epochs', 1,
                     'Num of batches to train (epochs).')
flags.DEFINE_string('task_type', 'ps', 'task type')
flags.DEFINE_integer('task_index', 0, 'task index')
FLAGS = flags.FLAGS


def run_train(train_input_fn, eval_input_fn):
    wide_columns, deep_columns = build_model_columns()
    hidden_units = [100, 75, 50, 25]
    run_config = tf.estimator.RunConfig().replace(
        keep_checkpoint_max=1,
        save_checkpoints_secs=60,
        session_config=tf.ConfigProto(device_count={'GPU': 0},
                                      inter_op_parallelism_threads=8,
                                      intra_op_parallelism_threads=8))

    estimator = tf.estimator.DNNLinearCombinedClassifier(
        model_dir=os.path.join(FLAGS.model_dir, FLAGS.model_version),
        linear_feature_columns=wide_columns,
        dnn_feature_columns=deep_columns,
        dnn_hidden_units=hidden_units,
        config=run_config)

    serving_input_fn = tf.estimator.export.build_parsing_serving_input_receiver_fn(
        tf.feature_column.make_parse_example_spec(wide_columns + deep_columns))

    eval_spec = tf.estimator.EvalSpec(eval_input_fn, start_delay_secs=5, throttle_secs=60, exporters=[
        tf.estimator.LatestExporter("export", serving_input_receiver_fn=serving_input_fn)])
    tf.estimator.train_and_evaluate(estimator,
                                    tf.estimator.TrainSpec(train_input_fn),
                                    eval_spec)


def tf_on_flink_stream(context):
    """Init and run a stream job"""
    from flink_ml_tensorflow.tensorflow_context import TFContext

    tf_context = TFContext(context)
    tf_context.export_estimator_cluster()

    def flink_train_input_fn():
        return flink_input_fn(FLAGS.batch_size, tf_context)

    def eval_input_fn():
        return input_fn(FLAGS.eval_data, FLAGS.num_epochs, False, FLAGS.batch_size)

    run_train(flink_train_input_fn, eval_input_fn)


def main():
    os.environ["TF_CONFIG"] = json.dumps({
        "cluster":
            {"chief": ["localhost:2001"],
             "worker": ["localhost:2002"],
             "ps": ["localhost:2000"]},
        "task": {"type": FLAGS.task_type, "index": FLAGS.task_index}
    })

    def train_input_fn():
        return input_fn(FLAGS.train_data, FLAGS.num_epochs, False, FLAGS.batch_size)

    def eval_input_fn():
        return input_fn(FLAGS.eval_data, FLAGS.num_epochs, False, FLAGS.batch_size)

    run_train(train_input_fn, eval_input_fn)


if __name__ == "__main__":
    main()

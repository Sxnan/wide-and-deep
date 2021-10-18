# Wide and Deep & TF on Flink example

## Environment
- python: 3.7
- tensorflow: 1.15.0
- Flink: 1.11.3

## Install dependency

```bash
pip install -r requirements.txt
```

## Download Dataset

```bash
python code/census_dataset.py
```

Check if the dataset exist at `/tmp/census_data`


## Training Distributed without Flink

```bash

# Start ps server
python code/census_distribute.py --num_epochs 5 --task_type ps --task_index 0

# Start chief and worker
python code/census_distribute.py --num_epochs 5 --task_type chief --task_index 0
python code/census_distribute.py --num_epochs 5 --task_type worker --task_index 0

```

Check if the module exist at `/tmp/census_model`.


## Training Distributed with TF on Flink

### Install TF-on-Flink from source

```shell
# Clone TF on Flink
git clone git@github.com:flink-extended/flink-ai-extended.git

# Install 
cd flink-ai-extended/deep-learning-on-flink
mvn clean install -DskipTests -Dfast -pl flink-ml-tensorflow -am

cd ../..
```

### Start Flink cluster

```bash
# Download Flink binary
wget https://archive.apache.org/dist/flink/flink-1.11.3/flink-1.11.3-bin-scala_2.11.tgz
tar -xvzf flink-1.11.3-bin-scala_2.11.tgz
```

We need to put TF-on-Flink `.jar` files under the `flink-1.11.3/lib` dir.
Find the  `flink-ml-tensorflow-0.4-SNAPSHOT-jar-with-dependencies.jar` in your TF-on-Flink project's target dir.
Then copy the jar and the https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.11.3/flink-sql-connector-kafka_2.11-1.11.3.jar into `flink-1.11.3/lib` dir. 

```bash
cp flink-ai-extended/deep-learning-on-flink/flink-ml-tensorflow/target/flink-ml-tensorflow-*-jar-with-dependencies.jar flink-1.11.3/lib
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.11.3/flink-sql-connector-kafka_2.11-1.11.3.jar -O flink-1.11.3/lib/flink-sql-connector-kafka_2.11-1.11.3.jar

# Config number of slot per taskmanager
sed -i 's/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: 8/' flink-1.11.3/conf/flink-conf.yaml

# Start flink cluster
flink-1.11.3/bin/start-cluster.sh
```

Visit `localhost:8081` to see the UI of the Flink.


### Start Kafka cluster
```bash
# Download kafka binary
wget https://dlcdn.apache.org/kafka/2.7.1/kafka_2.12-2.7.1.tgz
tar -xvzf kafka_2.12-2.7.1.tgz

kafka_2.12-2.7.1/bin/zookeeper-server-start.sh -daemon kafka_2.12-2.7.1/config/zookeeper.properties
kafka_2.12-2.7.1/bin/kafka-server-start.sh -daemon kafka_2.12-2.7.1/config/server.properties
```

### Prepare environment to run TF-on-Flink

```bash
zip -r code.zip code
cp code.zip /tmp
```

### Submit Flink job

```bash
flink-1.11.3/bin/flink run -py census_flink.py
```

You should see the Flink job running in the WebUI.


### Produce data to Kafka

```bash
python kafka_util/census_kafka_data.py
```

You should see the training log in the Stdout log of the TaskManager in the WebUi. The model should be saved at
`/tmp/census_model` after few second.

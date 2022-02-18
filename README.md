# kafka-bridge-client
Python async client for [Strimzi Kafka Bridge](https://github.com/strimzi/strimzi-kafka-bridge) and [Confluent REST Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html) Package include consumer only.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-green.svg)](https://github.com/shafa-dev/kafka-bridge-client/issues)
[![PyPI version](https://badge.fury.io/py/kafka-bridge-client.svg)](https://badge.fury.io/py/kafka-bridge-client)

## Install
```
pip install kafka-bridge-client
```

## Usage
By default client use [Strimzi Kafka Bridge](https://github.com/strimzi/strimzi-kafka-bridge) API

`Consumer (async)`

```python
from kafka_bridge_client import KafkaBridgeConsumer

# Strimzi Kafka Bridge

consumer1 = KafkaBridgeConsumer(
    'topic1',
    'topic2',
    group_id='my-group,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    bootstrap_server='your-kafka-bridge-url',
    consumer_name='consumer-name',
)

# Confluent REST Proxy
consumer2 = KafkaBridgeConsumer(
    'topic1',
    'topic2',
    group_id='my-group,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    bootstrap_server='your-kafka-bridge-url',
    consumer_name='consumer-name',
    proxy='confluent'
)

async for rec in consumer1.get_records():
    print(rec['value'])
    await consumer.commit()
```


`Producer (sync)`

```python
from kafka_bridge_client import KafkaBridgeProducer

producer = KafkaBridgeProducer('http://bridge.url' timeout=5)
producer.send(Message(key='1', value='value'))
```


## Deploy

You need to change version in `pyproject.toml` and run it

```
poetry publish --build
```
# kafka-bridge-client
Python async client for [Strimzi Kafka Bridge](https://github.com/strimzi/strimzi-kafka-bridge). Package include consumer only.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-green.svg)](https://github.com/shafa-dev/kafka-bridge-client/issues)
[![PyPI version](https://badge.fury.io/py/kafka-bridge-client.svg)](https://badge.fury.io/py/kafka-bridge-client)

## Install
```
pip install kafka-bridge-client
```

## Usage
```python
from kafka_bridge_client import KafkaBridgeConsumer


consumer = KafkaBridgeConsumer(
    'topic1',
    'topic2',
    group_id='my-group,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    bootstrap_server='your-kafka-bridge-url',
    consumer_name='consumer-name',
)

async for rec in consumer.get_records():
    print(rec['value'])
    await consumer.commit()
```

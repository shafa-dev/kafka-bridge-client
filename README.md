# kafka-bridge-client
Python async client for Strimzi Kafka Bridge. Package include consumer only.

## Install
```
pip install kafka-bridge-client
```

## Usage
```python
from kafka_bridge_client import KafkaBridgeConsumer


consumer = KafkaBridgeConsumer(
    CONFIG['topics']['name'],
    group_id=CONFIG['group_id'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    bootstrap_server=CONFIG['kafka_bridge']['url'],
    consumer_name='consumer-name',
)
```

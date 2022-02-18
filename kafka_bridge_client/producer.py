import typing as t
import logging
import base64

import requests as r
from mypy_extensions import TypedDict

from .exceptions import (
    KafkaBridgeTopicNotFoundError,
    ValidationError,
    KafkaBridgeResourceException,
    KafkaBridgeInternalError,
)


__all__ = ['KafkaBridgeProducer', 'Message', ]


logger = logging.getLogger(__name__)


class Message(TypedDict):
    """
    The object for representation message data.
    """
    value: t.Union[str, bytes]
    key: t.Optional[str]


class KafkaBridgeProducer:
    """
    Docs: https://strimzi.io/docs/bridge/latest

    This class is abstraction over producer from strimzi bridge.

    Example of usage:

        >>> producer = KafkaBridgeProducer('http://bridge.url' timeout=5)
        >>> producer.send(Message(key='1', value='value'))

    if you need to send binary data then u can use `binary` argument in send
    method.

    Raises:
        - KafkaBridgeTopicNotFoundError - topic not found
        - ValidationError - bad format of key or value
        - KafkaBridgeInternalError - internal error of bridge service
        - KafkaBridgeResourceException - common error
    """

    def __init__(self, url: str, timeout: float) -> None:
        self._url = url
        self._timeout = timeout

    def sends(
        self,
        topic: str,
        records: t.List[Message],
        binary: bool = False,
    ) -> bool:
        """
        This method provide aproache to send messages to kafka in
        batch (send all messages and flush) mode.
        """
        data: t.Any = None

        if binary:
            data = [
                {
                    'key': r.get('key'),
                    # we need to conver binary data to base64 for success serialization to json
                    'value':
                        base64.b64encode(r.get('value', b'')).decode(),  # type: ignore
                }
                for r in records
            ]
            headers = {'Content-Type': 'application/vnd.kafka.binary.v2+json'}
        else:
            data = records
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

        response = r.post(
            url=f'{self._url}/topics/{topic}',
            json={'records': data},
            timeout=self._timeout,
            headers=headers,
        )

        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            raise KafkaBridgeTopicNotFoundError(
                str({'topic': topic, 'values': data})
            )
        elif response.status_code == 422:
            raise ValidationError(
                str({'topic': topic, 'values': data})
            )
        elif response.status_code == 500:
            raise KafkaBridgeInternalError(
                str({'topic': topic, 'values': data})
            )
        else:
            msg = (
                '[KAFKA BRIDGE] request failed: '
                'status=%(status)s data=%(data)s'
            )
            ctx = {
                'status': response.status_code,
                'data': response.text,
            }

            logger.error(msg, ctx)

            raise KafkaBridgeResourceException(
                f'Request to Kafka Bridge failed. '
                f'{str({"topic": topic})}',
            )

    def send(
        self,
        topic: str,
        record: Message,
        binary: bool = False
    ) -> bool:
        """
        This method provide aproache to send messages to kafka in
        message-by-message (send and flush for each message) mode.
        """
        return self.sends(topic, records=[record], binary=binary)

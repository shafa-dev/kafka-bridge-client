class KafkaBridgeError(Exception):
    pass


class KafkaBridgeClientError(KafkaBridgeError):
    pass


class KafkaBridgeTimeoutError(KafkaBridgeError):
    pass


class ConsumerNotFound(KafkaBridgeError):
    pass

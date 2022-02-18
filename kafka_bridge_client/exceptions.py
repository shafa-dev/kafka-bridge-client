class KafkaBridgeError(Exception):
    pass


class KafkaBridgeClientError(KafkaBridgeError):
    pass


class KafkaBridgeTimeoutError(KafkaBridgeError):
    pass


class ConsumerNotFound(KafkaBridgeError):
    pass


class KafkaBridgeTopicNotFoundError(KafkaBridgeError):
    pass


class ValidationError(KafkaBridgeError):
    pass


class ValidationError(KafkaBridgeError):
    pass


class KafkaBridgeResourceException(KafkaBridgeError):
    pass


class KafkaBridgeInternalError(KafkaBridgeError):
    pass

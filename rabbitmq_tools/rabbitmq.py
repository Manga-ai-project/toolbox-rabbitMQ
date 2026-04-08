import logging
import time
from typing import Callable, Union
import pika

class BadMessageStructureException(Exception):
    pass


class RabbitBase:
    def __init__(self, logger: logging.Logger, uri: str):
        self.l = logger
        self.uri = uri
        self.parameters = pika.URLParameters(self.uri)
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.channel.Channel | None = None

    def connect(self):
        raise NotImplementedError

    def _ensure_connected(self):
        if (
            self.connection is None
            or self.connection.is_closed
            or self.channel is None
            or self.channel.is_closed
        ):
            self.connect()


# =========================
# PRODUCER
# =========================

class RabbitProducer(RabbitBase):
    def __init__(self, logger: logging.Logger, uri: str, exchange: str, key: str):
        super().__init__(logger, uri)
        self.exchange = exchange
        self.routing_key = key

    def connect(self, delay_seconds: int = 3):
        while True:
            try:
                self.l.info("[producer] connecting to RabbitMQ")
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                self.l.info("[producer] connected")
                return
            except Exception as e:
                self.l.error(f"[producer] connect error: {e}")
                time.sleep(delay_seconds)

    def publish(self, body: Union[bytes, str], retries: int = 3) -> bool:
        for attempt in range(1, retries + 1):
            try:
                self._ensure_connected()
                self.channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=body,
                )
                self.l.info("[producer] Successful publishing")
                return True

            except pika.exceptions.AMQPError as e:
                self.l.warning(
                    f"[producer] publish attempt {attempt} failed: {e}"
                )
                try:
                    if self.connection:
                        self.connection.close()
                except Exception:
                    pass

                self.connection = None
                self.channel = None
                time.sleep(1)

        self.l.error("[producer] publish failed permanently")
        return False


# =========================
# CONSUMER
# =========================

class RabbitConsumer(RabbitBase):
    def __init__(self, logger: logging.Logger, uri: str, queue: str):
        super().__init__(logger, uri)
        self.queue = queue

    def connect(self, delay_seconds: int = 3):
        while True:
            try:
                self.l.info("[consumer] connecting to RabbitMQ")
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=1)
                self.l.info("[consumer] connected")
                return
            except Exception as e:
                self.l.error(f"[consumer] connect error: {e}")
                time.sleep(delay_seconds)

    def consume(
        self,
        handler_func,
        extra_func,
    ):
        self._ensure_connected()

        def _callback(ch, method, properties, body: bytes):
            self.l.info(f"[consumer] received message")
            try:
                result = handler_func(body)
                if extra_func is not None:
                    ok = extra_func(result)
                    if not ok:
                        raise Exception("[consumer] Extra callback functional failed")

                ch.basic_ack(delivery_tag=method.delivery_tag)
            except BadMessageStructureException as e1:
                self.l.warning(f"[consumer] Got a bad message! Skipping: {e1}")
                # пропускаем
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except Exception as e2:

                self.l.error(f"[consumer] handler error: {e2}")
                # сообщение вернётся в очередь
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        while True:
            try:
                self.channel.basic_consume(
                    queue=self.queue,
                    on_message_callback=_callback,
                    auto_ack=False,
                )
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                self.l.error(f"[consumer] connection lost: {e}")
                self.connect()
            except Exception as e:
                self.l.error(f"[consumer] fatal error: {e}")
                raise
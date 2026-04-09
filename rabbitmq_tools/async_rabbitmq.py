import logging
import time
import asyncio
from typing import Callable, Union
from .rabbitmq import BadMessageStructureException
import aio_pika

class RabbitBaseAIO:
    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None

    async def connect(self):
        if self.connection and not self.connection.is_closed:
            return

        logging.info('Connecting to RabbitMQ')

        self.connection = await aio_pika.connect_robust(self.uri)
        self.channel = await self.connection.channel()

        logging.info('Connected to RabbitMQ')

    async def disconnect(self):
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logging.info("Connection closed RabbitMQ")

class RabbitProducerAIO(RabbitBaseAIO):
    def __init__(self, uri: str, exchange: str, key: str):
        super().__init__(uri)
        self.exchange = exchange
        self.routing_key = key

    async def produce(self, body: Union[bytes, str], retries: int = 3) -> bool:
        for attempt in range(1, retries + 1):
            try:
                await self.connect()

                await self.channel.default_exchange.publish(
                    aio_pika.Message(body=body.encode()),
                    routing_key=self.routing_key
                )

                logging.info('Published message')
                return True

            except (
                    aio_pika.exceptions.AMQPConnectionError,
                    aio_pika.exceptions.ChannelClosed,
            ) as e:
                logging.warning(f"Producing attempt {attempt} failed: {e}")

                self.connection = None
                self.channel = None

                await asyncio.sleep(1 * attempt)

            except Exception as e:
                logging.error(f"Producing unexpected error: {e}")
                return False

        logging.error("RabbitProducerAIO failed to publish")
        return False

class RabbitConsumerAIO(RabbitBaseAIO):
    def __init__(self, uri: str, prefetch_count: int, queue: str):
        super().__init__(uri)
        self.queue = queue
        self.prefetch_count = prefetch_count

    async def connect(self):
        if self.connection and not self.connection.is_closed:
            return

        logging.info('Connecting to RabbitMQ')

        self.connection = await aio_pika.connect_robust(self.uri)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        logging.info('Connected to RabbitMQ')

    async def consume(self, handler_func: Callable, extra_func: Callable=None):

        async def _callback(message: aio_pika.IncomingMessage):
            async with message.process():
                try:
                    if asyncio.iscoroutinefunction(handler_func):
                        result = await handler_func(message)
                    else:
                        result = handler_func(message.body)

                    if extra_func is not None:
                        if asyncio.iscoroutinefunction(extra_func):
                            ok = await extra_func(result)
                        else:
                            ok = extra_func(result)
                        if not ok:
                            raise Exception("Extra callback functional failed")

                except BadMessageStructureException as e1:
                    logging.warning(f"Bad message: {e1}")
                    await message.reject(requeue=False)

                except Exception as e2:
                    logging.error(f"Handler error: {e2}")
                    await message.reject(requeue=True)

        await self.connect()
        queue = await self.channel.declare_queue(self.queue, durable=True)

        await queue.consume(_callback)
        await asyncio.Future()

        logging.info("Started consuming")











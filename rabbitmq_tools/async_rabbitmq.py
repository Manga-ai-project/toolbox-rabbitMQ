import logging
import asyncio
import time
from typing import Callable, Union
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
    def __init__(self, uri: str, exchange: str, key: str, retries: int = 3) -> None:
        super().__init__(uri)
        self.exchange = exchange
        self.routing_key = key
        self.retries = int(retries)

    async def produce(self, body: Union[bytes, str], routing_key: str = None) -> bool:
        for attempt in range(1, self.retries + 1):
            try:
                await self.connect()

                exchange = await self.channel.get_exchange(self.exchange)

                body_bytes = body if isinstance(body, bytes) else body.encode()


                if routing_key is None:
                    routing_key = self.routing_key

                await exchange.publish(
                    aio_pika.Message(body=body_bytes),
                    routing_key=routing_key,
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

        logging.info('Consumer is connecting to RabbitMQ')

        self.connection = await aio_pika.connect_robust(self.uri)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        logging.info('Consumer is connected to RabbitMQ')

    async def consume(self, handler_func: Callable, extra_func: Callable = None):
        async def _callback(message: aio_pika.IncomingMessage):
            t_arr = time.monotonic()
            logging.info('Received message')

            # Запускаем обработку в фоне, НЕ ЖДЁМ
            async def handle():
                try:
                    if asyncio.iscoroutinefunction(handler_func):
                        result = await handler_func(message, t_arr)
                    else:
                        result = handler_func(message.body, t_arr)

                    if extra_func is not None:
                        if asyncio.iscoroutinefunction(extra_func):
                            ok = await extra_func(result)
                        else:
                            ok = extra_func(result)
                        if not ok:
                            raise Exception("Extra callback functional failed")

                except Exception as e:
                    logging.exception(f"Handler crashed: {e}")
                    try:
                        await message.nack(requeue=True)
                    except:
                        pass

            # Создаём фоновую задачу и сразу выходим
            asyncio.create_task(handle())

        await self.connect()
        queue = await self.channel.declare_queue(self.queue, durable=True)
        await queue.consume(_callback)
        await asyncio.Future()
        logging.info("Started consuming")











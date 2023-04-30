import aio_pika
import uuid
import asyncio

class FibonacciRpcClient(object):

    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.futures = {}

    async def connect(self):
        self.connection = await aio_pika.connect_robust("amqp://localhost/")
        self.channel = await self.connection.channel()

        self.callback_queue = await self.channel.declare_queue(exclusive=True)

        await self.callback_queue.consume(self.on_response)

    def on_response(self, message):
        future = self.futures.pop(message.correlation_id, None)
        if future is not None:
            future.set_result(message.body)

    async def call(self, n, num):
        if self.connection is None or self.connection.is_closed:
            await self.connect()

        correlation_id = str(uuid.uuid4())
        future = asyncio.Future()
        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=str(n).encode(),
                reply_to=self.callback_queue.name,
                correlation_id=correlation_id,
            ),
            routing_key="rpc_queue",
        )

        return await future


async def main():
    tasks = []
    fibonacci_rpc = FibonacciRpcClient()
    await fibonacci_rpc.connect()

    for num in range(1, 21):
        print(f" [x] Requesting fib({num})")
        tasks.append(fibonacci_rpc.call(num, num))

    responses = await asyncio.gather(*tasks)
    for response in responses:
        print(" [.] Got %r" % response)

    await fibonacci_rpc.connection.close()


asyncio.run(main())
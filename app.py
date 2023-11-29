import asyncio, aio_pika, json
from db.engine import SessionLocal, engine
from db import crud, models

async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        body: dict = json.loads(message.body)

        username: str = body['username']
        amount: int = body['amount']

        print(f" [x] Received {body}")

        # Manage Inventory.
        async with SessionLocal() as db:
            is_created = await crud.create_inventory(db=db, username=username, amount=amount, status="SUCCESS")
            if (is_created):
                routing_key = "from.deliver"

                channel = await connection.channel()

                body['deliver_number'] = is_created.id

                await channel.default_exchange.publish(
                    aio_pika.Message(body=bytes(json.dumps(body), 'utf-8')),
                    routing_key=routing_key,
                )
                print(f"update inventory success")
                await db.commit()
            else:
                await process_rb_status(message=message, connection=connection)


async def process_rb_status(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    body: dict = json.loads(message.body)

    print(f" [x] Rolling Back {body}")

    # from Insufficient Inventory
    channel = await connection.channel()

    # status will be "UNKNOWN", like how do you even die if its only 1 write in the db??
    await channel.default_exchange.publish(
        aio_pika.Message(body=message.body),
        routing_key="rb.inventory",
    )


async def process_rb(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        body: dict = json.loads(message.body)

        deliver_id = body["deliver_number"]

        print(f" [x] Rolling Back {body}")

        async with SessionLocal() as db:
            is_done = await crud.change_status(db, deliver_id=deliver_id, status="DELIVERY_FAILED")
            if is_done:
                channel = await connection.channel()

                body["status"] = "DELIVERY_FAILED"

                await channel.default_exchange.publish(
                    aio_pika.Message(body=bytes(json.dumps(body), 'utf-8')),
                    routing_key="rb.inventory",
                )

                await db.commit()
            else:
                print("GG[3]")


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://rabbit-mq",
    )

    # Init the tables in db
    async with engine.begin() as conn:
        # Drop all table every time
        await conn.run_sync(models.Base.metadata.drop_all)

        # Init all table every time
        await conn.run_sync(models.Base.metadata.create_all)

    queue_name = "to.deliver"

    # Creating channel
    channel = await connection.channel()

    # Maximum message count which will be processing at the same time.
    await channel.set_qos(prefetch_count=10)

    # Declaring queue
    queue = await channel.declare_queue(queue_name, arguments={
                                                    'x-message-ttl' : 1000,
                                                    'x-dead-letter-exchange' : 'dlx',
                                                    'x-dead-letter-routing-key' : 'dl'
                                                    })
    queue_rb = await channel.declare_queue("rb.deliver")

    print(' [*] Waiting for messages. To exit press CTRL+C')

    await queue.consume(lambda message: process_message(message, connection))
    await queue_rb.consume(lambda message: process_rb(message, connection))

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
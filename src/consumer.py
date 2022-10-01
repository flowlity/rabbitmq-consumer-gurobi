import pika
import os
import functools
import threading

from gurobi import do_work
from retry import retry

# @retry((pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker), delay=5, jitter=(5, 10))
@retry(Exception, delay=5, jitter=(5, 10))
def startConsumer():
    def on_message(channel, method, properties, body, args):
        t = threading.Thread(target=do_work, args=(channel, method, properties, body, args))
        t.start()


    credentials = pika.PlainCredentials("flowlity", "flowlity")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=f"rabbitmq-{os.getenv('PYTHON_ENV')}.flowlity.com",
            port="5672",
            credentials=credentials,
            heartbeat=60,  ## if = 0, Better to use connection.process_data_events()
        )
    )

    # creating channels
    channel = connection.channel()
    # basic_qos
    channel.basic_qos(prefetch_count=1, global_qos=True)  # per channel

    on_message_callback = functools.partial(on_message, args=(connection,))
    # connecting queues to channels
    channel.basic_consume(
        queue="gurobi", on_message_callback=on_message_callback, auto_ack=False
    )

    # Starting Threads for different channels to start consuming enqueued requests
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    except Exception as error:
        print(error)
        raise error
    channel.close()

startConsumer()

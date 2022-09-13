import pika
import os
from dotenv import load_dotenv
from pulp import LpProblem, GUROBI
from src.logs import GurobiLogging

load_dotenv()

def startConsumer():
    def callbackFunctionForQueue(ch,method,properties,body):
        print(f"Got a message from Queue C: {body}")
        print(f"message id: {properties.message_id}")
        print(method.delivery_tag)

        ch.basic_ack(delivery_tag = method.delivery_tag)


    credentials = pika.PlainCredentials('flowlity','flowlity')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=f"rabbitmq-{os.getenv("PYTHON_ENV")}.flowlity.com", port='5672', credentials= credentials))

    #creating channels
    channel= connection.channel()
    #basic_qos
    channel.basic_qos(prefetch_count=1, global_qos=True) # per channel
    #connecting queues to channels
    channel.basic_consume(queue="gurobi", on_message_callback=callbackFunctionForQueue, auto_ack=False)

    #Starting Threads for different channels to start consuming enqueued requests
    channel.start_consuming()

startConsumer()
import pika
import os
import json
from dotenv import load_dotenv
from pulp import LpProblem, GUROBI
from src.logs import GurobiLogging

load_dotenv()


def startConsumer():
    def _solve_model(dict_model, params, job_id):
        log_filename = f"gurobi_log_{job_id}"
        _, model = LpProblem.from_dict(dict_model)
        with GurobiLogging(log_filename=log_filename, job_id=job_id):
            model.solve(
                GUROBI(
                    logPath=log_filename,
                    options=[
                        ("timeLimit", float(params["timeLimit"])),
                        ("MIPgap", float(params["gapRel"])),
                    ],
                )
            )
        return model.to_dict()

    def pull_model_from_sa(job_id: str):
        return None

    def solve_model(time_limit: str = "0", gap_rel: str = "0", job_id: str = "0"):
        os.system("killall -9 gurobi_cl")
        params = {"timeLimit": time_limit, "gapRel": gap_rel}
        dict_model_in = pull_model_from_sa(job_id)
        dict_model_out = _solve_model(dict_model_in, params, job_id=job_id)

        return dict_model_out

    def callbackFunctionForQueue(channel, method, properties, body):
        print(f"Got a message from Queue C: {body}")
        print(f"message id: {properties.message_id}")
        message = json.loads(body)

        time_limit = message.get("time_limit", "0")
        gap_rel = message.get("gap_rel", "0")
        job_id = properties.message_id
        solve_model(
            time_limit=time_limit,
            gap_rel=gap_rel,
            job_id=job_id
        )

        channel.basic_ack(delivery_tag=method.delivery_tag)

    credentials = pika.PlainCredentials("flowlity", "flowlity")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=f"rabbitmq-{os.getenv('PYTHON_ENV')}.flowlity.com",
            port="5672",
            credentials=credentials,
        )
    )

    # creating channels
    channel = connection.channel()
    # basic_qos
    channel.basic_qos(prefetch_count=1, global_qos=True)  # per channel
    # connecting queues to channels
    channel.basic_consume(
        queue="gurobi", on_message_callback=callbackFunctionForQueue, auto_ack=False
    )

    # Starting Threads for different channels to start consuming enqueued requests
    channel.start_consuming()


startConsumer()

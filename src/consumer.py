import pika
import os
import json
from dotenv import load_dotenv
from pulp import LpProblem, GUROBI
from logs import GurobiLogging
from azure.storage.fileshare import ShareFileClient

load_dotenv()

conn_str = ("DefaultEndpointsProtocol=https;AccountName=aksexternalglobal;"
            "AccountKey=BpoiAAv0Sf0P5zdipcFa/seeAya7pl6f2v5BVf8MYqtsqE1wWgWXZ51j5jkUknF4B541o0b36fWHTMiLzQmAKw==;"
            "EndpointSuffix=core.windows.net")

def startConsumer():
    def _solve_model(dict_model, params, job_id):
        os.system("killall -9 gurobi_cl")
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

    def pull_model_from_sa(file_name: str):
        file_share_client = ShareFileClient.from_connection_string(
                conn_str=conn_str,
                share_name=f"gurobi-{os.getenv('PYTHON_ENV')}",
                file_path=f"un-opt-models/{file_name}"
            )
        data = file_share_client.download_file()
        return json.loads(data.readall())
    
    def push_model_to_sa(file_name: str, dict_model_out: dict):
        try:
            file_share_client = ShareFileClient.from_connection_string(
                conn_str=conn_str,
                share_name=f"gurobi-{os.getenv('PYTHON_ENV')}",
                file_path=f"models/{file_name}",
            )

            file_share_client.upload_file(json.dumps(dict_model_out))
        except Exception as error:
            print(error)
            # logger.exception(f"Failed to upload solution of job {job_id}")
        # else:
        #     logger.info(f"Solution of job {job_id} has been uploaded.")

    def solve_model(job_id: str):
        json_models = pull_model_from_sa(job_id)
        dict_res = {}
        for model_id, json_model in json_models.items():
            params = {
                "timeLimit": json_model["timeLimit"],
                "gapRel": json_model["gapRel"],
            }
            dict_res[model_id] = _solve_model(json_model["model"], params, job_id=job_id)

        return dict_res

    def callbackFunctionForQueue(channel, method, properties, body):
        print(f"Got a message from Queue C: {body}")
        print(f"message id: {properties.message_id}")
        message = json.loads(body)

        job_id = properties.message_id
        dict_model_out = solve_model(job_id=job_id)

        push_model_to_sa(file_name=job_id, dict_model_out=dict_model_out)

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

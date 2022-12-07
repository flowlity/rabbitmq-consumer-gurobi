import functools
import os
import json

from datetime import datetime
from pulp import LpProblem, GUROBI_CMD
from logs import GurobiLogging
from azure.storage.fileshare import ShareFileClient
from dotenv import load_dotenv

load_dotenv()


conn_str = (
    "DefaultEndpointsProtocol=https;AccountName=aksexternalglobal;"
    "AccountKey=BpoiAAv0Sf0P5zdipcFa/seeAya7pl6f2v5BVf8MYqtsqE1wWgWXZ51j5jkUknF4B541o0b36fWHTMiLzQmAKw==;"
    "EndpointSuffix=core.windows.net"
)
DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


def pull_model_from_sa(file_name: str):
    file_share_client = ShareFileClient.from_connection_string(
        conn_str=conn_str,
        share_name=f"gurobi-{os.getenv('PYTHON_ENV')}",
        file_path=f"un-opt-models/{file_name}",
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


def _solve_model(dict_model, params, job_id):
    log_filename = f"gurobi_log_{job_id}"
    _, model = LpProblem.from_dict(dict_model)
    with GurobiLogging(log_filename=log_filename, job_id=job_id):
        os.system("killall -9 gurobi_cl")
        model.solve(
            GUROBI_CMD(
                options=[
                    ("timeLimit", float(params["timeLimit"])),
                    ("MIPgap", float(params["gapRel"])),
                ]
            )
        )
        return {
            'solution_time': model.solutionTime, 'model_dict': model.to_dict()}


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


def ack_message(channel, delivery_tag):
    """Note that `channel` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass


def do_work(channel, method, properties, body, args):
    (connection,) = args
    message = json.loads(body)
    delivery_tag = method.delivery_tag

    job_id = properties.message_id

    # gurobi start
    start = datetime.now()
    dict_model_out = solve_model(job_id)
    end = datetime.now()
    dict_model_out["solver_start_end_dates"] = (
        start.strftime(DATE_FORMAT), end.strftime(DATE_FORMAT))
    push_model_to_sa(file_name=job_id, dict_model_out=dict_model_out)
    os.system("killall -9 gurobi_cl")
    # gurobi end

    cb = functools.partial(ack_message, channel, delivery_tag)
    connection.add_callback_threadsafe(cb)

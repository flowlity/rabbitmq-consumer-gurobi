import os
import time
import logging
import threading
import logstash
import platform
from dotenv import load_dotenv

load_dotenv()


run_environment = os.getenv("PYTHON_ENV", "local")
server_name = platform.node()


class GurobiLogger(logging.LoggerAdapter):
    def __init__(self, environment: str, job_id: str, subservice_name: str = None):
        default_logger = logging.Logger(f"gurobi_api_logger_{job_id}")
        default_logger.setLevel(logging.INFO)
        handler = self.choose_handler(environment=environment)
        default_logger.addHandler(handler)
        extra = {
            "environment": environment,
            "service": {
                "name": "gurobi_api",
            },
            "job": {"id": job_id},
        }
        if subservice_name is not None:
            extra.setdefault("subservice", dict())["name"] = subservice_name
        super().__init__(logger=default_logger, extra=extra)

    @staticmethod
    def choose_handler(environment: str):
        if environment == "preprod" or environment == "production":
            return logstash.TCPLogstashHandler("logstash.flowlity.com", 5000, version=1)
        else:
            return logging.StreamHandler()


def gurobi_logging(logger, log_filename, gurobi_finished):
    open(log_filename, "a").close()  # Creates the log file if it does not exist
    with open(log_filename, "r+") as log_file:
        logger_running = True
        while logger_running:
            where = log_file.tell()
            line = log_file.readline()
            if not line:
                if gurobi_finished.is_set():
                    logger_running = False
                time.sleep(1)
                log_file.seek(where)
            elif line.rstrip():
                logger.info(line)
                time.sleep(0.01)
    try:
        os.remove(log_filename)
    except FileNotFoundError:
        pass


class GurobiLogging:
    def __init__(self, log_filename, job_id):
        logger = GurobiLogger(
            environment=run_environment, job_id=job_id, subservice_name=server_name
        )
        self.gurobi_finished = threading.Event()
        self.gurobi_logging = threading.Thread(
            name="gurobi_logging",
            target=gurobi_logging,
            args=(logger, log_filename, self.gurobi_finished),
        )

    def __enter__(self):
        self.gurobi_logging.start()

    def __exit__(self, exception_type, value, traceback):
        self.gurobi_finished.set()


# class ConsumerLogger:
#     def __init__(self):

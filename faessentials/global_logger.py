import inspect
import logging
import queue
import logging.handlers
from faessentials import utils

loggers = {}


def setup_custom_logger(name):
    global loggers

    if loggers.get(name):
        return loggers.get(name)
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(module)s:%(lineno)d] %(levelname)s %(message)s"
        )

        logger = logging.getLogger(name)
        # clean the handlers, otherwise you get duplicated records when logging
        if logger.hasHandlers():
            return logger
            # logger.handlers.clear()
        logger.propagate = False
        level = logging.getLevelName(utils.get_logging_level())
        logger.setLevel(level)
        log_queue = queue.Queue()
        queue_handler = logging.handlers.QueueHandler(log_queue)
        # set the non-blocking handler first
        logger.addHandler(queue_handler)

        # Stream is important for looking at the k8s pod logs.
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)

        # the local logging is fine, but there should be a Fluent Bit integration,
        # which sends logs to the central LMM instance
        timerotating_handler = logging.handlers.TimedRotatingFileHandler(
            utils.get_log_path().joinpath("app_rolling.log"), when="D", backupCount=30
        )
        timerotating_handler.setLevel(utils.get_logging_level())
        timerotating_handler.setFormatter(formatter)
        listener = logging.handlers.QueueListener(
            log_queue, stream_handler, timerotating_handler, respect_handler_level=True
        )

        # Only print the following when instantiated by the main.py file - and not other files.
        # This will ensure that the important project variables are printed on startup.
        current_stack = inspect.stack()
        if any("main.py" in frame.filename for frame in current_stack):
            # Print settings:
            logger.info(f"Starting {utils.get_application_name()} in {utils.get_environment()}.")
            logger.info(f"Domain {utils.get_domain_name()}")
            logger.info(f"Root {utils.get_project_root()}")
            logger.info(f"Log Path {utils.get_log_path()}")
            logger.info(f"Logging Level {utils.get_logging_level()}")

        listener.start()

    return logger

import time, logging

from pipeline import pipeline


logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="logs/pipeline.log",
    filemode="a",
    format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


def main():
    start_time = time.time()

    pipeline()

    execution_time = time.strftime("%M:%S", time.gmtime(time.time() - start_time))
    logger.info(f"Execution time: {execution_time}")


if __name__ == "__main__":
    main()

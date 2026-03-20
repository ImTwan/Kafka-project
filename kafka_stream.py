from confluent_kafka import Consumer, Producer
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import os, threading, logging, time

# LOAD ENV
load_dotenv()

# =========================================================
# LOGGING (FILE + ROTATION)
# =========================================================
logger = logging.getLogger("kafka_stream")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
)

file_handler = RotatingFileHandler(
    "kafka_stream.log",
    maxBytes=50 * 1024 * 1024,   # 50MB
    backupCount=3
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# =========================================================
# ENV
# =========================================================
REMOTE_TOPIC = os.getenv("REMOTE_TOPIC")
LOCAL_TOPIC = os.getenv("LOCAL_TOPIC")
NUM_THREADS = int(os.getenv("NUM_THREADS"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))

# =========================================================
# KAFKA CONFIG
# =========================================================
remote_conf = {
    'bootstrap.servers': os.getenv("REMOTE_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("REMOTE_KAFKA_USERNAME"),
    'sasl.password': os.getenv("REMOTE_KAFKA_PASSWORD"),
    'group.id': 'mirror_secure_v3',
    'auto.offset.reset': 'earliest'
}

local_conf = {
    'bootstrap.servers': os.getenv("LOCAL_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("LOCAL_KAFKA_USERNAME"),
    'sasl.password': os.getenv("LOCAL_KAFKA_PASSWORD"),
    'compression.type': 'lz4'
}

# =========================================================
# SHARED
# =========================================================
lock = threading.Lock()
stop_event = threading.Event()
total = 0
last_msg_time = time.time()

# =========================================================
# WORKER
# =========================================================
def worker():
    global total, last_msg_time

    consumer = Consumer(remote_conf)
    producer = Producer(local_conf)

    consumer.subscribe([REMOTE_TOPIC])
    buffer = []

    logger.info("Worker started")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(0.05)

            if msg is None:
                continue

            if msg.error():
                logger.warning(f"Kafka error: {msg.error()}")
                continue

            buffer.append(msg.value())
            last_msg_time = time.time()

            if len(buffer) >= BATCH_SIZE:
                for item in buffer:
                    producer.produce(LOCAL_TOPIC, value=item)
                    producer.poll(0)

                size = len(buffer)
                buffer.clear()

                with lock:
                    total += size
                    if total % 500000 == 0:
                        logger.info(f"Processed: {total}")

    finally:
        producer.flush()
        consumer.close()
        logger.info("Worker stopped")

# =========================================================
# MAIN
# =========================================================
def main():
    start = time.time()

    logger.info("=== START KAFKA STREAM ===")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker) for _ in range(NUM_THREADS)]

        while True:
            time.sleep(5)
            logger.info(f"Total processed: {total}")

            if time.time() - last_msg_time > 300:
                logger.info("No more data → stopping")
                stop_event.set()
                break

        for f in futures:
            f.result()

    logger.info(f"Done: {total} in {time.time()-start:.2f}s")

if __name__ == "__main__":
    main()
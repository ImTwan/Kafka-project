from confluent_kafka import Consumer
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import os, threading, logging, time, json

# LOAD ENV
load_dotenv()

# =========================================================
# LOGGING
# =========================================================
logger = logging.getLogger("kafka_to_mongo")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
)

file_handler = RotatingFileHandler(
    "kafka_to_mongo.log",
    maxBytes=50 * 1024 * 1024,
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
TOPIC = os.getenv("LOCAL_TOPIC")
NUM_THREADS = int(os.getenv("NUM_THREADS"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))

# =========================================================
# KAFKA CONFIG
# =========================================================
consumer_conf = {
    'bootstrap.servers': os.getenv("LOCAL_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("LOCAL_KAFKA_USERNAME"),
    'sasl.password': os.getenv("LOCAL_KAFKA_PASSWORD"),
    'group.id': 'mongo_secure_v3',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# =========================================================
# MONGO CONFIG
# =========================================================
mongo_client = MongoClient(
    os.getenv("MONGO_URI"),
    maxPoolSize=100
)

db = mongo_client[os.getenv("MONGO_DB")]
collection = db[os.getenv("MONGO_COLLECTION")]

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

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

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

            try:
                data = json.loads(msg.value().decode("utf-8"))

                if "_id" in data:
                    del data["_id"]

                buffer.append(data)
                last_msg_time = time.time()

            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                continue

            if len(buffer) >= BATCH_SIZE:
                try:
                    collection.insert_many(buffer, ordered=False)
                except Exception as e:
                    logger.error(f"Mongo insert error: {e}")

                size = len(buffer)
                buffer.clear()

                consumer.commit(asynchronous=True)

                with lock:
                    total += size
                    if total % 500000 == 0:
                        logger.info(f"Inserted: {total}")

    finally:
        if buffer:
            try:
                collection.insert_many(buffer, ordered=False)
            except Exception as e:
                logger.error(f"Final insert error: {e}")

        consumer.close()
        logger.info("Worker stopped")

# =========================================================
# MAIN
# =========================================================
def main():
    start = time.time()

    logger.info("=== START KAFKA TO MONGO ===")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker) for _ in range(NUM_THREADS)]

        while True:
            time.sleep(5)
            logger.info(f"Total inserted: {total}")

            if time.time() - last_msg_time > 300:
                logger.info("No more data → stopping")
                stop_event.set()
                break

        for f in futures:
            f.result()

    logger.info(f"Done: {total} in {time.time()-start:.2f}s")

if __name__ == "__main__":
    main()
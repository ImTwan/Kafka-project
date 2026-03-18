from confluent_kafka import Consumer
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
import threading
import logging
import time
import json

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("kafka_to_mongo.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================================================
# CONFIG
# =========================================================
TOPIC = "product_view_local"

NUM_THREADS = 3             
BATCH_SIZE = 10000         
IDLE_TIMEOUT = 300          # 5 min no data → stop

# =========================================================
# KAFKA CONFIG (FAST CONSUME)
# =========================================================
consumer_conf = {
    'bootstrap.servers': '192.168.40.25:9094,192.168.40.25:9194,192.168.40.25:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',

    'group.id': 'mongo_fast_group_v2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,

    # performance tuning
    'fetch.min.bytes': 1048576,
    'fetch.wait.max.ms': 20,
    'max.partition.fetch.bytes': 10485760
}

# =========================================================
# MONGODB CONFIG (IMPORTANT)
# =========================================================
mongo_client = MongoClient(
    "mongodb://localhost:27017/",
    maxPoolSize=100,
    w=0   # 🔥 VERY IMPORTANT: no wait for write ack (huge speed boost)
)

db = mongo_client["kafka_db"]
collection = db["product_view"]

# =========================================================
# SHARED
# =========================================================
lock = threading.Lock()
stop_event = threading.Event()

total_inserted = 0
last_msg_time = time.time()

# =========================================================
# WORKER
# =========================================================
def worker(thread_id):
    global total_inserted, last_msg_time

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

    buffer = []

    logger.info(f"Thread-{thread_id} started")

    try:
        while not stop_event.is_set():

            msg = consumer.poll(0.05)

            if msg is None:
                continue

            if msg.error():
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))

                if "_id" in data:
                    del data["_id"]

                buffer.append(data)
                last_msg_time = time.time()

            except:
                continue

            if len(buffer) >= BATCH_SIZE:
                collection.insert_many(buffer, ordered=False)

                size = len(buffer)
                buffer.clear()

                consumer.commit(asynchronous=True)

                with lock:
                    total_inserted += size

                    if total_inserted % 500000 == 0:
                        logger.info(f"Inserted: {total_inserted}")

    finally:
        if buffer:
            collection.insert_many(buffer, ordered=False)

        consumer.close()
        logger.info(f"Thread-{thread_id} stopped")

# =========================================================
# MAIN
# =========================================================
def main():
    global last_msg_time

    start = time.time()
    last_msg_time = start

    logger.info("=== KAFKA TO MONGO ===")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker, i) for i in range(NUM_THREADS)]

        while True:
            time.sleep(5)

            elapsed = time.time() - start
            rate = total_inserted / elapsed if elapsed > 0 else 0

            logger.info(f"Speed: {rate:.0f} docs/sec")

            # AUTO STOP when no more data
            idle_time = time.time() - last_msg_time
            if idle_time > IDLE_TIMEOUT:
                logger.info("No more messages → stopping")
                stop_event.set()
                break

        for f in futures:
            f.result()

    duration = time.time() - start

    logger.info("===================================")
    logger.info(f"Total inserted: {total_inserted}")
    logger.info(f"Time: {round(duration,2)} sec")
    logger.info(f"Avg: {round(total_inserted/duration,2)} docs/sec")
    logger.info("DONE")
    logger.info("===================================")


if __name__ == "__main__":
    main()
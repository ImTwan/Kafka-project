from confluent_kafka import Consumer, Producer
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
import time

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("kafka_mirror.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================================================
# CONFIG
# =========================================================
REMOTE_TOPIC = "product_view"
LOCAL_TOPIC = "product_view_local"

NUM_THREADS = 3          
BATCH_SIZE = 10000       
IDLE_TIMEOUT = 600       # 10 minutes (IMPORTANT)

# =========================================================
# KAFKA CONFIG
# =========================================================
remote_conf = {
    'bootstrap.servers': '46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',

    # NEW GROUP → read from beginning
    'group.id': 'group_ultra_fast_v2',
    'auto.offset.reset': 'earliest',

    # better fetch
    'fetch.min.bytes': 1048576,
    'fetch.wait.max.ms': 50,
    'max.partition.fetch.bytes': 10485760
}

local_conf = {
    'bootstrap.servers': '192.168.40.25:9094,192.168.40.25:9194,192.168.40.25:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',

    # HIGH THROUGHPUT
    'linger.ms': 100,
    'batch.num.messages': 100000,
    'compression.type': 'lz4',
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.kbytes': 1048576
}

# =========================================================
# SHARED STATE
# =========================================================
lock = threading.Lock()
stop_event = threading.Event()

total_processed = 0
last_msg_time = time.time()

# =========================================================
# WORKER
# =========================================================
def worker(thread_id):
    global total_processed, last_msg_time

    consumer = Consumer(remote_conf)
    producer = Producer(local_conf)

    consumer.subscribe([REMOTE_TOPIC])
    buffer = []

    logger.info(f"Thread-{thread_id} started")

    try:
        while not stop_event.is_set():

            msg = consumer.poll(0.1)

            if msg is None:
                continue

            if msg.error():
                continue

            buffer.append(msg.value())
            last_msg_time = time.time()

            if len(buffer) >= BATCH_SIZE:
                for item in buffer:
                    producer.produce(LOCAL_TOPIC, value=item)
                    producer.poll(0)   # non-blocking send

                size = len(buffer)
                buffer.clear()

                with lock:
                    total_processed += size

                    if total_processed % 100000 == 0:
                        logger.info(f"Processed: {total_processed}")

                # flush occasionally (NOT every batch)
                if total_processed % 200000 == 0:
                    producer.flush()

    finally:
        # flush remaining
        if buffer:
            for item in buffer:
                producer.produce(LOCAL_TOPIC, value=item)

        producer.flush()
        consumer.close()

        logger.info(f"Thread-{thread_id} stopped")

# =========================================================
# MAIN
# =========================================================
def main():
    global last_msg_time

    start = time.time()
    last_msg_time = start

    logger.info("=== KAFKA STREAM ===")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker, i) for i in range(NUM_THREADS)]

        while True:
            time.sleep(5)

            elapsed = time.time() - start
            rate = total_processed / elapsed if elapsed > 0 else 0

            logger.info(f"Speed: {rate:.0f} msg/sec")

            # =================================================
            # AUTO STOP (FIXED)
            # =================================================
            idle_time = time.time() - last_msg_time

            if idle_time > IDLE_TIMEOUT:
                logger.info(f"No data for {IDLE_TIMEOUT}s → stopping...")
                stop_event.set()
                break

        for f in futures:
            f.result()

    duration = time.time() - start

    logger.info("===================================")
    logger.info(f"Total messages: {total_processed}")
    logger.info(f"Total time: {round(duration, 2)} sec")
    logger.info(f"Avg throughput: {round(total_processed/duration, 2)} msg/sec")
    logger.info("PROCESS FINISHED ✅")
    logger.info("===================================")

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    main()
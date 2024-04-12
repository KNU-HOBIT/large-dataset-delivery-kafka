import requests
import pandas as pd
from confluent_kafka import Consumer, KafkaError
import threading
from queue import Queue
import time
import json
import signal
import threading
from concurrent.futures import ThreadPoolExecutor


# Kafka 설정
KAFKA_TOPIC = "iot-sensor-data-p3-r1-retention1h"
BOOTSTRAP_SERVERS = "155.230.34.51:32100,155.230.34.52:32100,155.230.34.53:32100" 
CONSUMER_GROUP = 'iot-sensor-data-consumer-group'

# Worker 쓰레드 개수
NUM_WORKERS = 3

# 공유 데이터 큐
shared_queue = Queue()
# 데이터 수집 완료 이벤트
data_collected_event = threading.Event()

# Mutex lock for thread-safe operations on the shared queue
queue_lock = threading.Lock()


# 전역 변수로 워커 쓰레드 목록을 관리
workers = []

def format_memory_usage(bytes, suffix="B"):
    """바이트 단위의 메모리 사용량을 보기 쉽게 변환하는 함수"""
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(bytes) < 1024.0:
            return f"{bytes:3.1f}{unit}{suffix}"
        bytes /= 1024.0
    return f"{bytes:.1f}Y{suffix}"

def signal_handler(sig, frame):
    """SIGINT 시그널을 처리하는 함수"""
    print("SIGINT detected, gracefully shutting down...")
    data_collected_event.set()  # 데이터 수집 완료 이벤트 설정

    # 모든 워커 쓰레드의 종료를 기다림
    for worker in workers:
        worker.join()
    print("All workers have been terminated.")
    exit()

# SIGINT 핸들러 설정
signal.signal(signal.SIGINT, signal_handler)

def send_http_request(start, end, eqp_id):
    """HTTP 요청을 보내고 응답을 받는 함수"""
    url = f"http://155.230.34.51:3000/?start={start}&end={end}&eqp_id={eqp_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None
    
def kafka_consumer_worker():
    """Kafka 메시지를 consume하는 Worker 쓰레드 함수"""
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        # "fetch.min.bytes":   524288,               # 최소 0.5MB의 데이터를 가져옴
        # "max.partition.fetch.bytes": 1048576 * 5,      # 파티션 당 최대 50MB 데이터를 가져옴
    })
    consumer.subscribe([KAFKA_TOPIC])
    
    print(f"Consumer initialized: {consumer}, Memory Address: {hex(id(consumer))}")

    while not data_collected_event.is_set():
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

         # Use lock when accessing the shared queue
        # with queue_lock:
        shared_queue.put(msg.value())

    # 쓰레드 종료 시 컨슈머 리소스 정리
    consumer.close()
    print(f"Consumer at {hex(id(consumer))} closed.")

def start_kafka_workers():
    """Kafka Worker 쓰레드들을 시작하는 함수"""
    global workers
    workers = [threading.Thread(target=kafka_consumer_worker) for _ in range(NUM_WORKERS)]
    for worker in workers:
        worker.start()
        
def parse_messages(messages):
    """메시지 배치를 파싱하는 함수"""
    return [json.loads(msg) for msg in messages]

def batch_process():
    """큐에서 메시지를 배치로 가져와 파싱하는 함수"""
    while not data_collected_event.is_set() or not shared_queue.empty():
        batch = []
        while not shared_queue.empty():
            with queue_lock:
                temp = shared_queue.get()
            batch.append(temp)
        if batch:
            print("batch len : ", len(batch))
            yield parse_messages(batch)

def monitor_and_parse_messages(total_messages):
    """공유 큐를 모니터링하고 메시지를 배치로 파싱하는 함수"""
    collected_data = []
    for parsed_batch in batch_process():
        collected_data.extend(parsed_batch)
        if len(collected_data) >= total_messages:
            break
        
    print("Total collected_data count: ",len(collected_data))

    df = pd.DataFrame(collected_data)
    return df


# Kafka Consumer 워커와 별개로 파싱 워커 시작
def start_parsing_worker(total_messages):
    with ThreadPoolExecutor(max_workers=4) as executor:
        future = executor.submit(monitor_and_parse_messages, total_messages)
        return future.result()


def main():
    # Worker 쓰레드 시작
    start_kafka_workers()

    while True:
        # 사용자 입력 대기
        user_input = input("Press Enter to send HTTP request, or type 'exit' to quit: ")
        if user_input.lower() == 'exit':
            break

        # HTTP 요청 보내기 전의 시간 기록
        start_time = time.time()
        # HTTP 요청 보내기
        # response = send_http_request("2020-09-02T20:00:00Z", "2020-09-03T20:00:00Z", "201")
        response = send_http_request("2020-09-01T00:00:00Z", "2020-10-31T23:36:34Z", "201")
        if response and 'total_messages' in response:
            total_messages = response['total_messages']
            print(f"Total messages to collect: {total_messages}")

            # 메시지 모니터링 및 DataFrame 파싱
            df = start_parsing_worker(total_messages)
            
            # HTTP 요청을 보내고 처리 완료 후의 시간 기록
            end_time = time.time()

            # 경과 시간 계산 (초 단위)
            elapsed_time = end_time - start_time
            

            print("Data collection complete. DataFrame created.")

            # DataFrame 출력
            print(df.head())
            print(df.count())
            
            # DataFrame의 메모리 사용량 디버깅
            memory_usage_per_column = df.memory_usage(deep=True)
            total_memory_usage = memory_usage_per_column.sum()
            print("Memory usage by each column (in bytes):\n", memory_usage_per_column)
            print(f"Total memory usage by DataFrame: {format_memory_usage(total_memory_usage)} bytes")
            print(f"Elapsed time for HTTP request and message processing: {elapsed_time:.2f} seconds.")
        else:
            print("Failed to get response or 'total_messages' not in response")


if __name__ == "__main__":
    main()
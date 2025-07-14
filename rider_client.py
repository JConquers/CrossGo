import threading
import time
import requests
from kafka import KafkaConsumer


# SUBJECT-TO-BUISNESS-POLICY CONFIGURABLES
MAX_QUEUE_TIME = 60 
MAX_TIME_ADVERTISED = 30


# SERVICE CONFIGURABLE
KAFKA_TOPIC = 'rideAcceptance'
KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_CONSUMER_GROUP_ID='riders-group-cli'
KAFKA_TIMEOUT = MAX_QUEUE_TIME + MAX_TIME_ADVERTISED  # seconds
HEADERS = {'Content-Type': 'application/json'}

RIDER_SEVICE_URL = "http://localhost:8001/rider/requestRide"
CAB_SERVICE_URL = ""
CARGO_SERVICE_URL = ""
MATCHMAKING_SERVICE_URL = ""


def kafka_listener(rider_id, timeout=KAFKA_TIMEOUT):
    print("[THREAD] Ride Acceptance detection active. Listening for Kafka messages...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='latest',
        group_id=KAFKA_CONSUMER_GROUP_ID,
        enable_auto_commit=False
    )

    start = time.time()
    for message in consumer:
        msg = message.value.decode()
        print(msg)
        try:
            r_id, d_id, trip_id = msg.strip().split(',')
            if r_id == rider_id:
                print(f"[THREAD] Trip assigned! Driver: {d_id}, TripID: {trip_id}, Ending thread.")
                break
        except Exception as e:
            continue

        if time.time() - start > timeout:
            print("[THREAD] Listener timeout. No match found. Ending thread.")
            break
    
    consumer.close()

def send_post_request(rider_id, src_lat, src_lon, dst_lat, dst_lon, weight, volume, rider_type):
    data = {
        "riderId": rider_id,
        "source_lat": src_lat,
        "source_lon": src_lon,
        "dest_lat": dst_lat,
        "dest_lon": dst_lon,
        "weight": weight,
        "volume": volume,
        "cabOrCargo": rider_type
    }
    try:
        response = requests.post(RIDER_SEVICE_URL, json=data, headers=HEADERS)
        print(f"[INFO] Request sent: {response.status_code}")
        print(f"Waiting for match with potential driver, you'll be notified on a match within {KAFKA_TIMEOUT}s...")
        return 1
    except Exception as e:
        print(f"[ERROR] Failed to send POST request: {e}")
        return 0

def main():

    requestingFor = None # 'cab' or 'cargo'
    print("🚖 CrossGo says Hi!")
    print("\n[MAIN] commands format:")
    print("  r cab       --> to request a cab")
    print("  r cargo     --> to request cargo")
    print("  x <tripID>  --> to cancel trip\n")

    while True:
        statement = input(">>").split()
        if(len(statement) != 2):
            print('Invalid command format.')
            continue
        
        if(statement[0] == 'r'):
            if(statement[1] == 'cab'):
                rider_info = input("Enter rider_id, source_lat, source_lon, dest_lat, dest_lon:\n").split(',')
                success = send_post_request(rider_id=rider_info[0], src_lat=rider_info[1], src_lon=rider_info[2], dst_lat=rider_info[3], dst_lon=rider_info[4], weight="NA", volume="NA", rider_type="cab")
                if(success == 1):
                    ride_acceptance_detection = threading.Thread(target=kafka_listener, args=(rider_info[0],))
                    ride_acceptance_detection.start()

            elif(statement[1] == 'cargo'):
                pass
            else:
                print('Invalid mode, must be either \'cab\' or\'cargo\'')
                continue
        elif(statement[0] == 'x'):
            pass
        else:
            print('Wrong Command.')
            continue



if __name__ == "__main__":
    main()




 
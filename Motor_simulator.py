import time
import random
import json
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, Message
from dotenv import load_dotenv
import os

load_dotenv()


# ============================================================
# Azure IoT Hub Device Connection Strings
# Replace these with your actual device connection strings
# ============================================================

DEVICE_CONNECTION_STRINGS = {
    "MTR001": os.getenv("MTR001_CONNECTION_STRING"),
    "MTR002": os.getenv("MTR002_CONNECTION_STRING"),
    "MTR003": os.getenv("MTR003_CONNECTION_STRING"),
    "MTR004": os.getenv("MTR004_CONNECTION_STRING"),
}


# Create IoT Hub clients
clients = {}

for motor_id, connection_string in DEVICE_CONNECTION_STRINGS.items():
    clients[motor_id] = IoTHubDeviceClient.create_from_connection_string(
        connection_string
    )


def generate_sensor_data(motor_id):

    temperature = round(random.uniform(60, 90), 2)
    vibration = round(random.uniform(1, 8), 2)
    rpm = random.randint(1400, 1800)
    current = round(random.uniform(8, 15), 2)
    voltage = round(random.uniform(220, 240), 2)

    data = {
        "motor_id": motor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": temperature,
        "vibration": vibration,
        "rpm": rpm,
        "current": current,
        "voltage": voltage
    }

    return data


try:

    # Connect all motors
    for motor_id, client in clients.items():
        client.connect()
        print(f"{motor_id} connected to Azure IoT Hub")

    print("\nStarting sensor data transmission...\n")

    while True:

        for motor_id, client in clients.items():

            sensor_data = generate_sensor_data(motor_id)

            message = Message(
                json.dumps(sensor_data)
            )

            message.content_type = "application/json"
            message.content_encoding = "utf-8"

            client.send_message(message)

            print(
                f"{motor_id} → "
                f"Temperature: {sensor_data['temperature']}°C | "
                f"Vibration: {sensor_data['vibration']} mm/s | "
                f"RPM: {sensor_data['rpm']} | "
                f"Current: {sensor_data['current']} A"
            )

        print("\nWaiting 10 seconds...\n")
        time.sleep(10)


except KeyboardInterrupt:

    print("\nStopping sensor simulation...")


finally:

    for motor_id, client in clients.items():
        client.shutdown()

    print("All motor connections closed.")
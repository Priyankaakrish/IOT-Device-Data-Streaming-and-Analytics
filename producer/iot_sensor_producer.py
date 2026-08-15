"""
IoT Sensor Producer
====================
Simulates a fleet of factory machines streaming sensor telemetry into a
Kafka topic in real time. Each machine independently transitions between
RUNNING / IDLE / DOWN / FAULT states using a simple Markov-style model,
so the downstream pipeline sees realistic status changes, downtime
events, and occasional failure alerts -- not just flat random noise.

Usage:
    pip install -r requirements.txt
    python iot_sensor_producer.py --bootstrap-servers localhost:9092 --interval 1.0

Message schema (JSON, one per machine per tick):
{
  "machine_id": "M-101",
  "event_time": "2026-08-07T10:15:32.123456",
  "temperature_c": 68.4,
  "power_kw": 12.7,
  "vibration_mm_s": 1.9,
  "status": "RUNNING",
  "units_produced": 1,
  "good_units": 1
}
"""

import argparse
import json
import random
import time
from datetime import UTC, datetime

from kafka import KafkaProducer

MACHINES = [
    {"machine_id": "M-101", "base_temp": 65, "base_power": 12.0, "cycle_time": 12.0},
    {"machine_id": "M-102", "base_temp": 63, "base_power": 11.5, "cycle_time": 12.0},
    {"machine_id": "M-103", "base_temp": 45, "base_power": 4.0, "cycle_time": 4.0},
    {"machine_id": "M-104", "base_temp": 72, "base_power": 20.0, "cycle_time": 20.0},
    {"machine_id": "M-105", "base_temp": 50, "base_power": 6.0, "cycle_time": 6.5},
]

STATUSES = ["RUNNING", "IDLE", "DOWN", "FAULT"]

# Transition probabilities from each state -> next state.
TRANSITIONS = {
    "RUNNING": {"RUNNING": 0.93, "IDLE": 0.05, "DOWN": 0.015, "FAULT": 0.005},
    "IDLE": {"RUNNING": 0.35, "IDLE": 0.60, "DOWN": 0.03, "FAULT": 0.02},
    "DOWN": {"RUNNING": 0.20, "IDLE": 0.10, "DOWN": 0.68, "FAULT": 0.02},
    "FAULT": {"RUNNING": 0.10, "IDLE": 0.05, "DOWN": 0.15, "FAULT": 0.70},
}


class MachineState:
    def __init__(self, config):
        self.config = config
        self.status = "RUNNING"
        self.cycle_progress = 0.0

    def step(self, tick_seconds):
        # Transition status
        probs = TRANSITIONS[self.status]
        self.status = random.choices(list(probs.keys()), weights=list(probs.values()))[0]

        temp = self.config["base_temp"]
        power = self.config["base_power"]
        vibration = 1.0
        units = 0
        good = 0

        if self.status == "RUNNING":
            temp += random.uniform(-2, 6)  # running machines run hotter
            power += random.uniform(-0.5, 1.5)
            vibration = random.uniform(0.8, 2.2)
            self.cycle_progress += tick_seconds
            if self.cycle_progress >= self.config["cycle_time"]:
                self.cycle_progress = 0.0
                units = 1
                good = 0 if random.random() < 0.03 else 1  # ~3% defect rate
        elif self.status == "IDLE":
            temp += random.uniform(-3, 1)
            power = power * 0.15 + random.uniform(-0.2, 0.2)
            vibration = random.uniform(0.1, 0.5)
        elif self.status == "DOWN":
            temp += random.uniform(-8, -2)
            power = 0.0
            vibration = 0.0
        elif self.status == "FAULT":
            temp += random.uniform(8, 20)  # fault = overheating spike
            power += random.uniform(2, 5)
            vibration = random.uniform(3.5, 8.0)

        return {
            "machine_id": self.config["machine_id"],
            "event_time": datetime.now(UTC).isoformat(),
            "temperature_c": round(temp, 2),
            "power_kw": round(max(power, 0), 3),
            "vibration_mm_s": round(max(vibration, 0), 3),
            "status": self.status,
            "units_produced": units,
            "good_units": good,
        }


def main():
    parser = argparse.ArgumentParser(description="Simulate IoT sensor stream into Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="iot-sensor-data")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between ticks")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    machines = [MachineState(cfg) for cfg in MACHINES]

    print(
        f"Streaming simulated telemetry for {len(machines)} machines "
        f"to topic '{args.topic}' every {args.interval}s. Ctrl+C to stop."
    )

    try:
        while True:
            for m in machines:
                payload = m.step(args.interval)
                producer.send(args.topic, key=payload["machine_id"], value=payload)
                print(payload)
            producer.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

from kafka import KafkaProducer

# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "sensor-readings"

# One reading from every sensor every 2 seconds.
INTERVAL_SECONDS = 2


# ============================================================
# Simulation configuration
# ============================================================

# Probability that a sensor starts an anomaly episode
ANOMALY_START_PROBABILITY = 0.005

# Probability of sending a malformed message
MALFORMED_MESSAGE_PROBABILITY = 0.01

# Probability of sending a duplicate event
DUPLICATE_MESSAGE_PROBABILITY = 0.01

# Anomaly duration: 5–15 minutes
ANOMALY_MIN_SECONDS = 5 * 60
ANOMALY_MAX_SECONDS = 15 * 60


# ============================================================
# Sensors
# ============================================================

SENSORS = [
    # traffic_count
    {"sensor_id": "S001", "sensor_type": "traffic_count", "zone_id": 132},
    {"sensor_id": "S002", "sensor_type": "traffic_count", "zone_id": 138},
    {"sensor_id": "S003", "sensor_type": "traffic_count", "zone_id": 161},
    {"sensor_id": "S004", "sensor_type": "traffic_count", "zone_id": 236},
    {"sensor_id": "S005", "sensor_type": "traffic_count", "zone_id": 237},

    # CO
    {"sensor_id": "S006", "sensor_type": "co_ppm", "zone_id": 132},
    {"sensor_id": "S007", "sensor_type": "co_ppm", "zone_id": 138},
    {"sensor_id": "S008", "sensor_type": "co_ppm", "zone_id": 161},
    {"sensor_id": "S009", "sensor_type": "co_ppm", "zone_id": 236},
    {"sensor_id": "S010", "sensor_type": "co_ppm", "zone_id": 237},

    # temperature
    {"sensor_id": "S011", "sensor_type": "temperature_c", "zone_id": 132},
    {"sensor_id": "S012", "sensor_type": "temperature_c", "zone_id": 138},
    {"sensor_id": "S013", "sensor_type": "temperature_c", "zone_id": 161},
    {"sensor_id": "S014", "sensor_type": "temperature_c", "zone_id": 236},
    {"sensor_id": "S015", "sensor_type": "temperature_c", "zone_id": 237},

    # visibility
    {"sensor_id": "S016", "sensor_type": "visibility_m", "zone_id": 132},
    {"sensor_id": "S017", "sensor_type": "visibility_m", "zone_id": 138},
    {"sensor_id": "S018", "sensor_type": "visibility_m", "zone_id": 161},
    {"sensor_id": "S019", "sensor_type": "visibility_m", "zone_id": 236},
    {"sensor_id": "S020", "sensor_type": "visibility_m", "zone_id": 237},
]


# ============================================================
# Sensor state
# ============================================================

# Previous reading for each sensor.
previous_values = {}

# Stores when an anomaly episode should end.
# Example:
# {
#     "S006": 1791123456789
# }
anomaly_until = {}

# Stores the last event so we can intentionally duplicate it.
last_events = {}


# ============================================================
# Helper functions
# ============================================================

def now_ms():
    """Return current UTC time in epoch milliseconds."""
    return int(time.time() * 1000)


def start_anomaly_if_needed(sensor_id):
    """
    Occasionally start an anomaly episode.

    If the sensor is already in an anomaly episode,
    don't start another one.
    """

    current_time = now_ms()

    if sensor_id in anomaly_until:
        if current_time < anomaly_until[sensor_id]:
            return False

        # Existing anomaly has ended.
        del anomaly_until[sensor_id]

    if random.random() < ANOMALY_START_PROBABILITY:
        duration = random.randint(
            ANOMALY_MIN_SECONDS,
            ANOMALY_MAX_SECONDS,
        )

        anomaly_until[sensor_id] = (
            current_time + duration * 1000
        )

        print(
            f"[ANOMALY START] {sensor_id} "
            f"for approximately {duration // 60} minutes"
        )

        return True

    return False


def sensor_is_anomalous(sensor_id):
    """Return True if the sensor is currently in an anomaly episode."""

    if sensor_id not in anomaly_until:
        return False

    if now_ms() < anomaly_until[sensor_id]:
        return True

    del anomaly_until[sensor_id]
    print(f"[ANOMALY END] {sensor_id}")

    return False


# ============================================================
# Generate sensor values
# ============================================================

def generate_value(sensor):
    sensor_id = sensor["sensor_id"]
    sensor_type = sensor["sensor_type"]

    previous = previous_values.get(sensor_id)

    # Check whether an anomaly should start.
    start_anomaly_if_needed(sensor_id)

    anomalous = sensor_is_anomalous(sensor_id)

    sim_label = "anomaly" if anomalous else "normal"

    # --------------------------------------------------------
    # Traffic count
    # --------------------------------------------------------

    if sensor_type == "traffic_count":

        hour = datetime.now(UTC).hour

        # Lower traffic at night.
        # Higher traffic during morning/evening peaks.
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            baseline = 80

        elif 10 <= hour <= 16:
            baseline = 50

        else:
            baseline = 20

        if previous is None:
            value = baseline + random.randint(-10, 10)

        else:
            value = previous + random.randint(-8, 8)

        if anomalous:
            # Abnormally high traffic.
            value += random.randint(40, 100)

        value = max(0, min(value, 200))

    # --------------------------------------------------------
    # CO
    # --------------------------------------------------------

    elif sensor_type == "co_ppm":

        if previous is None:
            value = random.uniform(30, 70)

        else:
            value = previous + random.uniform(-8, 8)

        if anomalous:
            # Sustained elevated CO during anomaly episode.
            value += random.uniform(100, 250)

        # Occasional single-reading spike.
        elif random.random() < 0.02:
            value += random.uniform(100, 250)
            sim_label = "spike"

        value = max(10, min(value, 500))

    # --------------------------------------------------------
    # Temperature
    # --------------------------------------------------------

    elif sensor_type == "temperature_c":

        if previous is None:
            value = random.uniform(20, 28)

        else:
            value = previous + random.uniform(-0.5, 0.5)

        if anomalous:
            value += random.uniform(5, 12)

        value = max(10, min(value, 45))

    # --------------------------------------------------------
    # Visibility
    # --------------------------------------------------------

    elif sensor_type == "visibility_m":

        if previous is None:
            value = random.uniform(700, 1000)

        else:
            value = previous + random.uniform(-50, 50)

        if anomalous:
            # Poor visibility during anomaly.
            value -= random.uniform(300, 600)

        value = max(100, min(value, 1500))

    else:
        value = 0

    value = round(value, 2)

    previous_values[sensor_id] = value

    return value, sim_label


# ============================================================
# Create valid event
# ============================================================

def create_event(sensor):

    value, sim_label = generate_value(sensor)

    event = {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor["sensor_id"],
        "sensor_type": sensor["sensor_type"],
        "value": value,
        "event_ts_ms": now_ms(),
        "sim_label": sim_label,
    }

    return event


# ============================================================
# Create malformed event
# ============================================================

def create_malformed_event(sensor):

    malformed_type = random.choice(
        [
            "missing_event_id",
            "missing_sensor_id",
            "missing_sensor_type",
            "missing_value",
            "missing_timestamp",
            "invalid_sensor_type",
            "invalid_value",
        ]
    )

    event = create_event(sensor)

    if malformed_type == "missing_event_id":
        del event["event_id"]

    elif malformed_type == "missing_sensor_id":
        del event["sensor_id"]

    elif malformed_type == "missing_sensor_type":
        del event["sensor_type"]

    elif malformed_type == "missing_value":
        del event["value"]

    elif malformed_type == "missing_timestamp":
        del event["event_ts_ms"]

    elif malformed_type == "invalid_sensor_type":
        event["sensor_type"] = "unknown_sensor"

    elif malformed_type == "invalid_value":
        event["value"] = "not-a-number"

    return event


# ============================================================
# Kafka producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
)


# ============================================================
# Statistics
# ============================================================

total_sent = 0
normal_sent = 0
anomaly_sent = 0
spike_sent = 0
malformed_sent = 0
duplicate_sent = 0


# ============================================================
# Startup information
# ============================================================

print("=" * 60)
print("REAL-TIME SENSOR SIMULATOR")
print("=" * 60)
print(f"Kafka:      {KAFKA_BOOTSTRAP}")
print(f"Topic:      {TOPIC}")
print(f"Sensors:    {len(SENSORS)}")
print(f"Interval:   {INTERVAL_SECONDS} seconds")
print(f"Rate:       {len(SENSORS) / INTERVAL_SECONDS:.1f} events/sec")
print()
print("Simulation features:")
print("  - Normal sensor readings")
print("  - Gradual sensor movement")
print("  - Anomaly episodes")
print("  - Single spikes")
print("  - Malformed records")
print("  - Duplicate events")
print()
print("Press Ctrl+C to stop.")
print("=" * 60)


# ============================================================
# Main simulation loop
# ============================================================

try:

    while True:

        cycle_start = time.time()

        for sensor in SENSORS:

            sensor_id = sensor["sensor_id"]

            # ------------------------------------------------
            # Occasionally send a duplicate event.
            # ------------------------------------------------

            if (
                sensor_id in last_events
                and random.random() < DUPLICATE_MESSAGE_PROBABILITY
            ):

                duplicate_event = last_events[sensor_id]

                producer.send(
                    TOPIC,
                    key=sensor_id,
                    value=duplicate_event,
                )

                duplicate_sent += 1
                total_sent += 1

                print(
                    f"[DUPLICATE] "
                    f"{duplicate_event['event_id']} "
                    f"{sensor_id}"
                )

                continue

            # ------------------------------------------------
            # Occasionally send malformed data.
            # ------------------------------------------------

            if random.random() < MALFORMED_MESSAGE_PROBABILITY:

                event = create_malformed_event(sensor)

                producer.send(
                    TOPIC,
                    key=sensor_id,
                    value=event,
                )

                malformed_sent += 1
                total_sent += 1

                print(
                    f"[MALFORMED] "
                    f"{sensor_id}: "
                    f"{json.dumps(event)}"
                )

                continue

            # ------------------------------------------------
            # Normal valid event.
            # ------------------------------------------------

            event = create_event(sensor)

            producer.send(
                TOPIC,
                key=sensor_id,
                value=event,
            )

            # Store for possible duplicate later.
            last_events[sensor_id] = event

            total_sent += 1

            if event["sim_label"] == "anomaly":
                anomaly_sent += 1

            elif event["sim_label"] == "spike":
                spike_sent += 1

            else:
                normal_sent += 1

            print(json.dumps(event))

        # Make sure records are pushed to Kafka.
        producer.flush()

        # ----------------------------------------------------
        # Statistics
        # ----------------------------------------------------

        print()
        print(
            f"[STATS] "
            f"total={total_sent} | "
            f"normal={normal_sent} | "
            f"anomaly={anomaly_sent} | "
            f"spike={spike_sent} | "
            f"malformed={malformed_sent} | "
            f"duplicates={duplicate_sent}"
        )
        print()

        # Keep the configured interval between cycles.
        elapsed = time.time() - cycle_start
        sleep_time = max(0, INTERVAL_SECONDS - elapsed)

        time.sleep(sleep_time)


except KeyboardInterrupt:

    print("\nStopping sensor simulator...")


finally:

    producer.flush()
    producer.close()

    print("Sensor simulator stopped.")
    print(
        f"Final stats: "
        f"total={total_sent}, "
        f"normal={normal_sent}, "
        f"anomaly={anomaly_sent}, "
        f"spike={spike_sent}, "
        f"malformed={malformed_sent}, "
        f"duplicates={duplicate_sent}"
    )

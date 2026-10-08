import threading

class LamportClock:
    """Thread-safe implementation of a Lamport Logical Clock."""
    def __init__(self, initial_time: int = 0):
        self._lock = threading.Lock()
        self._value = initial_time

    def increment(self) -> int:
        """Increments internal clock by 1 for local events or before sending a message."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_time: int) -> int:
        """Updates internal clock on message receipt: max(own, received) + 1."""
        with self._lock:
            self._value = max(self._value, received_time) + 1
            return self._value

    def get_time(self) -> int:
        """Returns the current clock value."""
        with self._lock:
            return self._value


def log_event(node_id: str, event_type: str, details: str, clock_value: int, received_value: int = None) -> str:
    """Formats and prints event logs according to Task B1 requirements."""
    recv_str = f"  (received L={received_value})" if received_value is not None else ""
    line = f"[{node_id}] {event_type:<5} {details:<45} L={clock_value}{recv_str}"
    print(line, flush=True)
    return line
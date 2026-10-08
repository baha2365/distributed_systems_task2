import argparse
import concurrent.futures
import os
import sys
import threading
import time
import grpc

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock, log_event

class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, node_id: str = "replica-A", fault: str = None, delay_ms: int = 0):
        self.node_id = node_id
        self.fault = fault
        self.delay_ms = delay_ms
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self.clock = LamportClock()

    def Increment(self, request, context):

        recv_l = self.clock.update(request.lamport_time)
        log_event(self.node_id, "RECV", f"Increment(counter={request.counter_id}, delta={request.delta})", recv_l, received_value=request.lamport_time)


        if self.delay_ms > 0 or self.fault == "delay":
            sleep_sec = (self.delay_ms / 1000.0) if self.delay_ms > 0 else 3.0
            time.sleep(sleep_sec)


        if self.fault == "drop-after-recv":
            context.abort(grpc.StatusCode.UNAVAILABLE, "Fault injected: dropped request after receive")

        # Fault Injection: Crash mid-request
        if self.fault == "crash-mid-request":
            context.abort(grpc.StatusCode.INTERNAL, "Fault injected: node crashed mid-request")


        with self._lock:
            if request.idempotency_key in self._seen:
                val, _ = self._seen[request.idempotency_key]
                was_dup = True
            else:
                current = self._values.get(request.counter_id, 0)
                val = current + request.delta
                self._values[request.counter_id] = val
                self._seen[request.idempotency_key] = (val, False)
                was_dup = False

            apply_l = self.clock.increment()
            log_event(self.node_id, "APPLY", f"counter={request.counter_id} -> {val}", apply_l)

        # 3. Increment Lamport clock for SEND event
        send_l = self.clock.increment()
        log_event(self.node_id, "SEND", f"IncrementReply(new_value={val})", send_l)

        return counter_pb2.IncrementReply(
            new_value=val,
            was_duplicate=was_dup,
            lamport_time=send_l
        )

    def Get(self, request, context):
        recv_l = self.clock.update(request.lamport_time)
        log_event(self.node_id, "RECV", f"Get(counter={request.counter_id})", recv_l, received_value=request.lamport_time)

        with self._lock:
            found = request.counter_id in self._values
            val = self._values.get(request.counter_id, 0)

        send_l = self.clock.increment()
        log_event(self.node_id, "SEND", f"GetReply(value={val}, found={found})", send_l)

        return counter_pb2.GetReply(
            value=val,
            found=found,
            lamport_time=send_l
        )


def serve(port: int = 50051, node_id: str = "replica-A", fault: str = None, delay_ms: int = 0):
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(node_id=node_id, fault=fault, delay_ms=delay_ms)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, servicer


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replicated Counter gRPC Server")
    parser.add_argument("--port", type=int, default=50051, help="Port to listen on")
    parser.add_argument("--node-id", type=str, default="replica-A", help="Replica identifier")
    parser.add_argument("--fault", type=str, default=None, choices=["delay", "drop-after-recv", "crash-mid-request"], help="Injected fault mode")
    parser.add_argument("--delay-ms", type=int, default=0, help="Artificial delay in milliseconds")
    args = parser.parse_args()

    server, _ = serve(port=args.port, node_id=args.node_id, fault=args.fault, delay_ms=args.delay_ms)
    print(f"[{args.node_id}] Server started on port {args.port}...")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
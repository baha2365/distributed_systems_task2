import argparse
import concurrent.futures
import os
import sys
import time
import uuid
import grpc

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock, log_event

class ReplicatedCounterClient:
    def __init__(self, targets=None, client_id="client-1"):
        if targets is None:
            targets = ["localhost:50051", "localhost:50052", "localhost:50053"]
        self.targets = targets
        self.client_id = client_id
        self.clock = LamportClock()

    def _call_single_replica_incr(self, target: str, counter_id: str, delta: int, key: str, send_l: int, timeout: float):
        backoffs = [0.2, 0.4, 0.8]
        max_attempts = len(backoffs) + 1

        for attempt in range(max_attempts):
            try:
                channel = grpc.insecure_channel(target)
                stub = counter_pb2_grpc.CounterStub(channel)
                req = counter_pb2.IncrementRequest(
                    counter_id=counter_id,
                    delta=delta,
                    idempotency_key=key,
                    lamport_time=send_l
                )
                reply = stub.Increment(req, timeout=timeout)
                channel.close()
                return (target, reply, None)
            except grpc.RpcError as e:
                channel.close()
                if e.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL):
                    if attempt < len(backoffs):
                        time.sleep(backoffs[attempt])
                        continue
                return (target, None, e)
        return (target, None, Exception("Max retries exceeded"))

    def incr(self, counter_id: str, delta: int = 1, key: str = None, timeout: float = 2.0):

        if key is None:
            key = str(uuid.uuid4())

        send_l = self.clock.increment()
        log_event(self.client_id, "SEND", f"Increment(counter={counter_id}, delta={delta})", send_l)


        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.targets)) as executor:
            futures = [
                executor.submit(self._call_single_replica_incr, target, counter_id, delta, key, send_l, timeout)
                for target in self.targets
            ]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        acks = 0
        committed_val = None
        was_dup = False
        max_reply_lamport = 0

        for target, reply, err in results:
            if reply is not None:
                acks += 1
                committed_val = reply.new_value
                was_dup = reply.was_duplicate or was_dup
                if reply.lamport_time > max_reply_lamport:
                    max_reply_lamport = reply.lamport_time

        if max_reply_lamport > 0:
            recv_l = self.clock.update(max_reply_lamport)
            if committed_val is not None:
                log_event(self.client_id, "RECV", f"IncrementReply(new_value={committed_val})", recv_l, received_value=max_reply_lamport)

        majority = (len(self.targets) // 2) + 1
        committed = acks >= majority

        return {
            "committed": committed,
            "new_value": committed_val,
            "was_duplicate": was_dup,
            "acks": acks,
            "total_replicas": len(self.targets),
            "key": key
        }

    def get(self, counter_id: str, timeout: float = 2.0):
        send_l = self.clock.increment()
        log_event(self.client_id, "SEND", f"Get(counter={counter_id})", send_l)

        for target in self.targets:
            try:
                channel = grpc.insecure_channel(target)
                stub = counter_pb2_grpc.CounterStub(channel)
                req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=send_l)
                reply = stub.Get(req, timeout=timeout)
                channel.close()

                recv_l = self.clock.update(reply.lamport_time)
                log_event(self.client_id, "RECV", f"GetReply(value={reply.value}, found={reply.found})", recv_l, received_value=reply.lamport_time)

                return {
                    "value": reply.value,
                    "found": reply.found,
                    "target": target
                }
            except grpc.RpcError:
                channel.close()
                continue

        raise RuntimeError("All replicas failed to respond to Get request")


def main():
    parser = argparse.ArgumentParser(description="Replicated Counter gRPC Client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    incr_parser = subparsers.add_parser("incr")
    incr_parser.add_argument("counter_id", type=str, help="Counter ID")
    incr_parser.add_argument("--by", type=int, default=1, help="Delta to increment by")
    incr_parser.add_argument("--key", type=str, default=None, help="Explicit idempotency key")
    incr_parser.add_argument("--ports", type=str, default="50051,50052,50053", help="Comma-separated replica ports")
    incr_parser.add_argument("--timeout", type=float, default=2.0, help="Per-request timeout")

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("counter_id", type=str, help="Counter ID")
    get_parser.add_argument("--ports", type=str, default="50051,50052,50053", help="Comma-separated replica ports")
    get_parser.add_argument("--timeout", type=float, default=2.0, help="Per-request timeout")

    args = parser.parse_args()
    ports = [p.strip() for p in args.ports.split(",") if p.strip()]
    targets = [f"localhost:{p}" for p in ports]

    client = ReplicatedCounterClient(targets=targets)

    if args.command == "incr":
        res = client.incr(args.counter_id, delta=args.by, key=args.key, timeout=args.timeout)
        dup_str = "yes" if res["was_duplicate"] else "no"
        if res["committed"]:
            print(f"OK committed value={res['new_value']} (replicas acked: {res['acks']}/{res['total_replicas']}, duplicate: {dup_str})")
        else:
            print(f"FAIL write rejected (replicas acked: {res['acks']}/{res['total_replicas']} < majority {len(targets)//2 + 1})")
            sys.exit(1)

    elif args.command == "get":
        res = client.get(args.counter_id, timeout=args.timeout)
        if res["found"]:
            print(f"value={res['value']}")
        else:
            print("counter not found")

if __name__ == "__main__":
    main()
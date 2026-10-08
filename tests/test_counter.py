import concurrent.futures
import time
import pytest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server import serve
from client import ReplicatedCounterClient

@pytest.fixture
def running_single_server():
    port = 50101
    server, servicer = serve(port=port, node_id="test-replica-1")
    yield f"localhost:{port}"
    server.stop(0)

@pytest.fixture
def running_three_servers():
    ports = [50201, 50202, 50203]
    servers = []
    servicers = []
    targets = []
    for i, port in enumerate(ports):
        s, serv = serve(port=port, node_id=f"replica-{chr(65+i)}")
        servers.append(s)
        servicers.append(serv)
        targets.append(f"localhost:{port}")
    yield targets, servicers, servers
    for s in servers:
        s.stop(0)

def test_increment_applies_delta(running_single_server):
    client = ReplicatedCounterClient(targets=[running_single_server])
    res = client.incr("test_counter", delta=5)
    assert res["committed"] is True
    assert res["new_value"] == 5
    assert res["was_duplicate"] is False

def test_duplicate_key_not_reapplied(running_single_server):
    client = ReplicatedCounterClient(targets=[running_single_server])
    key = "fixed-key-123"
    r1 = client.incr("x", delta=5, key=key)
    r2 = client.incr("x", delta=5, key=key)
    assert r1["new_value"] == 5
    assert r1["was_duplicate"] is False
    assert r2["new_value"] == 5
    assert r2["was_duplicate"] is True

def test_concurrent_increments_exact(running_single_server):
    client = ReplicatedCounterClient(targets=[running_single_server])
    counter_id = "concurrent_counter"

    def worker():
        for _ in range(1000):
            client.incr(counter_id, delta=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(worker)
        f2 = executor.submit(worker)
        f1.result()
        f2.result()

    res = client.get(counter_id)
    assert res["found"] is True
    assert res["value"] == 2000

def test_get_missing_counter(running_single_server):
    client = ReplicatedCounterClient(targets=[running_single_server])
    res = client.get("non_existent_counter")
    assert res["found"] is False
    assert res["value"] == 0

def test_retry_after_timeout_is_safe():
    port = 50301
    server, servicer = serve(port=port, node_id="delayed-replica", delay_ms=2500)
    try:
        client = ReplicatedCounterClient(targets=[f"localhost:{port}"])
        res = client.incr("delayed_cnt", delta=10, timeout=1.0)
        assert res["committed"] is True
        assert res["new_value"] == 10
    finally:
        server.stop(0)

def test_majority_commit_two_acks(running_three_servers):
    targets, servicers, servers = running_three_servers
    servers[2].stop(0)
    client = ReplicatedCounterClient(targets=targets)
    res = client.incr("likes", delta=1)
    assert res["committed"] is True
    assert res["acks"] == 2

def test_no_commit_below_majority(running_three_servers):
    targets, servicers, servers = running_three_servers
    servers[1].stop(0)
    servers[2].stop(0)
    client = ReplicatedCounterClient(targets=targets)
    res = client.incr("likes", delta=1)
    assert res["committed"] is False
    assert res["acks"] == 1

def test_replicas_converge(running_three_servers):
    targets, servicers, servers = running_three_servers
    client = ReplicatedCounterClient(targets=targets)
    for i in range(10):
        client.incr("converge_cnt", delta=1)

    for servicer in servicers:
        assert servicer._values["converge_cnt"] == 10
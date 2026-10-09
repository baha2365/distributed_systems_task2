import concurrent.futures
import time
import pytest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server import serve
from client import ReplicatedCounterClient

@pytest.fixture
def running_three_servers():
    ports = [50401, 50402, 50403]
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

def test_replica_crash_mid_request(running_three_servers):
    targets, servicers, servers = running_three_servers
    servicers[2].fault = "crash-mid-request"
    client = ReplicatedCounterClient(targets=targets)
    res = client.incr("crash_test", delta=1)
    assert res["committed"] is True
    assert res["acks"] == 2
    assert res["new_value"] == 1

def test_request_duplication(running_three_servers):
    targets, servicers, servers = running_three_servers
    client = ReplicatedCounterClient(targets=targets)
    key = "dup-test-key-999"
    res1 = client.incr("dup_counter", delta=7, key=key)
    res2 = client.incr("dup_counter", delta=7, key=key)
    assert res1["committed"] is True
    assert res1["new_value"] == 7
    assert res1["was_duplicate"] is False
    assert res2["committed"] is True
    assert res2["new_value"] == 7
    assert res2["was_duplicate"] is True

def test_induced_timeout_with_retry(running_three_servers):
    targets, servicers, servers = running_three_servers
    servicers[0].delay_ms = 3000  # Force timeout on replica A
    client = ReplicatedCounterClient(targets=targets)
    res = client.incr("timeout_cnt", delta=3, timeout=1.0)
    assert res["committed"] is True
    assert res["acks"] == 2
    assert res["new_value"] == 3
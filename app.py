"""
Vault — Distributed Object Storage Backend
Flask REST API implementing: replication, write/read consistency levels
(ONE/QUORUM/ALL), checksums, corruption injection, node failure/partition,
and background + read-repair.

Run:
    pip install -r requirements.txt
    python app.py
Then open http://localhost:5000/api/health to confirm it's running.
"""

import hashlib
import random
import threading
import time
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # allow a frontend on a different origin to call this API

lock = threading.Lock()

N_NODES = 8
nodes = {f"n{i+1}": {"id": f"n{i+1}", "status": "healthy"} for i in range(N_NODES)}

# key -> {size, rf, write_cl, checksum, replicas: [{node, checksum}]}
objects = {}

stats = {
    "background_repairs": 0,
    "read_repairs": 0,
    "write_quorum_fails": 0,
    "read_quorum_fails": 0,
}

STATUS_CYCLE = ["healthy", "failed", "partitioned"]


def checksum_of(*parts):
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()
    return h[:12]


def quorum_for(rf, cl):
    if cl == "ONE":
        return 1
    if cl == "ALL":
        return rf
    return rf // 2 + 1  # QUORUM


def healthy_node_ids():
    return [n["id"] for n in nodes.values() if n["status"] == "healthy"]


def pick_nodes(count, exclude):
    pool = [nid for nid in healthy_node_ids() if nid not in exclude]
    random.shuffle(pool)
    return pool[:count]


def status_of(obj):
    valid = sum(
        1 for r in obj["replicas"]
        if r["checksum"] == obj["checksum"] and nodes[r["node"]]["status"] == "healthy"
    )
    mismatch = any(r["checksum"] != obj["checksum"] for r in obj["replicas"])
    if valid == 0:
        return "unavailable"
    if valid < obj["rf"] or mismatch:
        return "degraded"
    return "consistent"


def repair_cycle():
    repaired = healed = 0
    with lock:
        for key, obj in objects.items():
            # drop replicas on failed nodes — data considered lost there
            obj["replicas"] = [r for r in obj["replicas"] if nodes[r["node"]]["status"] != "failed"]

            def is_good(r):
                return r["checksum"] == obj["checksum"] and nodes[r["node"]]["status"] == "healthy"

            good_source = next((r for r in obj["replicas"] if is_good(r)), None)

            # scrub mismatched checksums on reachable nodes if a good source exists
            for r in obj["replicas"]:
                if r["checksum"] != obj["checksum"] and nodes[r["node"]]["status"] == "healthy" and good_source:
                    r["checksum"] = obj["checksum"]
                    healed += 1

            have = [r["node"] for r in obj["replicas"]]
            valid_count = sum(1 for r in obj["replicas"] if is_good(r))
            needed = obj["rf"] - valid_count
            if needed > 0 and (valid_count > 0 or len(obj["replicas"]) == 0):
                for nid in pick_nodes(needed, have):
                    obj["replicas"].append({"node": nid, "checksum": obj["checksum"]})
                    repaired += 1
        stats["background_repairs"] += repaired + healed
    return repaired, healed


def background_repair_loop():
    while True:
        time.sleep(5)
        repair_cycle()


threading.Thread(target=background_repair_loop, daemon=True).start()


# ---------- routes ----------

@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "time": time.time()})


@app.get("/api/nodes")
def list_nodes():
    return jsonify(list(nodes.values()))


@app.post("/api/nodes/<node_id>/cycle")
def cycle_node(node_id):
    with lock:
        if node_id not in nodes:
            return jsonify({"error": "unknown node"}), 404
        cur = nodes[node_id]["status"]
        nodes[node_id]["status"] = STATUS_CYCLE[(STATUS_CYCLE.index(cur) + 1) % 3]
    return jsonify(nodes[node_id])


@app.get("/api/objects")
def list_objects():
    with lock:
        return jsonify([
            {**obj, "key": key, "status": status_of(obj)}
            for key, obj in objects.items()
        ])


@app.post("/api/objects")
def put_object():
    body = request.get_json(force=True)
    key = body.get("key")
    size = int(body.get("size", 1))
    rf = int(body.get("rf", 3))
    write_cl = body.get("write_cl", "QUORUM")
    if not key:
        return jsonify({"error": "key required"}), 400
    with lock:
        if key in objects:
            return jsonify({"error": "key already exists"}), 409
        checksum = checksum_of(key, size, random.random())
        chosen = pick_nodes(rf, [])
        required = quorum_for(rf, write_cl)
        satisfied = len(chosen) >= required
        if not satisfied:
            stats["write_quorum_fails"] += 1
        objects[key] = {
            "size": size, "rf": rf, "write_cl": write_cl, "checksum": checksum,
            "replicas": [{"node": nid, "checksum": checksum} for nid in chosen],
        }
    return jsonify({
        "key": key, "acked": len(chosen), "required": required,
        "satisfied": satisfied, "checksum": checksum,
    }), (201 if satisfied else 207)


@app.get("/api/objects/<key>")
def get_object(key):
    cl = request.args.get("cl", "QUORUM")
    with lock:
        obj = objects.get(key)
        if not obj:
            return jsonify({"error": "not found"}), 404
        required = quorum_for(obj["rf"], cl)
        reachable = [r for r in obj["replicas"] if nodes[r["node"]]["status"] == "healthy"]
        if len(reachable) < required:
            stats["read_quorum_fails"] += 1
            return jsonify({
                "error": "consistency level not met",
                "reachable": len(reachable), "required": required,
            }), 503
        sample = random.sample(reachable, required)
        matches = [r for r in sample if r["checksum"] == obj["checksum"]]
        mismatches = [r for r in sample if r["checksum"] != obj["checksum"]]
        if not matches:
            stats["read_quorum_fails"] += 1
            return jsonify({"error": "no consistent value among sampled replicas"}), 409
        for r in mismatches:
            r["checksum"] = obj["checksum"]
        stats["read_repairs"] += len(mismatches)
    return jsonify({
        "key": key, "checksum": obj["checksum"],
        "served_by": [r["node"] for r in sample],
        "read_repaired": [r["node"] for r in mismatches],
    })


@app.delete("/api/objects/<key>")
def delete_object(key):
    with lock:
        objects.pop(key, None)
    return jsonify({"deleted": key})


@app.post("/api/objects/<key>/corrupt")
def corrupt_object(key):
    with lock:
        obj = objects.get(key)
        if not obj:
            return jsonify({"error": "not found"}), 404
        live = [
            r for r in obj["replicas"]
            if nodes[r["node"]]["status"] != "failed" and r["checksum"] == obj["checksum"]
        ]
        if not live:
            return jsonify({"error": "no eligible replica to corrupt"}), 409
        pick = random.choice(live)
        pick["checksum"] = checksum_of(pick["checksum"], "corrupt", random.random())
    return jsonify({"corrupted_node": pick["node"], "new_checksum": pick["checksum"]})


@app.post("/api/repair")
def trigger_repair():
    repaired, healed = repair_cycle()
    return jsonify({"repaired": repaired, "healed": healed})


@app.get("/api/stats")
def get_stats():
    with lock:
        return jsonify({
            **stats,
            "healthy_nodes": len(healthy_node_ids()),
            "total_nodes": N_NODES,
            "objects": len(objects),
            "degraded_objects": sum(1 for o in objects.values() if status_of(o) != "consistent"),
        })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

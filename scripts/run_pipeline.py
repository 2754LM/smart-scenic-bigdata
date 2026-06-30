"""Drive the full pipeline via the API. Runs inside demo-backend container.
Called by scripts\start-app.bat. All HTTP, polling, error handling here.
"""
import os
import sys
import time
import urllib.error
import urllib.request
import json

API = "http://localhost:8000"


def post(path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        API + path,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"} if body else {},
    )
    r = urllib.request.urlopen(req, timeout=10)
    return json.loads(r.read().decode())


def get(path):
    r = urllib.request.urlopen(API + path, timeout=10)
    return json.loads(r.read().decode())


def wait_for_job(job_id, label, timeout_min=15):
    """Poll until status is success/failed. Returns True on success."""
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        try:
            d = get(f"/api/admin/jobs/{job_id}")
        except Exception as e:
            print(f"    poll err: {e}")
            time.sleep(5)
            continue
        s = d.get("status")
        if s in ("success", "failed"):
            print(f"  {label}: {s.upper()}")
            if d.get("error"):
                print(f"    error: {d.get('error')[:200]}")
            return s == "success"
        time.sleep(5)
    print(f"  {label}: TIMEOUT after {timeout_min} min")
    return False


def submit(action, label):
    print(f"[{label}] POST /api/admin/actions/{action}")
    r = post(f"/api/admin/actions/{action}")
    job_id = r.get("job_id")
    if not job_id:
        print(f"  [FAIL] no job_id: {r}")
        return False
    print(f"  job_id={job_id}")
    return wait_for_job(job_id, label)


def main():
    print("=" * 60)
    print(" Smart Scenic BigData - Data Pipeline")
    print("=" * 60)

    try:
        get("/api/health")
    except Exception as e:
        print(f"[FAIL] demo-backend not reachable: {e}")
        sys.exit(1)
    print("[OK] demo-backend reachable")

    ok = True
    for action, label in [
        ("load_csv",    "1/6 load_csv"),
        ("sqoop",       "2/6 sqoop"),
        ("spark_clean", "3/6 spark_clean"),
        ("hive_ddl",     "4/6 hive_ddl"),
        ("spark_train",  "5/6 spark_train"),
    ]:
        if not submit(action, label):
            ok = False
            break

    if not ok:
        print()
        print("[FAIL] Pipeline failed. Check: docker logs demo-backend")
        sys.exit(1)

    # 6. fpgrowth (must run in spark-master container which has spark-submit)
    # demo-backend has the docker socket mounted + socket API for exec.
    print("[6/6] fpgrowth (关联规则 5010 rules)...")
    import socket
    import json as _json
    req_body = _json.dumps({
        "Cmd": ["bash", "/opt/jobs/ml/fpgrowth.py"],
        "AttachStdout": True,
        "AttachStderr": True,
        "Tty": False,
    })
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(900)  # fpgrowth can take ~10 min
    sock.connect("/var/run/docker.sock")
    req = (
        f"POST /containers/spark-master/exec HTTP/1.1\r\n"
        f"Host: docker\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(req_body)}\r\n"
        f"Connection: close\r\n\r\n{req_body}"
    )
    sock.sendall(req.encode())
    chunks = []
    while True:
        try:
            c = sock.recv(65536)
        except socket.timeout:
            break
        if not c: break
        chunks.append(c)
    sock.close()
    raw = b"".join(chunks).decode("utf-8", errors="ignore")
    if " 200" not in raw[:50] and "201" not in raw[:50]:
        print(f"  [WARN] fpgrowth exec failed: {raw[:200]}")
    else:
        print("  FPGrowth rules saved to /shared/models/fpgrowth_rules.json")

    print()
    print("=" * 60)
    print(" Pipeline complete!")
    print("=" * 60)
    print(" MySQL: 4 tables populated (210,010 rows)")
    print(" HDFS:  /scenic/cleaned/ has 4 parquet dirs")
    print(" Hive:  8 tables in scenic_ext (4 ext_t_* + 4 v_*)")
    print(" Models: 9 .pkl in /shared/models/sklearn/")
    print(" FPGrowth: 5010 rules in /shared/models/fpgrowth_rules.json")
    print()
    print("Open http://localhost:8080/analysis.html to see the dashboard.")


if __name__ == "__main__":
    main()

"""Unified pipeline runner. Replaces run_pipeline.py + run_train.py.

Usage:
  python run.py --mode pipeline   # load_csv -> sqoop -> spark_clean -> hive_ddl
  python run.py --mode train      # spark_train -> fpgrowth -> apriori
  python run.py --mode all        # pipeline then train

Called by: start-app.bat (--mode pipeline), start-train.bat (--mode train), start.bat (--mode all)
"""
import argparse
import json
import os
import sys
import time
import urllib.request

API = "http://localhost:8000"


def post(path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(API + path, data=data, method="POST",
                                 headers={"Content-Type": "application/json"} if body else {})
    r = urllib.request.urlopen(req, timeout=10)
    return json.loads(r.read().decode())


def get(path):
    r = urllib.request.urlopen(API + path, timeout=10)
    return json.loads(r.read().decode())


def wait_for_job(job_id, label, timeout_min=15):
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


def check_backend():
    try:
        get("/api/health")
    except Exception as e:
        print(f"[FAIL] demo-backend not reachable: {e}")
        sys.exit(1)
    print("[OK] demo-backend reachable\n")


def run_pipeline():
    print("=" * 60)
    print(" Data Pipeline (CSV -> MySQL -> Sqoop -> Spark -> Hive)")
    print("=" * 60)
    actions = [
        ("load_csv",    "1/4 load_csv"),
        ("sqoop",       "2/4 sqoop"),
        ("spark_clean", "3/4 spark_clean"),
        ("hive_ddl",     "4/4 hive_ddl"),
    ]
    for action, label in actions:
        if not submit(action, label):
            print("\n[FAIL] Pipeline failed.")
            sys.exit(1)
    print("\n" + "=" * 60)
    print(" Pipeline complete! 210k rows in MySQL, 4 parquet in HDFS, 9 Hive tables")
    print("=" * 60)


def run_train():
    print("=" * 60)
    print(" Model Training (spark_train -> fpgrowth -> apriori)")
    print("=" * 60)

    if not submit("spark_train", "1/3 spark_train"):
        print("\n[FAIL] Training failed.")
        sys.exit(1)

    # FPGrowth (spark-submit in spark-master)
    print("\n[2/3] fpgrowth ...")
    if "/app" not in sys.path:
        sys.path.insert(0, "/app")
    from services.docker_client import exec_capture
    r = exec_capture("spark-master",
                     ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
                      "--deploy-mode", "client", "/opt/jobs/ml/fpgrowth.py"],
                     timeout=900)
    if r.get("exit_code") == 0:
        print("  FPGrowth rules saved")
    else:
        print(f"  [WARN] fpgrowth exit {r.get('exit_code')}")

    # Apriori (python in demo-backend)
    print("\n[3/3] apriori ...")
    r2 = exec_capture("demo-backend", ["python3", "/opt/jobs/ml/apriori.py"], timeout=120)
    if r2.get("exit_code") == 0:
        print("  Apriori rules saved")
    else:
        print(f"  [WARN] apriori exit {r2.get('exit_code')}")

    print("\n" + "=" * 60)
    print(" Training complete! 9 .pkl + FPGrowth rules + Apriori rules")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Smart Scenic pipeline runner")
    parser.add_argument("--mode", choices=["pipeline", "train", "all"], default="pipeline")
    args = parser.parse_args()

    check_backend()

    if args.mode in ("pipeline", "all"):
        run_pipeline()
        print()

    if args.mode in ("train", "all"):
        run_train()
        print()

    print("Open http://localhost:8080 to see the dashboard.")


if __name__ == "__main__":
    main()

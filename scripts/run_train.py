"""Run model training (spark_train + fpgrowth) on demand.

Used by scripts\start-train.bat. Models are written to

/shared/models/sklearn/ and fpgrowth rules to /shared/models/.

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

    print(" Smart Scenic BigData - Model Training")

    print("=" * 60)



    try:

        get("/api/health")

    except Exception as e:

        print(f"[FAIL] demo-backend not reachable: {e}")

        sys.exit(1)

    print("[OK] demo-backend reachable")



    ok = True

    if not submit("spark_train", "1/2 spark_train"):

        ok = False
    # Step 3: Apriori (runs in demo-backend, not spark-master)
    print()
    print("[3/3] Running Apriori ...")
    try:
        r2 = exec_capture("demo-backend", ["python3", "/opt/jobs/ml/apriori.py"], timeout=120)
        if r2.get("exit_code") == 0:
            print("  Apriori rules saved to /shared/models/apriori_rules.json")
        else:
            print("  [WARN] apriori failed")
    except Exception as e:
        print(f"  [WARN] apriori: {e}")

    if ok:

        # FPGrowth runs in spark-master container via spark-submit

        # (it needs PySpark context; direct python fails).

        print("[2/2] fpgrowth (关联规则 5010 rules)...")

        if "/app" not in sys.path:

            sys.path.insert(0, "/app")

        from services.docker_client import exec_capture

        r = exec_capture(

            "spark-master",

            ["/opt/spark/bin/spark-submit",

             "--master", "spark://spark-master:7077",

             "--deploy-mode", "client",

             "/opt/jobs/ml/fpgrowth.py"],

            timeout=900,

        )

        if r.get("exit_code") == 0:

            print("  FPGrowth rules saved to /shared/models/fpgrowth_rules.json")

        else:

            print(f"  [WARN] fpgrowth exit {r.get('exit_code')}, stderr={r.get('stderr','')[:200]}")

            ok = False



    if not ok:

        print()

        print("[FAIL] Training failed. Check: docker logs demo-backend")

        sys.exit(1)



    print()

    print("=" * 60)

    print(" Training complete!")

    print("=" * 60)

    print(" Models: 9 .pkl in /shared/models/sklearn/")

    print(" FPGrowth: 5010 rules in /shared/models/fpgrowth_rules.json
 print(" Apriori: 810 rules in /shared/models/apriori_rules.json")")

    print()

    print("Open http://localhost:8080/predict.html to test predictions.")





if __name__ == "__main__":

    main()


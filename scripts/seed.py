import argparse
import datetime as dt
import json
import random
import urllib.request

SPECIALTIES = ["кардіологія", "неврологія", "ендокринологія", "онкологія",
               "офтальмологія", "травматологія", "гастроентерологія", "пульмонологія"]


def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--n", type=int, default=100)
    args = ap.parse_args()
    rnd = random.Random()
    ids = []
    for i in range(args.n):
        body = {
            "patient_code": f"P-{rnd.randint(100000, 999999)}",
            "specialty": rnd.choice(SPECIALTIES),
            "consultation_date": (dt.date(2026, 1, 1) + dt.timedelta(days=rnd.randint(0, 300))).isoformat(),
            "conclusion": f"Стан пацієнта стабільний, патологічних змін не виявлено, запис {i + 1}.",
            "recommendations": "Контрольний огляд через 3 місяці.",
        }
        status, data = post(f"{args.url}/consultations", body)
        assert status == 201, (status, data)
        ids.append(data["id"])
    print(f"created {len(ids)} records, ids {ids[0]}..{ids[-1]}")


if __name__ == "__main__":
    main()

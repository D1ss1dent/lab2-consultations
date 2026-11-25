import argparse
import json
import os
import urllib.error
import urllib.request

RESULTS = os.path.join(os.path.dirname(__file__), "..", "results")
LOG = []


def call(base, method, path, body=None):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            status, raw, served = r.status, r.read(), r.headers.get("X-Pod")
    except urllib.error.HTTPError as e:
        status, raw, served = e.code, e.read(), e.headers.get("X-Pod")
    text = raw.decode("utf-8") if raw else ""
    try:
        parsed = json.loads(text) if text else None
    except ValueError:
        parsed = text
    LOG.append((method, path, body, status, parsed, served))
    print(f"{method} {path} -> {status}")
    return status, parsed


def expect(status, wanted, step):
    mark = "OK" if status == wanted else "FAIL"
    LOG[-1] = LOG[-1] + (step, wanted, mark)
    if status != wanted:
        print(f"   !!! {step}: отримано {status}, очікувалось {wanted}")


def write_log(name):
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Журнал перевірки\n\n")
        for i, (m, p, body, st, resp, served, step, wanted, mark) in enumerate(LOG, 1):
            f.write(f"## {i}. {step}\n\n")
            f.write(f"Запит: `{m} {p}`\n\n")
            if body is not None:
                f.write("```json\n" + json.dumps(body, ensure_ascii=False, indent=2) + "\n```\n\n")
            f.write(f"Відповідь: `{st}`, очікувалось `{wanted}`, {mark}"
                    f"{f', інстанс {served}' if served else ''}\n\n")
            if resp is not None:
                f.write("```json\n" + json.dumps(resp, ensure_ascii=False, indent=2) + "\n```\n\n")
    print(f"log -> {os.path.abspath(path)}")


VALID = {
    "patient_code": "P-000777",
    "specialty": "кардіологія",
    "consultation_date": "2026-11-20",
    "conclusion": "Ознак гострої серцевої недостатності не виявлено.",
    "recommendations": "ЕКГ-контроль через 1 місяць, обмеження солі.",
}


def crud(base):
    st, _ = call(base, "GET", "/healthz"); expect(st, 200, "Готовність сервісу")
    st, created = call(base, "POST", "/consultations", VALID); expect(st, 201, "Створення запису")
    cid = created["id"]
    bad = dict(VALID, conclusion="Коротко.")
    st, _ = call(base, "POST", "/consultations", bad)
    expect(st, 400, "Створення з висновком < 20 символів")
    missing = {k: v for k, v in VALID.items() if k != "specialty"}
    st, _ = call(base, "POST", "/consultations", missing)
    expect(st, 400, "Створення без обов'язкового поля specialty")
    st, lst = call(base, "GET", "/consultations?specialty=%D0%BA%D0%B0%D1%80%D0%B4%D1%96%D0%BE%D0%BB%D0%BE%D0%B3%D1%96%D1%8F&limit=100")
    expect(st, 200, "Перелік із фільтром за спеціальністю")
    print("   запис у переліку:", any(it["id"] == cid for it in lst["items"]))
    st, _ = call(base, "GET", "/consultations?limit=1000"); expect(st, 400, "Перелік з limit > 100")
    st, _ = call(base, "GET", f"/consultations/{cid}"); expect(st, 200, "Отримання за id")
    new = dict(VALID, specialty="неврологія",
               conclusion="Після повторного огляду неврологічного дефіциту не виявлено.",
               recommendations="Повторна консультація не потрібна.")
    st, _ = call(base, "PUT", f"/consultations/{cid}", new); expect(st, 200, "Повна заміна PUT")
    st, _ = call(base, "PUT", f"/consultations/{cid}", {"specialty": "x"})
    expect(st, 400, "PUT з неповним тілом")
    st, _ = call(base, "PUT", "/consultations/999999999", new); expect(st, 404, "PUT неіснуючого id")
    st, _ = call(base, "DELETE", f"/consultations/{cid}"); expect(st, 204, "Видалення")
    st, _ = call(base, "DELETE", f"/consultations/{cid}"); expect(st, 404, "Повторне видалення")
    st, _ = call(base, "GET", f"/consultations/{cid}"); expect(st, 404, "Отримання видаленого")
    write_log("verify_log.md")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["crud", "persist-create", "persist-check", "empty-check"])
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    a = ap.parse_args()
    idfile = os.path.join(RESULTS, "persist_id.txt")
    if a.action == "crud":
        crud(a.url)
    elif a.action == "persist-create":
        st, created = call(a.url, "POST", "/consultations", VALID)
        expect(st, 201, "Створення перед down/up")
        os.makedirs(RESULTS, exist_ok=True)
        open(idfile, "w").write(str(created["id"]))
        write_log("persist_1_create.md")
    elif a.action == "persist-check":
        cid = open(idfile).read().strip()
        st, _ = call(a.url, "GET", f"/consultations/{cid}")
        expect(st, 200, "Після down + up запис на місці")
        write_log("persist_2_after_down_up.md")
    else:
        st, lst = call(a.url, "GET", "/consultations")
        expect(st, 200, "Після down -v + up")
        print("   total =", lst["total"])
        write_log("persist_3_after_down_v.md")


if __name__ == "__main__":
    main()

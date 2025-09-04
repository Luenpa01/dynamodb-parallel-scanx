import csv, json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            try:
                return float(o) if (abs(o) % 1) else int(o)
            except Exception:
                return float(o)
        return super().default(o)

def write_jsonl_items(pages_iter, out):
    for page in pages_iter:
        for item in page.get("Items", []):
            out.write(json.dumps(item, cls=DecimalEncoder) + "\n")

def write_jsonl_pages(pages_iter, out):
    for page in pages_iter:
        out.write(json.dumps(page, cls=DecimalEncoder) + "\n")

def write_csv_items(pages_iter, out_path):
    rows = []
    for page in pages_iter:
        for item in page.get("Items", []):
            flat = {}
            for k, v in item.items():
                if isinstance(v, dict) and len(v) == 1:
                    flat[k] = next(iter(v.values()))
                else:
                    flat[k] = v
            rows.append(flat)
    if not rows:
        return 0
    cols = sorted({c for r in rows for c in r.keys()})
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)

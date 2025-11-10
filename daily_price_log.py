import os, csv, datetime, requests, json, glob, traceback, time, random

# -------------------------
# Config / constants
# -------------------------
SHOP_DOMAIN = os.environ["SHOP_DOMAIN"]
ADMIN_TOKEN = os.environ["SHOPIFY_ADMIN_TOKEN"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

SNAPSHOT_DIR = "./snapshots"
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# Use UTC so filenames sort nicely and avoid daylight issues
NOW = datetime.datetime.utcnow()
TODAY_STR = NOW.strftime("%Y-%m-%d")
STAMP = NOW.strftime("%Y-%m-%d_%H-%M-%S")   # unique per run

# Retry / backoff settings
MAX_RETRIES = 8
BASE_BACKOFF = 1.5
MAX_BACKOFF = 90
PAGE_PAUSE_S = 0.4  # small pause between pages to be gentle
REQUEST_TIMEOUT_S = 60

# -------------------------
# Helpers
# -------------------------
def latest_prior_snapshot(exclude_path: str | None):
    """Return the most recent snapshot file that is NOT the just-written one."""
    files = sorted(glob.glob(f"{SNAPSHOT_DIR}/snapshot_*.csv"))
    for path in reversed(files):
        if exclude_path is None or os.path.abspath(path) != os.path.abspath(exclude_path):
            return path
    return None

def _sleep_with_backoff(attempt: int, retry_after: str | None = None):
    if retry_after and str(retry_after).isdigit():
        time.sleep(min(MAX_BACKOFF, int(retry_after)))
        return
    # exponential backoff with jitter
    delay = min(MAX_BACKOFF, (BASE_BACKOFF ** attempt) + random.random() * 2)
    time.sleep(delay)

def shopify_get(path, params=None, max_retries=MAX_RETRIES):
    """
    GET with retries for:
      - 429 (rate limited): honors Retry-After if present
      - 5xx & network errors: exponential backoff + jitter
    """
    url = f"https://{SHOP_DOMAIN}/admin/api/2024-10{path}"
    headers = {"X-Shopify-Access-Token": ADMIN_TOKEN}

    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.get(url, headers=headers, params=params or {}, timeout=REQUEST_TIMEOUT_S)

            if r.status_code == 429:
                _sleep_with_backoff(attempt, r.headers.get("Retry-After"))
                if attempt >= max_retries:
                    r.raise_for_status()
                continue

            if 500 <= r.status_code < 600:
                if attempt >= max_retries:
                    r.raise_for_status()
                _sleep_with_backoff(attempt)
                continue

            r.raise_for_status()
            return r

        except requests.RequestException:
            if attempt >= max_retries:
                raise
            _sleep_with_backoff(attempt)

def _parse_next_link(link_header: str) -> str | None:
    """
    Extract the next page path (products.json?...page_info=...) from the Link header.
    """
    if not link_header:
        return None
    parts = [p.strip() for p in link_header.split(",")]
    for p in parts:
        if 'rel="next"' in p:
            # format: <https://.../products.json?limit=250&page_info=...>; rel="next"
            start = p.find("<")
            end = p.find(">")
            if start != -1 and end != -1 and end > start:
                full_url = p[start+1:end]
                # keep only path + query to append to base
                idx = full_url.find("/admin/api/")
                if idx != -1:
                    # we already add /admin/api/2024-10; just take the path after that
                    after_api = full_url.split("/admin/api/")[-1]
                    # after_api looks like "2024-10/products.json?...."
                    qpos = after_api.find("/")
                    if qpos != -1:
                        return "/" + after_api[qpos+1:]  # "/products.json?..."
                else:
                    # fallback: if already path-like
                    pos = full_url.find("/products.json?")
                    if pos != -1:
                        return full_url[pos:]
    return None

def fetch_all_products():
    """
    Fetch all products with robust pagination.
    Returns a list of product dicts (id, title, variants, updated_at fields).
    """
    products = []
    next_path = "/products.json"
    params = {"limit": 250, "fields": "id,title,variants,updated_at"}

    safety_pages = 0
    MAX_PAGES = 20000  # very high safety cap

    while True:
        r = shopify_get(next_path, params=params)
        data = r.json().get("products", [])
        if data:
            products.extend(data)

        link = r.headers.get("Link", "")
        next_path = _parse_next_link(link)
        params = None  # after first page we follow the page_info links as-is

        safety_pages += 1
        if not next_path:
            break
        if safety_pages >= MAX_PAGES:
            # avoid infinite loops if Shopify returns a weird Link header loop
            break

        time.sleep(PAGE_PAUSE_S)

    return products

def write_snapshot(products, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id","product_title","product_updated_at","variant_id","variant_title","sku","price"])
        for p in products:
            pid = p["id"]
            ptitle = p.get("title","")
            pupd = p.get("updated_at","")
            for v in p.get("variants", []) or []:
                w.writerow([pid, ptitle, pupd, v.get("id",""), v.get("title",""), v.get("sku",""), v.get("price","")])

def load_snapshot(path):
    d = {}
    if not path or not os.path.exists(path):
        return d
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            d[str(row.get("variant_id",""))] = row
    return d

# ---- Enhanced editor lookup & debugging ----
def _dump_events_debug(product_id, events):
    """Save raw Events API payload for inspection (committed to snapshots/)."""
    try:
        fname = os.path.join(SNAPSHOT_DIR, f"events_{product_id}_{STAMP}.json")
        with open(fname, "w", encoding="utf-8") as fh:
            json.dump(
                {"product_id": product_id,
                 "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
                 "events": events},
                fh, indent=2, ensure_ascii=False
            )
    except Exception:
        try:
            with open(os.path.join(SNAPSHOT_DIR, "events_debug_fail.txt"), "a", encoding="utf-8") as fh:
                fh.write(f"dump failed for {product_id}:\n{traceback.format_exc()}\n")
        except Exception:
            pass

def try_fetch_editor(product_id, since_iso):
    """
    Enhanced Events API lookup with retries.
      - Dumps raw events JSON to snapshots/events_{productid}_{STAMP}.json
      - Tries multiple fields to infer actor name
      - Returns (who, when)
    """
    # retry wrapper around the events call
    attempt = 0
    events = []
    while True:
        attempt += 1
        try:
            r = shopify_get(f"/products/{product_id}/events.json")
            data = r.json()
            events = data.get("events", [])
            break
        except Exception:
            if attempt >= MAX_RETRIES:
                break
            _sleep_with_backoff(attempt)

    # Save raw for inspection even if empty
    _dump_events_debug(product_id, events)

    # Filter to recent
    if since_iso:
        events = [e for e in events if e.get("created_at") and e["created_at"] >= since_iso]

    # newest first
    events.sort(key=lambda e: e.get("created_at",""), reverse=True)

    for e in events:
        # direct author
        if e.get("author"):
            return e["author"], e.get("created_at")

        # nested actor details
        actor = e.get("actor")
        if isinstance(actor, dict):
            who = actor.get("name") or actor.get("email") or actor.get("login")
            if who:
                return who, e.get("created_at")

        # details object sometimes has a user/actor/author
        details = e.get("details")
        if isinstance(details, dict):
            who = details.get("user") or details.get("actor") or details.get("author")
            if who:
                return who, e.get("created_at")

        # message/description heuristics
        message = e.get("message") or ""
        description = e.get("description") or ""
        combined = f"{message} {description}".strip()
        if combined:
            low = combined.lower()
            if " by " in low:
                try:
                    idx = low.index(" by ")
                    snippet = combined[idx+4: idx+4+80]
                    for stop in ["\n", ".", ",", ";", " — ", " - "]:
                        if stop in snippet:
                            snippet = snippet.split(stop)[0]
                    who = snippet.strip()
                    if who:
                        return who, e.get("created_at")
                except Exception:
                    pass

        # fallback to some identifier if present
        admin_id = e.get("admin_graphql_api_id") or e.get("actor_admin_graphql_api_id")
        if admin_id:
            return f"admin_id:{admin_id}", e.get("created_at")

    return "unknown", None

def post_slack(lines, note=None):
    header = f"Daily Price Changes — {TODAY_STR}"
    body = "\n".join(lines) if lines else "No price changes vs previous snapshot."
    if note:
        body = f"{body}\n{note}"
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": f"{header}\n{body}"}, timeout=REQUEST_TIMEOUT_S)
    except Exception:
        # last resort: write report to disk so it's not lost
        try:
            with open(os.path.join(SNAPSHOT_DIR, f"report_{STAMP}.txt"), "w", encoding="utf-8") as f:
                f.write(f"{header}\n{body}\n")
        except Exception:
            pass

# -------------------------
# Main
# -------------------------
def main():
    try:
        # Write this run's snapshot with a timestamped name (prevents merge conflicts)
        snap_this = os.path.join(SNAPSHOT_DIR, f"snapshot_{STAMP}.csv")

        # Extra resilience: try fetching products (with outer retries)
        products = None
        for i in range(1, 5):  # up to 4 outer attempts
            try:
                products = fetch_all_products()
                break
            except Exception:
                if i == 4:
                    raise
                _sleep_with_backoff(i)

        write_snapshot(products or [], snap_this)

        # Compare against most recent prior snapshot
        snap_prev = latest_prior_snapshot(snap_this)
        prev = load_snapshot(snap_prev)
        curr = load_snapshot(snap_this)

        if not prev:
            post_slack([], note="Baseline snapshot saved. Future runs will report changes.")
            return

        # since_iso = date portion of prior snapshot (first 10 chars after 'snapshot_')
        prev_base = os.path.basename(snap_prev)
        prev_date = prev_base[len("snapshot_"):len("snapshot_")+10] if len(prev_base) >= len("snapshot_")+10 else TODAY_STR
        since_iso = f"{prev_date}T00:00:00Z"

        changes = []
        # Iterate current variants and compare to prior prices
        for vid, now in curr.items():
            if vid in prev:
                old_price = str(prev[vid].get("price","")).strip()
                new_price = str(now.get("price","")).strip()
                if old_price != new_price:
                    product_id = now.get("product_id")
                    who, when = try_fetch_editor(product_id, since_iso)
                    changes.append(
                        f"• {now.get('product_title','')} / {now.get('variant_title','')} — {old_price} → {new_price} — by: {who} — at: {when or TODAY_STR}"
                    )

        post_slack(changes)

    except Exception:
        # Never hard-crash silently: write an error file and post a short Slack note
        err_file = os.path.join(SNAPSHOT_DIR, f"run_error_{STAMP}.txt")
        try:
            with open(err_file, "w", encoding="utf-8") as fh:
                fh.write(traceback.format_exc())
        except Exception:
            pass
        post_slack([], note=f"Run encountered an error. Details saved to {os.path.basename(err_file)}")

if __name__ == "__main__":
    main()

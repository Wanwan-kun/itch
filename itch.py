#!/usr/bin/env python3
"""
itch_fetch.py

Fetch itch.io public data.json for a list of appids and save CSV/JSON with fields:
  app_id, id, title, original_price, price, tags

Tags are saved as a list in JSON, and as a pipe-separated string in CSV.

Also prints Price statistics, Top N games by price and Top M tags of all games
(with header "Rank  Tag  Frequency").

Usage example:
  python itch_fetch.py --appids appids.txt --out-data out.csv --format csv --top 10 --tags-top 10
"""
import argparse
import asyncio
import aiohttp
import csv
import json
import math
import random
import re
import time
from collections import Counter
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List, Optional

URL_TEMPLATE = "https://{appid}/data.json"


# ---------------- Adaptive token bucket ----------------
class AdaptiveTokenBucket:
    def __init__(self, rate: float = 1.0, min_rate: float = 0.1, max_rate: float = 5.0,
                 increase: float = 0.05, decrease_factor: float = 0.5):
        self._rate = float(rate)
        self.min_rate = float(min_rate)
        self.max_rate = float(max_rate)
        self.increase = float(increase)
        self.decrease_factor = float(decrease_factor)
        self._tokens = 0.0
        self._last = time.monotonic()
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0
        self._burst_factor = 2.0

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed <= 0:
            return
        add = elapsed * self._rate
        burst = max(1.0, self._rate * self._burst_factor)
        self._tokens = min(burst, self._tokens + add)
        self._last = now

    async def consume(self):
        while True:
            async with self._lock:
                self._refill()
                now = time.monotonic()
                if self._cooldown_until > now:
                    wait = self._cooldown_until - now
                    await asyncio.sleep(wait + random.uniform(0, 0.2))
                    continue
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                needed = 1.0 - self._tokens
                wait_time = 1.0 if self._rate <= 0 else needed / self._rate
            await asyncio.sleep(wait_time + random.uniform(0, 0.02))

    def on_success(self):
        async def _inc():
            async with self._lock:
                self._rate = min(self.max_rate, self._rate + self.increase)
        try:
            asyncio.get_event_loop().create_task(_inc())
        except Exception:
            self._rate = min(self.max_rate, self._rate + self.increase)

    def on_429(self, retry_after: Optional[float] = None):
        async def _do():
            async with self._lock:
                new_rate = max(self.min_rate, self._rate * self.decrease_factor)
                self._rate = new_rate
                self._tokens = 0.0
                now = time.monotonic()
                if retry_after and retry_after > 0:
                    cooldown = float(retry_after)
                else:
                    cooldown = max(1.0, 1.0 / max(0.1, self._rate))
                self._cooldown_until = now + cooldown
        try:
            asyncio.get_event_loop().create_task(_do())
        except Exception:
            new_rate = max(self.min_rate, self._rate * self.decrease_factor)
            self._rate = new_rate
            self._tokens = 0.0
            now = time.monotonic()
            cooldown = retry_after if retry_after else max(1.0, 1.0 / max(0.1, self._rate))
            self._cooldown_until = now + cooldown

    def get_rate(self):
        return self._rate


# ---------------- helpers ----------------
def normalize_appid_line(line: str) -> Optional[str]:
    if not line:
        return None
    s = line.strip()
    if not s:
        return None
    s = re.sub(r"^https?://", "", s)
    s = s.rstrip("/")
    return s


def parse_price_to_float(s: Any) -> Optional[float]:
    """
    Convert strings like "$3.99", "€1,50", "3.00", 3 -> 3.0
    Returns None if not parseable.
    """
    if s is None:
        return None
    if isinstance(s, (int, float)) and not (isinstance(s, float) and math.isnan(s)):
        try:
            return float(s)
        except Exception:
            return None
    text = str(s).strip()
    if text == "" or text.lower() in ("n/a", "-", "none", "null"):
        return None
    neg = False
    if text.startswith("(") and text.endswith(")"):
        neg = True
        text = text[1:-1].strip()
    cleaned = re.sub(r"[^\d\.,\-]", "", text)
    if cleaned == "":
        return None
    # if both . and , present, assume comma is thousands separator
    if "." in cleaned and "," in cleaned:
        cleaned = cleaned.replace(",", "")
    elif "," in cleaned and "." not in cleaned:
        # single comma like "1,50" -> decimal comma
        if cleaned.count(",") == 1 and re.match(r"^\-?\d+,\d{1,3}$", cleaned):
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    try:
        val = float(cleaned)
        if neg:
            val = -val
        return val
    except Exception:
        return None


def fmt_currency(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        return f"${v:,.2f}"
    except Exception:
        return str(v)


def normalize_tags_field(raw: Any) -> List[str]:
    """
    Normalize tags field into a list of strings.
    Handles: list of strings, stringified JSON array, comma-separated string.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        out = []
        for t in raw:
            if t is None:
                continue
            if isinstance(t, str):
                out.append(t.strip())
            else:
                out.append(str(t))
        return [x for x in out if x != ""]
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("[") or s.startswith("{"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return normalize_tags_field(parsed)
            except Exception:
                pass
        parts = re.split(r"[,\|;]+", s)
        return [p.strip() for p in parts if p.strip()]
    try:
        return [str(raw)]
    except Exception:
        return []


# ---------------- fetch ----------------
async def fetch_one(session: aiohttp.ClientSession, appid: str, token_bucket: AdaptiveTokenBucket,
                    timeout: int = 20, max_attempts: int = 5, base_backoff: float = 0.8) -> Dict[str, Any]:
    url = URL_TEMPLATE.format(appid=appid)
    attempt = 0
    last_exc = None

    while attempt < max_attempts:
        attempt += 1
        headers = {"Accept": "application/json", "User-Agent": "itch-fetch/1.0"}

        await token_bucket.consume()

        try:
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                status = resp.status
                text = await resp.text()
                if 200 <= status < 300:
                    token_bucket.on_success()
                if status == 404:
                    return {"app_id": appid, "error": "not_found", "status": 404}
                if status == 401:
                    last_exc = f"401 unauthorized (attempt {attempt})"
                    await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random() * 0.3)
                    continue
                if status == 429:
                    ra = resp.headers.get("Retry-After")
                    retry_after = None
                    if ra:
                        try:
                            retry_after = float(ra)
                        except Exception:
                            retry_after = None
                    token_bucket.on_429(retry_after)
                    wait = retry_after if retry_after and retry_after > 0 else (base_backoff * (2 ** (attempt - 1)) + random.random())
                    await asyncio.sleep(wait)
                    continue
                if status >= 500:
                    await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
                    continue

                try:
                    data = json.loads(text) if text else {}
                except Exception:
                    return {"app_id": appid, "error": "invalid_json", "raw_text": text, "status": status}

                game_obj = data.get("data") if isinstance(data, dict) and "data" in data else data
                if not isinstance(game_obj, dict):
                    return {"app_id": appid, "error": "unexpected_json_structure", "status": status, "raw": game_obj}

                out: Dict[str, Any] = {"app_id": appid}
                out["id"] = game_obj.get("id")
                out["title"] = game_obj.get("title")
                out["original_price"] = game_obj.get("original_price")
                out["price"] = game_obj.get("price")
                out["tags"] = normalize_tags_field(game_obj.get("tags"))
                return out

        except asyncio.TimeoutError:
            last_exc = "timeout"
            await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
            continue
        except aiohttp.ClientError as e:
            last_exc = str(e)
            await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
            continue
        except Exception as e:
            last_exc = str(e)
            await asyncio.sleep(base_backoff * (2 ** (attempt - 1)) + random.random())
            continue

    return {"app_id": appid, "error": "failed", "reason": last_exc}


async def fetch_all(appids: List[str], concurrency: int, timeout: int, delay: float, token_bucket: AdaptiveTokenBucket) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    semaphore = asyncio.Semaphore(concurrency)
    conn = aiohttp.TCPConnector(limit=concurrency * 2, ttl_dns_cache=300)
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(connector=conn, timeout=client_timeout) as session:
        tasks = []
        total = len(appids)
        for idx, aid in enumerate(appids, start=1):
            async def bounded_fetch(aid_local=aid, idx_local=idx):
                async with semaphore:
                    if delay and delay > 0:
                        await asyncio.sleep(delay)
                    print(f"[{idx_local}/{total}] fetching {aid_local} ...", flush=True)
                    r = await fetch_one(session, aid_local, token_bucket, timeout=timeout)
                    status = "ok" if "error" not in r else f"err:{r.get('error')}"
                    print(f"[{idx_local}/{total}] {aid_local} -> {status}", flush=True)
                    results.append(r)
            tasks.append(asyncio.create_task(bounded_fetch()))
        await asyncio.gather(*tasks)
    order = {aid: i for i, aid in enumerate(appids)}
    results_sorted = sorted(results, key=lambda r: order.get(str(r.get("app_id")), 999999))
    return results_sorted


# ---------------- IO and stats ----------------
def save_data(results: List[Dict[str, Any]], outpath: str, fmt: str = "csv"):
    p = Path(outpath)
    fmt = fmt.lower()
    records = []
    for r in results:
        rec = {
            "app_id": r.get("app_id"),
            "id": r.get("id"),
            "title": r.get("title"),
            "original_price": r.get("original_price"),
            "price": r.get("price"),
            "tags": r.get("tags", []),
        }
        records.append(rec)

    if fmt == "json" or p.suffix.lower() == ".json":
        with p.open("w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        print(f"Saved JSON to {p}")
        return

    fieldnames = ["app_id", "id", "title", "original_price", "price", "tags"]
    with p.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            tags_val = r.get("tags") or []
            tags_s = "|".join(tags_val) if isinstance(tags_val, list) else str(tags_val)
            row = {
                "app_id": r.get("app_id", ""),
                "id": r.get("id", ""),
                "title": r.get("title", ""),
                "original_price": r.get("original_price", "") or "",
                "price": r.get("price", "") or "",
                "tags": tags_s
            }
            writer.writerow(row)
    print(f"Saved CSV to {p}")


def compute_price_stats_and_top_and_tags(results: List[Dict[str, Any]], top_n: int = 5, tags_top_n: int = 10) -> Dict[str, Any]:
    total = len(results)
    fetched = sum(1 for r in results if "error" not in r)
    errors = total - fetched

    entries_for_stats = []
    missing_price_count = 0
    tag_counter = Counter()

    for r in results:
        if "error" in r:
            continue
        orig = r.get("original_price")
        curr = r.get("price")
        # prefer original_price, fallback to price
        val = parse_price_to_float(orig)
        if val is None:
            val = parse_price_to_float(curr)
        if val is None:
            missing_price_count += 1
        else:
            entries_for_stats.append({"app_id": r.get("app_id"), "title": r.get("title") or "", "price_val": val})
        tags = r.get("tags", [])
        if isinstance(tags, list):
            for t in tags:
                if t and isinstance(t, str):
                    tag_counter[t] += 1
        elif isinstance(tags, str):
            for t in re.split(r"[,\|;]+", tags):
                t = t.strip()
                if t:
                    tag_counter[t] += 1

    prices = [e["price_val"] for e in entries_for_stats]

    def summarize(arr: List[float]):
        if not arr:
            return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
        return {"count": len(arr), "min": float(min(arr)), "max": float(max(arr)),
                "mean": float(mean(arr)), "median": float(median(arr))}

    ps = summarize(prices)

    entries_sorted = sorted(entries_for_stats, key=lambda x: x["price_val"], reverse=True)
    top_entries = entries_sorted[:top_n]

    top_tags = tag_counter.most_common(tags_top_n)

    return {
        "total": total,
        "fetched": fetched,
        "errors": errors,
        "missing_price_count": missing_price_count,
        "price_summary": ps,
        "top_entries": top_entries,
        "top_tags": top_tags
    }


def print_pretty_stats(stats: Dict[str, Any], top_n: int = 5, tags_top_n: int = 10):
    print()
    print(f"Total appids     : {stats['total']}")
    print(f"Successfully got : {stats['fetched']}")
    print(f"Errors / missing : {stats['errors']}")
    print(f"Free games       : {stats['missing_price_count']}")
    print()
    print("Price statistics:")
    p = stats["price_summary"]
    print(f"  Count numeric: {p['count']}")
    if p["count"] > 0:
        print(f"  Average: {fmt_currency(p['mean'])}")
        print(f"  Median : {fmt_currency(p['median'])}")
        print(f"  Min    : {fmt_currency(p['min'])}")
        print(f"  Max    : {fmt_currency(p['max'])}")
    else:
        print("  (no numeric price values found)")
    print()
    print(f"Top {top_n} games by price:")
    top = stats["top_entries"]
    if not top:
        print("  (no entries)")
    else:
        rank_w = max(4, len(str(len(top))))
        name_w = min(40, max(10, max((len(t['title']) for t in top), default=10)))
        price_w = max(7, max((len(fmt_currency(t['price_val'])) for t in top), default=7))
        header = f"  {'Rank':>{rank_w}}  {'Name':<{name_w}}  {'Price':>{price_w}}  Link"
        print(header)
        for i, t in enumerate(top, start=1):
            name = t["title"] or ""
            if len(name) > name_w:
                name = name[:name_w-3] + "..."
            price_s = fmt_currency(t["price_val"])
            link = f"https://{t['app_id']}"
            print(f"  {i:>{rank_w}}  {name:<{name_w}}  {price_s:>{price_w}}  {link}")
    print()
    # Top tags with header
    print(f"Top {tags_top_n} tags of all games:")
    top_tags = stats.get("top_tags", [])
    if not top_tags:
        print("  (no tags found)")
    else:
        print(f"  {'Rank':>4}  {'Tag':<25} {'Frequency':>9}")
        for i, (tag, cnt) in enumerate(top_tags, start=1):
            print(f"  {i:>4}  {tag:<25} {cnt:>9}")


# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser(description="Fetch itch.io /data.json for appids and save CSV/JSON")
    p.add_argument("--appids", "-a", required=True, help="file with appids (one per line) e.g. author.itch.io/slug")
    p.add_argument("--out-data", "-o", default="itch_out.csv", help="output file (csv or json)")
    p.add_argument("--format", choices=("csv", "json"), default="csv", help="output format")
    p.add_argument("--concurrency", "-c", type=int, default=6, help="number of concurrent requests")
    p.add_argument("--timeout", type=int, default=30, help="per-request timeout (seconds)")
    p.add_argument("--delay", type=float, default=0.0, help="delay (seconds) before each request")
    p.add_argument("--rps", type=float, default=1.0, help="initial requests-per-second")
    p.add_argument("--rps-min", type=float, default=0.1, help="minimum requests-per-second")
    p.add_argument("--rps-max", type=float, default=5.0, help="maximum requests-per-second")
    p.add_argument("--rps-increase", type=float, default=0.05, help="additive increase on success")
    p.add_argument("--rps-decrease-factor", type=float, default=0.5, help="multiplicative decrease factor on 429")
    p.add_argument("--out-stats", default="itch_stats.txt", help="file for textual stats summary")
    p.add_argument("--fetch-only", action="store_true", help="only fetch and save data, do not print stats")
    p.add_argument("--top", type=int, default=10, help="Top N games to show in summary")
    p.add_argument("--tags-top", type=int, default=10, help="Top M tags to show in summary")
    return p.parse_args()


async def main_async(args):
    p = Path(args.appids)
    if not p.exists():
        print("appids file not found:", p)
        return 2
    raw_lines = []
    with p.open("r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s:
                continue
            normalized = normalize_appid_line(s)
            if normalized:
                raw_lines.append(normalized)

    if not raw_lines:
        print("No appids found in file.")
        return 2

    token_bucket = AdaptiveTokenBucket(rate=args.rps, min_rate=args.rps_min, max_rate=args.rps_max,
                                       increase=args.rps_increase, decrease_factor=args.rps_decrease_factor)

    print(f"Fetching {len(raw_lines)} appids with concurrency={args.concurrency}, delay={args.delay}s, initial rps={token_bucket.get_rate():.2f} ...")
    results = await fetch_all(raw_lines, concurrency=args.concurrency, timeout=args.timeout, delay=args.delay, token_bucket=token_bucket)

    # Save only requested fields (no internal debug fields)
    normed = []
    for r in results:
        rec = {
            "app_id": r.get("app_id"),
            "id": r.get("id"),
            "title": r.get("title"),
            "original_price": r.get("original_price"),
            "price": r.get("price"),
            "tags": r.get("tags", []),
        }
        normed.append(rec)

    save_data(normed, args.out_data, args.format)

    stats = compute_price_stats_and_top_and_tags(results, top_n=args.top, tags_top_n=args.tags_top)

    # write textual stats file
    stats_lines = []
    stats_lines.append(f"Total appids     : {stats['total']}")
    stats_lines.append(f"Successfully got : {stats['fetched']}")
    stats_lines.append(f"Errors / missing : {stats['errors']}")
    stats_lines.append(f"Free games       : {stats['missing_price_count']}")
    stats_lines.append("")
    p_summary = stats["price_summary"]
    if p_summary["count"] == 0:
        stats_lines.append("Price statistics:")
        stats_lines.append("  (no numeric price values found)")
    else:
        stats_lines.append("Price statistics:")
        stats_lines.append(f"  Count numeric: {p_summary['count']}")
        stats_lines.append(f"  Average: {fmt_currency(p_summary['mean'])}")
        stats_lines.append(f"  Median : {fmt_currency(p_summary['median'])}")
        stats_lines.append(f"  Min    : {fmt_currency(p_summary['min'])}")
        stats_lines.append(f"  Max    : {fmt_currency(p_summary['max'])}")
    stats_lines.append("")
    stats_lines.append(f"Top {args.top} games by price:")
    top_entries = stats["top_entries"]
    if not top_entries:
        stats_lines.append("  (no entries)")
    else:
        for i, e in enumerate(top_entries, start=1):
            title = (e['title'] or "")[:60]
            stats_lines.append(f"  {i:>3}. {title:60} {fmt_currency(e['price_val']):>8}  https://{e['app_id']}")
    stats_lines.append("")
    stats_lines.append(f"Top {args.tags_top} tags of all games:")
    # header for tags
    stats_lines.append(f"  {'Rank':>4}  {'Tag':<25} {'Frequency':>9}")
    for i, (tag, cnt) in enumerate(stats.get("top_tags", []), start=1):
        stats_lines.append(f"  {i:>4}  {tag:<25} {cnt:>9}")
    stats_text = "\n".join(stats_lines)
    Path(args.out_stats).write_text(stats_text, encoding="utf-8")

    if not args.fetch_only:
        print_pretty_stats(stats, top_n=args.top, tags_top_n=args.tags_top)
        print()
        print(f"Saved textual stats to {args.out_stats}")

    return 0


def main():
    args = parse_args()
    try:
        rc = asyncio.run(main_async(args))
        raise SystemExit(rc or 0)
    except KeyboardInterrupt:
        print("Interrupted by user")
        raise SystemExit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""The logging backend's exit test (docs/backend.md, "Exit test").

Runs against a running deployment: the sidecar (blogs ingest) publishing a
stream, three archives (blogs archive -follow) each following its own host,
a stock Loki, and the read gateway (blogs gateway) over the first archive.
Standard library only.

  roots    every interval file the three archives hold in common has the
           same root, token and leaves; prints how many were compared
  tamper   pushes an altered copy of a verified line straight into Loki,
           with the original's placement, and checks the gateway marks the
           copy unverified (altered) and the original still verified
  retract  retracts one proven batch with blogs sweep (the sidecar must be
           stopped first: it holds the producer home), waits for every
           archive to keep the receipt, and checks the gateway marks the
           batch's lines retracted and its coverage names the receipt
  soak     repeats roots and a gateway sample every --every for --duration,
           one JSON line per round, and fails if any round failed

Exit 0 when every check passed, 1 otherwise.
"""

import argparse
import glob
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request


def get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def query(base, logql, minutes, limit=1000, categorized=True):
    end = time.time_ns()
    q = urllib.parse.urlencode({"query": logql, "start": end - minutes * 60 * 10**9, "end": end, "limit": limit, "direction": "backward"})
    headers = {"X-Loki-Response-Encoding-Flags": "categorize-labels"} if categorized else {}
    ans = get(base.rstrip("/") + "/loki/api/v1/query_range?" + q, headers)
    out = []
    for s in ans["data"]["result"]:
        for v in s["values"]:
            md = v[2].get("structuredMetadata", {}) if len(v) > 2 else s["stream"]
            if isinstance(md, list):
                md = {p["name"]: p["value"] for p in md}
            out.append({"ts": v[0], "line": v[1], "labels": s["stream"], "meta": md})
    return out


def intervals(archive, stream, producer):
    out = {}
    for n in glob.glob(os.path.join(archive, stream, producer, "intervals", "*.json")):
        with open(n) as f:
            d = json.load(f)
        out[d["interval"]] = (d["root"], d["token"], tuple((l["seq"], l["commitment"]) for l in d["leaves"]))
    return out


def check_roots(a):
    ok, report = True, {}
    for stream in a.streams:
        sets = [intervals(d, stream, a.producer) for d in a.archives]
        common = set.intersection(*(set(s) for s in sets))
        bad = sorted(k for k in common if len({s[k] for s in sets}) != 1)
        report[stream] = {"compared": len(common), "held": [len(s) for s in sets], "disagree": bad,
                          "last": max(common) if common else 0}
        ok = ok and not bad and len(common) > 0
    return ok, report


def sample(a):
    lines = query(a.gateway, a.selector, a.minutes)
    states = {}
    for l in lines:
        k = l["meta"].get("blogs_state", "?") + "/" + l["meta"].get("blogs_reason", "")
        states[k] = states.get(k, 0) + 1
    # Lines older than the settle time must be verified: an interval is
    # anchored, mined and archived by then.
    cutoff = time.time_ns() - a.settle * 10**9
    old_unverified = [l for l in lines if int(l["ts"]) < cutoff and l["meta"].get("blogs_state") != "verified"
                      and l["meta"].get("blogs_reason") not in ("retracted", "altered", "relabeled")]
    return not old_unverified, {"lines": len(lines), "states": states, "settled_unverified": len(old_unverified),
                                "example": old_unverified[0]["meta"] if old_unverified else None}


def cmd_roots(a):
    ok, r = check_roots(a)
    print(json.dumps({"check": "roots", "ok": ok, "streams": r}))
    return ok


def cmd_tamper(a):
    lines = query(a.gateway, a.selector, a.minutes, limit=200)
    target = next((l for l in lines if l["meta"].get("blogs_state") == "verified"), None)
    if target is None:
        print(json.dumps({"check": "tamper", "ok": False, "why": "no verified line to alter"}))
        return False
    altered = target["line"].replace('"body":"', '"body":"ALTERED ', 1)
    md = {k: target["meta"][k] for k in ("blogs_stream", "blogs_producer", "blogs_seq", "blogs_line")}
    labels = {k: v for k, v in target["labels"].items() if not k.startswith("blogs_")}
    body = json.dumps({"streams": [{"stream": labels, "values": [[str(int(target["ts"]) + 1), altered, md]]}]}).encode()
    req = urllib.request.Request(a.loki.rstrip("/") + "/loki/api/v1/push", data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=30).close()
    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(2)
        got = query(a.gateway, a.selector + ' |= "ALTERED "', a.minutes)
        mine = [l for l in got if l["line"] == altered]
        if mine:
            orig = [l for l in query(a.gateway, a.selector, a.minutes, limit=5000) if l["line"] == target["line"]]
            ok = mine[0]["meta"].get("blogs_reason") == "altered" and mine[0]["meta"].get("blogs_state") == "unverified" \
                and all(o["meta"].get("blogs_state") == "verified" for o in orig) and len(orig) > 0
            print(json.dumps({"check": "tamper", "ok": ok, "placement": md, "altered_mark": mine[0]["meta"].get("blogs_state") + "/" + mine[0]["meta"].get("blogs_reason", ""),
                              "original_marks": sorted({o["meta"].get("blogs_state") for o in orig})}))
            return ok
    print(json.dumps({"check": "tamper", "ok": False, "why": "the altered line never came back from the gateway"}))
    return False


def cmd_retract(a):
    stream = a.streams[0]
    ivs = intervals(a.archives[0], stream, a.producer)
    if not ivs:
        print(json.dumps({"check": "retract", "ok": False, "why": "no proven interval"}))
        return False
    leaves = ivs[max(ivs)][2]
    seq = a.seq or leaves[len(leaves) // 2][0]
    out = subprocess.run([a.blogs, "sweep", stream, "-retract-seqs", str(seq), "-yes"], capture_output=True, text=True)
    if out.returncode != 0:
        print(json.dumps({"check": "retract", "ok": False, "seq": seq, "sweep": out.stdout + out.stderr}))
        return False
    deadline = time.time() + a.wait
    receipts = []
    while time.time() < deadline:
        receipts = [glob.glob(os.path.join(d, stream, a.producer, "retracted", "%016d-*.json" % seq)) for d in a.archives]
        if all(receipts):
            break
        time.sleep(10)
    held = [bool(r) for r in receipts]
    sweeps = set()
    for r in receipts:
        for n in r:
            with open(n) as f:
                sweeps.add(json.load(f)["sweep"])
    cov = get(a.gateway.rstrip("/") + "/blogs/v1/coverage")
    in_cov = [rc for c in cov if c["stream"] == stream for rc in c["retracted"] if rc["seq"] == seq]
    marks = sorted({l["meta"].get("blogs_reason", "") for l in query(a.gateway, a.selector, a.minutes, limit=5000)
                    if l["meta"].get("blogs_stream") == stream and l["meta"].get("blogs_seq") == str(seq)})
    ok = all(held) and len(sweeps) == 1 and bool(in_cov) and (marks == ["retracted"] or not marks)
    print(json.dumps({"check": "retract", "ok": ok, "seq": seq, "receipts": held, "sweep": sorted(sweeps),
                      "coverage": in_cov, "gateway_marks": marks, "sweep_output": out.stdout.strip().splitlines()[-3:]}))
    return ok


def cmd_soak(a):
    end = time.time() + a.duration
    failed = 0
    rounds = 0
    while True:
        rounds += 1
        ok1, roots = check_roots(a)
        try:
            ok2, smp = sample(a)
        except Exception as e:  # a gateway that does not answer fails the round
            ok2, smp = False, {"error": str(e)}
        failed += not (ok1 and ok2)
        print(json.dumps({"check": "soak", "round": rounds, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                          "ok": ok1 and ok2, "roots": roots, "gateway": smp}), flush=True)
        if time.time() + a.every > end:
            break
        time.sleep(a.every)
    print(json.dumps({"check": "soak", "rounds": rounds, "failed": failed, "ok": failed == 0}))
    return failed == 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("check", choices=["roots", "tamper", "retract", "soak"])
    p.add_argument("--archive", action="append", required=True, help="an archive directory (three)")
    p.add_argument("--stream", action="append", required=True, help="a stream (the first is retracted from)")
    p.add_argument("--producer", required=True)
    p.add_argument("--gateway", default="http://127.0.0.1:3101")
    p.add_argument("--loki", default="http://127.0.0.1:3100")
    p.add_argument("--blogs", default="blogs", help="the blogs command, configured for the producer home")
    p.add_argument("--selector", default='{service_name=~".+"}')
    p.add_argument("--minutes", type=int, default=30, help="how far back the gateway is asked")
    p.add_argument("--settle", type=int, default=600, help="seconds after which a line must be verified")
    p.add_argument("--seq", type=int, default=0, help="retract: the seq (default: the middle of the last interval)")
    p.add_argument("--wait", type=int, default=900, help="retract: seconds to wait for the receipts")
    p.add_argument("--duration", type=int, default=3600)
    p.add_argument("--every", type=int, default=300)
    a = p.parse_args()
    a.archives, a.streams = a.archive, a.stream
    if len(a.archives) != 3:
        p.error("name three archives")
    ok = {"roots": cmd_roots, "tamper": cmd_tamper, "retract": cmd_retract, "soak": cmd_soak}[a.check](a)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

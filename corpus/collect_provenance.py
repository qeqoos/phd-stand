#!/usr/bin/env python3
"""Збір provenance npm-пакетів (корпус P2, RQ1–RQ2).

Для кожного пакета: остання версія -> атестації реєстру npm -> SLSA provenance
-> репозиторій, коміт, шлях workflow, buildType, builder, runner_environment,
resolvedDependencies. Опційно завантажує workflow-файл на коміті з provenance.

Лише stdlib. Приклади:
  python3 corpus/collect_provenance.py --top 50
  python3 corpus/collect_provenance.py --top 5000 --source top-download --fetch-workflow
  python3 corpus/collect_provenance.py --list my_packages.txt
  python3 corpus/collect_provenance.py --source ecosystems --top 5000 --downloads --fetch-workflow
"""
from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import hashlib
import io
import json
import re
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REGISTRY = "https://registry.npmjs.org"
RAW_GH = "https://raw.githubusercontent.com"
ECOSYSTEMS = "https://packages.ecosyste.ms/api/v1"
NPM_API = "https://api.npmjs.org"
UA = "phd-stand-corpus/0.1 (research; npm provenance study)"
TOPLIST_PKG = "npm-high-impact"
SLSA_TYPES = ("https://slsa.dev/provenance/v1", "https://slsa.dev/provenance/v0.2")

CSV_FIELDS = [
    "rank", "package", "version", "downloads_week", "downloads_eco", "status", "has_publish_att", "has_provenance",
    "predicate_type", "build_type", "builder_id", "runner_environment",
    "event_name", "invocation_id", "repository", "git_commit", "workflow_ref", "workflow_path",
    "resolved_deps_n", "resolved_deps_uris", "rekor_log_index",
    "workflow_fetch", "workflow_sha256", "error",
]


# ---------------------------------------------------------------- HTTP
def http_get(url: str, retries: int = 4, timeout: int = 30) -> bytes | None:
    """GET з повторами; None для 404."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(2 ** attempt + 1)
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if attempt < retries - 1:
                time.sleep(2 ** attempt + 1)
                continue
            raise
    return None


def get_json(url: str):
    b = http_get(url)
    return None if b is None else json.loads(b)


def pkg_url(name: str) -> str:
    # scoped: @scope/name -> @scope%2Fname
    return f"{REGISTRY}/{urllib.parse.quote(name, safe='@')}"


# ---------------------------------------------------------------- рамка вибірки
def load_toplist(source: str) -> tuple[list[str], str]:
    """Список популярних пакетів з npm-high-impact (тарбол із реєстру)."""
    meta = get_json(f"{pkg_url(TOPLIST_PKG)}/latest")
    ver, tarball = meta["version"], meta["dist"]["tarball"]
    data = http_get(tarball)
    fname = {"top-download": "top-download.js", "top-dependent": "top-dependent.js",
             "top": "top.js"}[source]
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        src = tf.extractfile(f"package/lib/{fname}").read().decode()
    names = re.findall(r"'((?:@[^'/]+/)?[^']+)'", src)
    return names, f"{TOPLIST_PKG}@{ver}/{fname}"


def load_ecosystems(n: int, per_page: int = 100) -> tuple[list[str], str, dict]:
    """Топ-N npm за завантаженнями з packages.ecosyste.ms (публічний API, без ключа)."""
    names: list[str] = []
    eco_dl: dict = {}
    page = 1
    while len(names) < n:
        url = (f"{ECOSYSTEMS}/registries/npmjs.org/packages"
               f"?sort=downloads&order=desc&per_page={per_page}&page={page}")
        data = get_json(url)
        items = data if isinstance(data, list) else (data or {}).get("packages", [])
        if not items:
            break
        for it in items:
            nm = it.get("name")
            if nm and nm not in eco_dl:
                names.append(nm)
                eco_dl[nm] = it.get("downloads")
        page += 1
        time.sleep(0.5)  # ввічливо до API
    names = names[:n]
    return names, f"ecosyste.ms packages?sort=downloads (pages 1..{page - 1})", eco_dl


def load_downloads(names: list[str], period: str = "last-week") -> dict:
    """Завантаження з api.npmjs.org: bulk (≤128) для unscoped, поштучно для scoped."""
    out: dict = {}
    unscoped = [n for n in names if not n.startswith("@")]
    for i in range(0, len(unscoped), 128):
        chunk = unscoped[i:i + 128]
        try:
            d = get_json(f"{NPM_API}/downloads/point/{period}/{','.join(chunk)}") or {}
        except Exception:  # noqa: BLE001
            d = {}
        if len(chunk) == 1 and "downloads" in d:
            d = {chunk[0]: d}
        for k, v in d.items():
            if isinstance(v, dict):
                out[k] = v.get("downloads")
    for n in (n for n in names if n.startswith("@")):
        try:
            d = get_json(f"{NPM_API}/downloads/point/{period}/{urllib.parse.quote(n, safe='@')}") or {}
            out[n] = d.get("downloads")
        except Exception:  # noqa: BLE001
            out[n] = None
    return out


# ---------------------------------------------------------------- provenance
def decode_statement(att: dict) -> dict:
    env = att["bundle"]["dsseEnvelope"]
    return json.loads(base64.b64decode(env["payload"]))


def parse_provenance(stmt: dict, pred_type: str) -> dict:
    p = stmt.get("predicate", {})
    out: dict = {"predicate_type": pred_type}
    if pred_type.endswith("/v1"):
        bd = p.get("buildDefinition", {})
        rd = p.get("runDetails", {})
        wf = bd.get("externalParameters", {}).get("workflow", {})
        gh = bd.get("internalParameters", {}).get("github", {})
        deps = bd.get("resolvedDependencies", []) or []
        builder_id = rd.get("builder", {}).get("id", "")
        # actions.github.io buildType: internalParameters.github.runner_environment;
        # slsa-framework buildType: лише в builder.id (.../runner/github-hosted)
        runner_env = gh.get("runner_environment") or next(
            (k for k in ("github-hosted", "self-hosted") if builder_id.endswith(k)), "")
        out.update(
            build_type=bd.get("buildType", ""),
            builder_id=builder_id,
            runner_environment=runner_env,
            event_name=gh.get("event_name", ""),
            invocation_id=rd.get("metadata", {}).get("invocationId", ""),
            repository=wf.get("repository", ""),
            workflow_ref=wf.get("ref", ""),
            workflow_path=wf.get("path", ""),
            resolved_deps_n=len(deps),
            resolved_deps_uris=" ".join(d.get("uri", "") for d in deps),
            git_commit=next((d.get("digest", {}).get("gitCommit", "") for d in deps
                             if "gitCommit" in d.get("digest", {})), ""),
        )
    else:  # v0.2
        inv = p.get("invocation", {})
        cs = inv.get("configSource", {})
        env = inv.get("environment", {})
        mats = p.get("materials", []) or []
        uri = cs.get("uri", "")
        repo, _, ref = uri.partition("@")
        out.update(
            build_type=p.get("buildType", ""),
            builder_id=p.get("builder", {}).get("id", ""),
            runner_environment=env.get("GITHUB_RUNNER_ENVIRONMENT", ""),
            event_name=env.get("GITHUB_EVENT_NAME", ""),
            invocation_id="",
            repository=repo.removeprefix("git+"),
            workflow_ref=ref,
            workflow_path=cs.get("entryPoint", ""),
            resolved_deps_n=len(mats),
            resolved_deps_uris=" ".join(m.get("uri", "") for m in mats),
            git_commit=cs.get("digest", {}).get("sha1", ""),
        )
    return out


def gh_owner_repo(repo_url: str) -> str | None:
    m = re.match(r"(?:git\+)?https://github\.com/([^/]+/[^/@#]+?)(?:\.git)?/?$", repo_url)
    return m.group(1) if m else None


def fetch_workflow(row: dict, wf_dir: Path) -> None:
    owner_repo = gh_owner_repo(row.get("repository", ""))
    commit, path = row.get("git_commit"), row.get("workflow_path")
    if not (owner_repo and commit and path):
        row["workflow_fetch"] = "skip"
        return
    try:
        b = http_get(f"{RAW_GH}/{owner_repo}/{commit}/{path}")
    except Exception as e:  # noqa: BLE001
        row["workflow_fetch"] = f"error:{type(e).__name__}"
        return
    if b is None:
        row["workflow_fetch"] = "404"
        return
    safe = row["package"].replace("/", "__")
    (wf_dir / f"{safe}@{row['version']}.yml").write_bytes(b)
    row["workflow_fetch"] = "ok"
    row["workflow_sha256"] = hashlib.sha256(b).hexdigest()


def process(name: str, raw_dir: Path, wf_dir: Path | None) -> dict:
    row: dict = {"package": name, "has_publish_att": False, "has_provenance": False}
    try:
        man = get_json(f"{pkg_url(name)}/latest")
        if man is None:
            row["status"] = "not_found"
            return row
        row["version"] = man["version"]
        att_meta = man.get("dist", {}).get("attestations")
        if not att_meta:
            row["status"] = "no_attestations"
            return row
        url = att_meta.get("url") or f"{REGISTRY}/-/npm/v1/attestations/{name}@{man['version']}"
        data = get_json(url)
        if not data:
            row["status"] = "attestations_404"
            return row
        safe = name.replace("/", "__")
        (raw_dir / f"{safe}@{man['version']}.json").write_text(json.dumps(data))
        for att in data.get("attestations", []):
            pt = att.get("predicateType", "")
            if "npm/attestation" in pt and "publish" in pt:
                row["has_publish_att"] = True
            elif pt in SLSA_TYPES:
                row["has_provenance"] = True
                row.update(parse_provenance(decode_statement(att), pt))
                tl = att["bundle"].get("verificationMaterial", {}).get("tlogEntries", [])
                row["rekor_log_index"] = tl[0].get("logIndex", "") if tl else ""
        row["status"] = "provenance" if row["has_provenance"] else "attestations_no_slsa"
        if wf_dir and row["has_provenance"]:
            fetch_workflow(row, wf_dir)
    except Exception as e:  # noqa: BLE001
        row["status"] = "error"
        row["error"] = f"{type(e).__name__}: {e}"[:300]
    return row


# ---------------------------------------------------------------- зведення
def summarize(rows: list[dict], meta: dict) -> dict:
    prov = [r for r in rows if r.get("has_provenance")]
    n = len(rows)
    return {
        **meta,
        "n_packages": n,
        "status": dict(Counter(r["status"] for r in rows)),
        "n_provenance": len(prov),
        "share_provenance": round(len(prov) / n, 4) if n else 0,
        "predicate_type": dict(Counter(r["predicate_type"] for r in prov)),
        "build_type": dict(Counter(r["build_type"] for r in prov)),
        "runner_environment": dict(Counter(r["runner_environment"] or "?" for r in prov)),
        "event_name": dict(Counter(r["event_name"] or "?" for r in prov)),
        "builder_id": dict(Counter(r["builder_id"] for r in prov).most_common(10)),
        "resolved_deps_n": dict(Counter(r["resolved_deps_n"] for r in prov)),
        "workflow_path_top": dict(Counter(r["workflow_path"] for r in prov).most_common(15)),
        "workflow_fetch": dict(Counter(r.get("workflow_fetch", "-") for r in prov)),
        "non_github_repo": sum(1 for r in prov if not gh_owner_repo(r.get("repository", ""))),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--top", type=int, default=50, help="перші N пакетів зі списку (0 = усі)")
    ap.add_argument("--source", default="top-download", choices=["top-download", "top-dependent", "top", "ecosystems"],
                    help="рамка: npm-high-impact (top-download|top-dependent|top) або ecosyste.ms")
    ap.add_argument("--list", type=Path, help="свій список пакетів (по одному в рядку) замість npm-high-impact")
    ap.add_argument("--out", type=Path, help="каталог результатів (типово results/corpus-<дата>-<джерело>-n<N>)")
    ap.add_argument("--fetch-workflow", action="store_true", help="завантажити workflow на коміті з provenance")
    ap.add_argument("--downloads", action="store_true", help="додати тижневі завантаження з api.npmjs.org")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()

    eco_dl: dict = {}
    if a.list:
        names = [l.strip() for l in a.list.read_text().splitlines() if l.strip() and not l.startswith("#")]
        frame = str(a.list)
    elif a.source == "ecosystems":
        names, frame, eco_dl = load_ecosystems(a.top or 5000)
    else:
        names, frame = load_toplist(a.source)
    if a.top:
        names = names[: a.top]
    print(f"frame: {frame}, n={len(names)}", file=sys.stderr)

    started = dt.datetime.now(dt.timezone.utc)
    src = "list" if a.list else a.source
    out = a.out or Path("results") / f"corpus-{started:%Y%m%d}-{src}-n{len(names)}"
    raw_dir, wf_dir = out / "attestations", out / "workflows"
    raw_dir.mkdir(parents=True, exist_ok=True)
    if a.fetch_workflow:
        wf_dir.mkdir(exist_ok=True)
    (out / "frame.txt").write_text("\n".join(names) + "\n")  # рамка для відтворюваності
    week_dl = load_downloads(names) if a.downloads else {}

    rows: list[dict] = []
    with ThreadPoolExecutor(a.workers) as ex:
        futs = {ex.submit(process, n, raw_dir, wf_dir if a.fetch_workflow else None): n for n in names}
        for i, f in enumerate(as_completed(futs), 1):
            rows.append(f.result())
            if i % 50 == 0 or i == len(names):
                print(f"[{i}/{len(names)}]", file=sys.stderr)
    order = {n: i for i, n in enumerate(names)}
    rows.sort(key=lambda r: order[r["package"]])
    for r in rows:
        r["rank"] = order[r["package"]] + 1
        r["downloads_week"] = week_dl.get(r["package"], "")
        r["downloads_eco"] = eco_dl.get(r["package"], "")

    with open(out / "packages.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    with open(out / "packages.jsonl", "w") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    meta = {"collected_at": started.isoformat(timespec="seconds"), "frame": frame,
            "top": a.top, "fetch_workflow": a.fetch_workflow, "downloads": a.downloads}
    summary = summarize(rows, meta)
    (out / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"-> {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

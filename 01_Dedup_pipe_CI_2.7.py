# ============================================================
# 01_Dedup_pipe_CI_2.7.py
# ============================================================
# 실행 흐름:
#   1) ROOT / BASE 경로 입력 및 확인
#   2) 모드 선택:
#        1 = DUP 모드     (중복 그룹 TOP N)
#        2 = BIGFILE 모드 (대용량 중복 그룹 TOP N)
#   3) 생성 수량 N 입력 (TOP N 그룹 개수)
#   4) BASE\Runs\run_YYYYMMDD_HHMM 생성  ← 초 단위 제거
#        - DUP    : 01_review_dup/ 이하 그룹 폴더 + .lnk
#        - BIGFILE: 01_review_big/ 이하 그룹 폴더 + .lnk
#   5) run_meta.txt 기록 (ROOT/BASE/RUN_ID/MODE/TOP_N 등)
#   6) 02 실행용 cmd / 텍스트 생성
#
# 모드별 동작:
#   [DUP]
#     1) 사이즈 → blake3 → SHA256 중복 후보 탐지
#     2) 그룹 리포트 생성
#     3) COUNT>=3 필터
#     4) wasted_bytes 기준 TOP N 그룹 선택
#     5) review 링크 생성
#
#   [BIGFILE]
#     1) min_size_mb 이상(+ 확장자 필터) 파일만 후보
#     2) size → blake3 → SHA256으로 중복 그룹(>=2개) 탐지
#     3) wasted_bytes 기준 TOP N 그룹 선택
#     4) review 링크 생성
#
#   삭제/이동 없음. 리뷰/후보만 생성.
# ============================================================

import os
import csv
import re
import time
import hashlib
import subprocess
from pathlib import Path
from collections import defaultdict
from datetime import datetime

try:
    from blake3 import blake3
except Exception as e:
    raise SystemExit(
        "blake3 모듈이 필요함. 아래로 설치 후 재실행:\n"
        r"  C:\elice\venv\Scripts\python.exe -m pip install blake3"
    ) from e

# 기본 경로
DEFAULT_ROOT = r"C:\Users\Meclaser\Desktop\Mec_DB"
DEFAULT_BASE = r"C:\elice\dedup"

CHUNK = 1024 * 1024

# DUP 모드용
MIN_COUNT = 3

# BIGFILE 모드 기본값들
BIG_MIN_SIZE_MB = 200
BIG_EXT_WHITELIST = [
    ".mp4", ".mov", ".mkv", ".avi",
    ".wmv", ".flv", ".mpg", ".mpeg",
]
# BIGFILE 모드에서 중복으로 인정할 최소 개수
BIG_MIN_DUP_COUNT = 2

PRINT_EVERY_FILES = 5000
PRINT_EVERY_HASH = 1000

WIN_PATH_RE = re.compile(r"""[A-Za-z]:\\[^\r\n\|\;\,"]+""")

# 02 실행 스크립트 이름 (나중에 버전 바꾸면 여기만 수정)
NEXT_02_SCRIPT = "02_Full_pipe_CI_2.7.py"


def stamp(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def prompt_dir(prompt: str, default: str, must_exist: bool, create_if_missing: bool) -> Path:
    while True:
        s = input(f"{prompt}\n(default: {default})\n> ").strip()
        if not s:
            s = default
        p = Path(s)

        if must_exist:
            if not p.exists():
                print(f"[ERR] 경로가 존재하지 않음: {p}")
                continue
            if not p.is_dir():
                print(f"[ERR] 폴더가 아님: {p}")
                continue
            return p

        if (not p.exists()) and create_if_missing:
            try:
                p.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"[ERR] 폴더 생성 실패: {p} ({e})")
                continue
        return p


# ---------------- 공통 해시 함수 ----------------

def fast_hash(path: str) -> str:
    h = blake3()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(CHUNK), b""):
            h.update(c)
    return h.hexdigest()


def sha256_hex(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(CHUNK), b""):
            h.update(c)
    return h.hexdigest()


# ---------------- DUP 모드: 중복 탐지 ----------------

def step1_scan_duplicates(ROOT: Path, CSV_DUP: Path):
    stamp("STEP 1/5: 파일 크기 수집 시작")
    t0 = time.time()

    size_groups = defaultdict(list)
    scanned = 0
    failed = 0

    for root, _, files in os.walk(str(ROOT)):
        for name in files:
            p = os.path.join(root, name)
            scanned += 1
            try:
                size_groups[os.path.getsize(p)].append(p)
            except Exception:
                failed += 1

            if scanned % PRINT_EVERY_FILES == 0:
                stamp(f"  scanned={scanned:,} failed={failed:,} size_buckets={len(size_groups):,}")

    candidates = {k: v for k, v in size_groups.items() if len(v) > 1}
    cand_files = sum(len(v) for v in candidates.values())
    stamp(
        "STEP 1/5: 크기 후보 추출 완료 "
        f"buckets={len(candidates):,} files={cand_files:,} "
        f"(elapsed={time.time()-t0:.1f}s)"
    )

    stamp("STEP 1/5: blake3 해시 계산 시작")
    t1 = time.time()

    fast_groups = defaultdict(list)
    hashed_fast = 0

    for group in candidates.values():
        for p in group:
            try:
                fast_groups[(os.path.getsize(p), fast_hash(p))].append(p)
                hashed_fast += 1
            except Exception:
                pass

            if hashed_fast % PRINT_EVERY_HASH == 0:
                stamp(f"  blake3 hashed={hashed_fast:,} fast_groups={len(fast_groups):,}")

    stamp(f"STEP 1/5: blake3 완료 (elapsed={time.time()-t1:.1f}s) fast_groups={len(fast_groups):,}")

    stamp("STEP 1/5: SHA256 최종 검증 시작")
    t2 = time.time()

    final = defaultdict(list)
    hashed_sha = 0

    for (size, _fh), files in fast_groups.items():
        if len(files) > 1:
            for p in files:
                try:
                    final[(size, sha256_hex(p))].append(p)
                    hashed_sha += 1
                except Exception:
                    pass

                if hashed_sha % PRINT_EVERY_HASH == 0:
                    stamp(f"  sha256 hashed={hashed_sha:,} final_groups={len(final):,}")

    stamp(f"STEP 1/5: SHA256 완료 (elapsed={time.time()-t2:.1f}s) final_groups={len(final):,}")

    stamp("STEP 1/5: 01_duplicate_result.csv 저장")
    with CSV_DUP.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["SIZE", "SHA256", "FILE_PATH"])
        out_rows = 0
        for (size, sha), files in final.items():
            if len(files) > 1:
                for p in files:
                    w.writerow([size, sha, p])
                    out_rows += 1

    stamp(
        "STEP 1/5: 완료 -> "
        f"{CSV_DUP} rows={out_rows:,} (total_elapsed={time.time()-t0:.1f}s)"
    )


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    i = 0
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{int(f)} {units[i]}" if i == 0 else f"{f:.2f} {units[i]}"


def step2_group_report(CSV_DUP: Path, CSV_GROUP: Path, TXT_GROUP: Path):
    stamp("STEP 2/5: grouped_report 생성 시작")
    t0 = time.time()

    groups = defaultdict(list)  # (sha, size) -> [paths]
    rows = 0

    with CSV_DUP.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows += 1
            try:
                size = int(row["SIZE"])
            except Exception:
                continue

            sha = (row.get("SHA256") or "").strip()
            fp = (row.get("FILE_PATH") or "").strip()
            if sha and fp:
                groups[(sha, size)].append(fp)

            if rows % 20000 == 0:
                stamp(f"  read_rows={rows:,} groups={len(groups):,}")

    dup_groups = [(k, v) for k, v in groups.items() if len(v) > 1]
    stamp(f"  duplicate groups(sha+size)={len(dup_groups):,}")

    with CSV_GROUP.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["SHA256", "SIZE_BYTES", "SIZE_HUMAN", "COUNT", "PATHS"])
        for (sha, size), paths in dup_groups:
            w.writerow([sha, size, human_bytes(size), len(paths), " | ".join(paths)])

    lines = []
    lines.append("Grouped Duplicate Report (by SHA256 + SIZE)\n")
    lines.append(f"Input: {CSV_DUP}\n")
    lines.append(f"Total duplicate groups (sha+size): {len(dup_groups)}\n")
    lines.append("\n")

    for (sha, size), paths in dup_groups:
        header = f"[{size} bytes | {human_bytes(size)} | count={len(paths)} | sha256={sha}]"
        lines.append(header)
        for p in paths:
            lines.append(f"  - {p}")
        lines.append("")

    TXT_GROUP.write_text("\n".join(lines), encoding="utf-8")
    stamp(
        "STEP 2/5: 완료 -> "
        f"{CSV_GROUP}, {TXT_GROUP} (elapsed={time.time()-t0:.1f}s)"
    )


def step3_count_filter(CSV_GROUP: Path, CSV_COUNT3: Path):
    stamp("STEP 3/5: COUNT>=3 필터 생성 시작")
    t0 = time.time()
    kept = 0

    with CSV_GROUP.open("r", encoding="utf-8-sig", newline="") as f_in, \
         CSV_COUNT3.open("w", encoding="utf-8-sig", newline="") as f_out:

        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()

        for r in reader:
            try:
                if int(r.get("COUNT", "0")) >= MIN_COUNT:
                    writer.writerow(r)
                    kept += 1
            except Exception:
                pass

    stamp(
        "STEP 3/5: 완료 -> "
        f"{CSV_COUNT3} rows={kept:,} (elapsed={time.time()-t0:.1f}s)"
    )


def extract_paths(path_blob: str):
    if not path_blob:
        return []
    found = WIN_PATH_RE.findall(str(path_blob).strip())
    seen = set()
    out = []
    for p in found:
        p2 = p.strip().strip('"').strip("'").strip()
        if p2 and p2 not in seen:
            seen.add(p2)
            out.append(p2)
    return out


def step4_big_dup_analysis(CSV_COUNT3: Path, CSV_BIG: Path, CSV_BIG_PATHS: Path, top_n: int):
    """
    DUP 모드 STEP 4/5

    - COUNT >= MIN_COUNT(기본 3) 그룹만 대상
    - 각 그룹에서 "가장 큰 파일 1개 크기(max_file_bytes)" 기준으로 TOP N 선정
    - 이 정렬 순서를 그대로 review 그룹 순서에 반영하기 위해
      CSV_BIG_PATHS도 top 리스트 순서대로 기록한다.
    """
    stamp("STEP 4/5: 큰 파일 기준 TOP 분석 시작")
    t0 = time.time()

    groups = []  # 각 원소: dict(sha256, count, max_file_bytes, total_bytes, wasted_bytes, keeper_candidate_path, path_sizes)
    read_rows = 0

    with CSV_COUNT3.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        need = {"SHA256", "COUNT", "PATHS"}
        if not need.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"Missing columns: need={need}, got={reader.fieldnames}")

        for row in reader:
            read_rows += 1
            sha = (row.get("SHA256") or "").strip()
            if not sha:
                continue

            paths = extract_paths(row.get("PATHS", ""))
            if len(paths) < MIN_COUNT:
                continue  # COUNT 필터

            max_size = -1
            keeper = ""
            total = 0
            path_sizes: list[tuple[str, int]] = []

            for p in paths:
                try:
                    sz = os.path.getsize(p)
                except OSError:
                    sz = 0
                total += sz
                path_sizes.append((p, sz))
                if sz > max_size:
                    max_size = sz
                    keeper = p

            # max_size가 0 이하인 그룹(모두 접근 실패)은 스킵
            if max_size <= 0:
                continue

            wasted = total - max_size

            groups.append({
                "sha256": sha,
                "count": len(paths),
                "max_file_bytes": max_size,
                "total_bytes": total,
                "wasted_bytes": wasted,
                "keeper_candidate_path": keeper,
                "path_sizes": path_sizes,
            })

            if read_rows % 5000 == 0:
                stamp(f"  scanned_rows={read_rows:,} kept_groups={len(groups):,}")

    if not groups:
        stamp("STEP 4/5: COUNT/사이즈 조건에 맞는 그룹이 없습니다.")
        # 빈 CSV라도 만들어 둔다.
        with CSV_BIG.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["sha256", "count", "max_file_bytes", "total_bytes", "wasted_bytes", "keeper_candidate_path"],
            )
            w.writeheader()
        with CSV_BIG_PATHS.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["sha256", "path", "size_bytes"])
            w.writeheader()
        return

    # 🔹 정렬 기준 변경: "가장 큰 파일 1개 크기" 기준 내림차순
    groups.sort(key=lambda g: g["max_file_bytes"], reverse=True)
    top = groups[:top_n]

    # 04_big_dup_top.csv 작성 (요약 메타)
    with CSV_BIG.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "sha256",
                "count",
                "max_file_bytes",
                "total_bytes",
                "wasted_bytes",
                "keeper_candidate_path",
            ],
        )
        w.writeheader()
        for g in top:
            w.writerow({
                "sha256": g["sha256"],
                "count": g["count"],
                "max_file_bytes": g["max_file_bytes"],
                "total_bytes": g["total_bytes"],
                "wasted_bytes": g["wasted_bytes"],
                "keeper_candidate_path": g["keeper_candidate_path"],
            })

    # 05_big_dup_top_paths.csv 작성 (실제 경로 + 크기)
    # 👉 여기서도 top 순서를 그대로 사용하므로,
    #    review 그룹 폴더 순서 = max_file_bytes 기준 순서가 된다.
    with CSV_BIG_PATHS.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["sha256", "path", "size_bytes"])
        w.writeheader()

        for g in top:  # 이미 max_file_bytes 기준으로 정렬된 상태
            sha = g["sha256"]
            for p, sz in g["path_sizes"]:
                w.writerow({"sha256": sha, "path": p, "size_bytes": sz})

    stamp(
        "STEP 4/5: 완료 -> "
        f"{CSV_BIG}, {CSV_BIG_PATHS} groups={len(top):,} (elapsed={time.time()-t0:.1f}s)"
    )


# ---------------- BIGFILE 모드: 대용량 "중복" 후보 그룹 ----------------

def step_bigfile_candidates(ROOT: Path, CSV_BIG: Path, CSV_BIG_PATHS: Path,
                            min_size_mb: int, max_groups: int,
                            exts=None):
    """
    BIGFILE 모드:
      - min_size_mb 이상(+ 확장자 필터)
      - size → blake3 → sha256 중복 그룹(파일 수 >= BIG_MIN_DUP_COUNT)만 대상
      - wasted_bytes 기준 TOP max_groups 그룹 선택
    """
    stamp("BIGFILE MODE: 대용량 중복 그룹 수집 시작")
    t0 = time.time()

    min_bytes = min_size_mb * 1024 * 1024
    exts_norm = None
    if exts:
        exts_norm = {e.lower() for e in exts}

    size_buckets = defaultdict(list)
    scanned = 0

    for root, _, files in os.walk(str(ROOT)):
        for name in files:
            p = os.path.join(root, name)
            scanned += 1
            try:
                size = os.path.getsize(p)
            except OSError:
                continue

            if size < min_bytes:
                continue

            if exts_norm:
                ext = os.path.splitext(name)[1].lower()
                if ext not in exts_norm:
                    continue

            size_buckets[size].append(p)

            if scanned % PRINT_EVERY_FILES == 0:
                stamp(f"  scanned={scanned:,} big_size_buckets={len(size_buckets):,}")

    # size 기준으로 2개 이상 있는 것만 남김
    size_buckets = {
        sz: paths for sz, paths in size_buckets.items()
        if len(paths) >= BIG_MIN_DUP_COUNT
    }

    if not size_buckets:
        stamp("BIGFILE MODE: 조건에 맞는 '대용량 중복 그룹(size)' 없음.")
        with CSV_BIG.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["sha256", "count", "total_bytes", "wasted_bytes", "keeper_candidate_path"]
            )
            w.writeheader()
        with CSV_BIG_PATHS.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["sha256", "path", "size_bytes"])
            w.writeheader()
        return

    stamp(f"BIGFILE MODE: size 기준 중복 후보 버킷={len(size_buckets):,}")

    # blake3 1차
    fast_groups = defaultdict(list)
    hashed_fast = 0

    for size, paths in size_buckets.items():
        for p in paths:
            try:
                h_fast = fast_hash(p)
            except Exception:
                continue
            fast_groups[(size, h_fast)].append(p)
            hashed_fast += 1

            if hashed_fast % PRINT_EVERY_HASH == 0:
                stamp(f"  blake3 hashed={hashed_fast:,} fast_groups={len(fast_groups):,}")

    # sha256 최종
    final_groups = defaultdict(list)
    hashed_sha = 0

    for (size, h_fast), paths in fast_groups.items():
        if len(paths) < BIG_MIN_DUP_COUNT:
            continue
        for p in paths:
            try:
                h_sha = sha256_hex(p)
            except Exception:
                continue
            final_groups[(size, h_sha)].append(p)
            hashed_sha += 1

            if hashed_sha % PRINT_EVERY_HASH == 0:
                stamp(f"  sha256 hashed={hashed_sha:,} final_groups={len(final_groups):,}")

    metrics = []
    for (size, sha), paths in final_groups.items():
        if len(paths) < BIG_MIN_DUP_COUNT:
            continue

        total_bytes = 0
        keeper = ""
        max_sz = -1

        for p in paths:
            try:
                sz = os.path.getsize(p)
            except OSError:
                sz = size
            total_bytes += sz
            if sz > max_sz:
                max_sz = sz
                keeper = p

        wasted = total_bytes - (max_sz if max_sz > 0 else 0)

        metrics.append({
            "sha256": sha,
            "count": len(paths),
            "total_bytes": total_bytes,
            "wasted_bytes": wasted,
            "keeper_candidate_path": keeper,
        })

    if not metrics:
        stamp("BIGFILE MODE: 해시 기준으로 남는 대용량 중복 그룹이 없음.")
        with CSV_BIG.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["sha256", "count", "total_bytes", "wasted_bytes", "keeper_candidate_path"]
            )
            w.writeheader()
        with CSV_BIG_PATHS.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["sha256", "path", "size_bytes"])
            w.writeheader()
        return

    metrics.sort(key=lambda x: x["wasted_bytes"], reverse=True)
    top = metrics[:max_groups]
    top_sha = {m["sha256"] for m in top}

    with CSV_BIG.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["sha256", "count", "total_bytes", "wasted_bytes", "keeper_candidate_path"]
        )
        w.writeheader()
        w.writerows(top)

    with CSV_BIG_PATHS.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["sha256", "path", "size_bytes"])
        w.writeheader()
        for (size, sha), paths in final_groups.items():
            if sha not in top_sha:
                continue
            for p in paths:
                try:
                    sz = os.path.getsize(p)
                except OSError:
                    sz = size
                w.writerow({"sha256": sha, "path": p, "size_bytes": sz})

    stamp(
        "BIGFILE MODE: 완료 -> "
        f"{CSV_BIG}, {CSV_BIG_PATHS} groups={len(top):,} (elapsed={time.time()-t0:.1f}s)"
    )


# ---------------- 리뷰 링크 생성 (그룹 폴더 넘버링) ----------------

def safe_filename(s: str, max_len=180) -> str:
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len].rstrip() if len(s) > max_len else s


def short_label_from_path(p: Path) -> str:
    parts = p.parts
    drive = parts[0].replace(":", "") if len(parts) > 0 else "DRV"
    parents = p.parent.parts[-2:] if len(p.parent.parts) >= 2 else p.parent.parts
    parent_str = "__".join([x for x in parents if x and x not in (p.drive, "\\")])
    base = p.name
    label = f"{drive}__{parent_str}__{base}" if parent_str else f"{drive}__{base}"
    return safe_filename(label)


def format_size_tag(sz: int) -> str:
    if sz >= 1024**3:
        return f"{sz / (1024**3):.1f}GB"
    if sz >= 1024**2:
        return f"{sz / (1024**2):.3f}MB"
    return f"{max(1, sz // 1024)}KB"


def create_shortcut_lnk(link_path: Path, target_path: Path):
    lnk = str(link_path).replace("'", "''")
    tgt = str(target_path).replace("'", "''")

    ps = (
        "$WshShell = New-Object -ComObject WScript.Shell;\n"
        f"$Shortcut = $WshShell.CreateShortcut('{lnk}');\n"
        f"$Shortcut.TargetPath = '{tgt}';\n"
        f"$Shortcut.WorkingDirectory = (Split-Path '{tgt}' -Parent);\n"
        "$Shortcut.Save();\n"
    )

    subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


def step5_make_review_links(CSV_BIG_PATHS: Path, REVIEW_DIR: Path):
    # 입력 CSV: sha256(=그룹ID), path, size_bytes
    #  - DUP 모드   : sha256 = 실제 중복 그룹 해시
    #  - BIGFILE 모드: sha256 = "BIG_0001" 같은 가짜 그룹 ID 또는 실제 해시
    # 그룹 폴더 이름: 01_SHA_xxx, 02_SHA_xxx ...
    stamp(f"STEP 5: review 링크 생성 시작 -> {REVIEW_DIR}")
    t0 = time.time()

    groups = defaultdict(list)

    with CSV_BIG_PATHS.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = (row.get("sha256") or "").strip()
            p = (row.get("path") or "").strip()
            if not gid or not p:
                continue
            try:
                sz = int(row.get("size_bytes") or 0)
            except Exception:
                sz = 0
            groups[gid].append((p, sz))

    made_groups = 0
    made_links = 0
    missing_targets = 0

    ordered = list(groups.items())  # dict 삽입 순서 유지

    for idx, (gid, items) in enumerate(ordered, start=1):
        sha_tag = gid[:32] if len(gid) > 32 else gid
        group_dir = REVIEW_DIR / f"{idx:02d}_SHA_{sha_tag}"
        group_dir.mkdir(parents=True, exist_ok=True)
        made_groups += 1

        items.sort(key=lambda x: x[1], reverse=True)

        for file_idx, (p_str, sz) in enumerate(items, start=1):
            target = Path(p_str)
            if not target.exists():
                missing_targets += 1

            label = short_label_from_path(target)
            size_tag = format_size_tag(sz)

            link_name = f"{file_idx:02d}__{size_tag}__{label}.lnk"
            link_path = group_dir / safe_filename(link_name, max_len=220)

            if link_path.exists():
                continue

            create_shortcut_lnk(link_path, target)
            made_links += 1

            if made_links % 500 == 0:
                stamp(f"  shortcuts_created={made_links:,} groups={made_groups:,}")

    stamp(f"STEP 5: 완료 (elapsed={time.time()-t0:.1f}s)")
    stamp(f"- Review root: {REVIEW_DIR}")
    stamp(f"- Groups created: {made_groups:,}")
    stamp(f"- Shortcuts created: {made_links:,}")
    stamp(f"- Missing target files: {missing_targets:,}")


# ---------------- 모드/개수 입력 ----------------

def prompt_mode() -> str:
    while True:
        print()
        print("[모드 선택]")
        print("  1) DUP 모드     (중복 그룹 TOP N)")
        print("  2) BIGFILE 모드 (대용량 중복 그룹 TOP N)")
        s = input("선택 (Enter=2 BIGFILE): ").strip()

        if not s:
            return "BIGFILE"
        if s == "1":
            return "DUP"
        if s == "2":
            return "BIGFILE"

        s_up = s.upper()
        if s_up in ("DUP", "BIGFILE"):
            return s_up

        print("잘못된 입력. 1 / 2 / DUP / BIGFILE 중 하나를 입력하세요.")


def prompt_top_n(default_n: int = 50) -> int:
    while True:
        s = input(f"생성할 그룹 개수 N (Enter={default_n}): ").strip()
        if not s:
            return default_n
        try:
            n = int(s)
            if n <= 0:
                print("N은 1 이상의 정수여야 합니다.")
                continue
            return n
        except ValueError:
            print("정수를 입력하세요.")


# ---------------- 02 실행용 cmd 생성 ----------------

def write_next_02_cmd(BASE: Path, RUN_DIR: Path, sample_n: int = 10):
    # RUN_DIR 하위에:
    #   - run_02_next.cmd
    #   - NEXT_02_CMD.txt
    # 를 생성하고, 콘솔에도 동일한 명령을 출력한다.
    base_str = str(BASE)
    run_str = str(RUN_DIR)

    cmd_lines = [
        "@echo off",
        f'cd /d "{base_str}"',
        f'py {NEXT_02_SCRIPT} "{run_str}" {sample_n}',
        "pause",
        "",
    ]
    cmd_text = "\n".join(cmd_lines)

    cmd_path = RUN_DIR / "run_02_next.cmd"
    txt_path = RUN_DIR / "NEXT_02_CMD.txt"

    try:
        cmd_path.write_text(cmd_text, encoding="utf-8")
    except Exception as e:
        stamp(f"[WARN] run_02_next.cmd 작성 실패: {e}")

    try:
        txt_path.write_text(
            f'cd /d "{base_str}" && py {NEXT_02_SCRIPT} "{run_str}" {sample_n}\n',
            encoding="utf-8",
        )
    except Exception as e:
        stamp(f"[WARN] NEXT_02_CMD.txt 작성 실패: {e}")

    print()
    print("[NEXT: run 02]")
    print(f'cd /d "{base_str}"')
    print(f'py {NEXT_02_SCRIPT} "{run_str}" {sample_n}')
    print()


# ---------------- main ----------------

def main():
    stamp("BOOT: starting...")

    ROOT = prompt_dir(
        prompt="대상 폴더(ROOT)를 입력 (Enter=기본값)",
        default=DEFAULT_ROOT,
        must_exist=True,
        create_if_missing=False
    )

    BASE = prompt_dir(
        prompt="결과 폴더(BASE)를 입력 (Enter=기본값, 없으면 생성)",
        default=DEFAULT_BASE,
        must_exist=False,
        create_if_missing=True
    )

    # 1) 먼저 RUN_ID만 시간 기준으로 만든 뒤
    RUN_ID = datetime.now().strftime("%Y%m%d_%H%M")

    # 2) 모드/개수부터 선택
    print()
    stamp(f"RUN_ID = {RUN_ID}")
    stamp(f"ROOT   = {ROOT}")
    stamp(f"BASE   = {BASE}")
    print("위 경로/폴더 구성이 맞는지 확인하세요. (잘못되었으면 Ctrl+C로 중단)")

    mode = prompt_mode()
    top_n = prompt_top_n(default_n=50)

    # 3) 모드에 따라 run 폴더 이름에 suffix 부여
    #    예: run_20260126_0553_dup, run_20260126_0553_big
    if mode == "DUP":
        mode_suffix = "dup"
        review_sub = "01_review_dup"
    else:
        mode_suffix = "big"
        review_sub = "01_review_big"

    RUNS_ROOT = BASE / "Runs"
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)

    RUN_DIR = RUNS_ROOT / f"run_{RUN_ID}_{mode_suffix}"
    REVIEW_DIR = RUN_DIR / review_sub

    RUN_DIR.mkdir(parents=True, exist_ok=True)
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)

    # 기본 CSV 경로 (이름은 그대로)
    CSV_DUP = RUN_DIR / "01_duplicate_result.csv"
    CSV_GROUP = RUN_DIR / "02_grouped_report.csv"
    TXT_GROUP = RUN_DIR / "02_grouped_report.txt"
    CSV_COUNT3 = RUN_DIR / "03_count_3_plus.csv"
    CSV_BIG = RUN_DIR / "04_big_dup_top.csv"
    CSV_BIG_PATHS = RUN_DIR / "05_big_dup_top_paths.csv"

    # run_meta.txt 기록
    meta_path = RUN_DIR / "run_meta.txt"
    try:
        with meta_path.open("w", encoding="utf-8") as f:
            f.write(f"ROOT={ROOT}\n")
            f.write(f"BASE={BASE}\n")
            f.write(f"RUN_ID={RUN_ID}\n")
            f.write(f"MODE={mode}\n")
            f.write(f"TOP_N={top_n}\n")
    except Exception as e:
        stamp(f"[WARN] run_meta.txt 기록 실패: {e}")

    stamp(f"RUN_DIR = {RUN_DIR}")
    stamp(f"MODE    = {mode}")
    stamp(f"TOP_N   = {top_n}")
    stamp(f"RUN_DIR = {RUN_DIR}")
    stamp(f"MODE    = {mode}")
    stamp(f"TOP_N   = {top_n}")

    # ---------------- 실제 파이프라인 실행 ----------------
    if mode == "DUP":
        stamp("=== DUP 모드 파이프라인 시작 ===")
        # 1) 전체 중복 탐지
        step1_scan_duplicates(ROOT, CSV_DUP)
        # 2) 그룹 리포트
        step2_group_report(CSV_DUP, CSV_GROUP, TXT_GROUP)
        # 3) COUNT>=3 필터
        step3_count_filter(CSV_GROUP, CSV_COUNT3)
        # 4) wasted_bytes 기준 TOP N 그룹 선택
        step4_big_dup_analysis(CSV_COUNT3, CSV_BIG, CSV_BIG_PATHS, top_n)
        # 5) 리뷰 링크 생성
        step5_make_review_links(CSV_BIG_PATHS, REVIEW_DIR)
    else:
        stamp("=== BIGFILE 모드 파이프라인 시작 ===")
        # 대용량 중복 그룹 후보 수집 + TOP N
        step_bigfile_candidates(
            ROOT,
            CSV_BIG,
            CSV_BIG_PATHS,
            BIG_MIN_SIZE_MB,
            top_n,
            BIG_EXT_WHITELIST,
        )
        # 리뷰 링크 생성
        step5_make_review_links(CSV_BIG_PATHS, REVIEW_DIR)

    # 02 실행용 cmd / 텍스트 생성
    write_next_02_cmd(BASE, RUN_DIR, sample_n=10)



if __name__ == "__main__":
    main()


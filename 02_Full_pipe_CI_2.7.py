# ============================================================
# 02_Full_pipe_CI.py  (최종본)
# ============================================================
# 사용법:
#   py 02_Full_pipe_CI.py <RunDir> [SampleN]
#
#   <RunDir>  : run_YYYYMMDD_HHMM 폴더 경로
#   [SampleN] : 해시 샘플 개수 (기본 10)
#
# 전제:
#   - 01_Dedup_pipe_CI_new.py 실행으로
#       BASE\Runs\run_...\01_review_dup 또는 01_review_big 생성됨
#   - 각 그룹 폴더에서 "남길 파일"만 mmm...lnk 로 이름 변경
#   - run_...\run_meta.txt 에 ROOT/BASE/RUN_ID/MODE/TOP_N 기록됨
#
# 동작 요약:
#   0) 리뷰 디렉터리 자동 탐색:
#        01_review_dup → 01_review_big → 01_review 순서로 존재 여부 확인
#   1) REVIEW STATS / GROUP STATS / DISK CHECK / ROOT STATS 출력
#        - 총 링크 수, mmm/후보 개수, 실제 존재하는 대상 용량
#        - remove 시 예상 감소 용량, ROOT 기준 % 감소
#        - 디스크 여유 공간과 quarantine 예상 복사 용량
#        → 사용자에게 y/N로 최종 확인
#   2) 01_review에서 mmm 아닌 .lnk만 후보로 추출
#        -> 02_post_review\remove_candidates.csv
#        -> 02_post_review\broken_targets.csv
#   3) TARGET_EXISTS=True 인 것들만 03_quarantine 에 flat 복사
#        (원본 파일명 그대로, 중복 시 __DUP__n suffix)
#        -> 02_post_review\copy_log.csv
#        -> 02_post_review\quarantine_candidates.csv
#   4) 샘플 N개 해시 검증 (원본 vs quarantine 사본)
#   5) 원본 파일들을 04_removed_originals 로 flat 이동
#        -> 02_post_review\finalize_move_log.csv
#   6) mmm_*.lnk 가 가리키는 원본이
#        - 실제 존재하는지(EXISTS=True)
#        - 04_removed_originals 아래로 들어가 있지 않은지
#      검증
#        -> 02_post_review\mmm_check_ok.csv
#        -> 02_post_review\mmm_check_problem.csv
#   7) 6번에서 "정상"으로 판정된 mmm 타깃들에 대해
#        - run_dir\05_confirm_keep 폴더 생성
#        - 더블클릭 시 explorer /select 로
#          해당 파일이 선택된 Explorer 창 열리는 .lnk 생성
#
#   삭제/이동 대상은 "mmm 표시 안 된 링크들의 대상 파일" 뿐이다.
# ============================================================

import os
import sys
import csv
import shutil
import random
import hashlib
from pathlib import Path

try:
    import win32com.client  # pywin32
except ImportError:
    print("ERROR: pywin32 not installed. Run: python -m pip install pywin32")
    sys.exit(1)


# ---------------- 공통 유틸 ----------------

def is_mmm(name: str) -> bool:
    return name.lower().startswith("mmm")


def sanitize_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    return "".join("_" if ch in bad else ch for ch in name)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def next_free_path(root: Path, name: str) -> Path:
    base, ext = os.path.splitext(name)
    candidate = root / name
    i = 1
    while candidate.exists():
        candidate = root / f"{base}__DUP__{i}{ext}"
        i += 1
    return candidate


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    i = 0
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{int(f)} {units[i]}" if i == 0 else f"{f:.2f} {units[i]}"


# ---------------- 리뷰 루트 / 메타 / 통계 ----------------

def find_review_root(run_dir: Path) -> Path | None:
    """
    우선순위:
      1) 01_review_dup
      2) 01_review_big
      3) 01_review  (레거시 호환)
    """
    for name in ("01_review_dup", "01_review_big", "01_review"):
        p = run_dir / name
        if p.exists() and p.is_dir():
            return p
    return None


def resolve_root_from_meta(run_dir: Path) -> Path | None:
    """
    run_...\run_meta.txt 에서 ROOT=... 라인을 찾아 Path로 리턴.
    없으면 None.
    """
    meta = run_dir / "run_meta.txt"
    if not meta.exists():
        return None

    try:
        for line in meta.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip().upper()
            val = val.strip()
            if key == "ROOT" and val:
                return Path(val)
    except Exception:
        return None

    return None


def scan_review_links(review_root: Path) -> dict:
    """
    리뷰 폴더 전체를 스캔해서:
      - 총 링크 수
      - mmm / non-mmm 링크 수
      - 실제 존재하는 대상 파일 개수 및 용량 (전체 / keep / remove)
    를 계산.
    """
    shell = win32com.client.Dispatch("WScript.Shell")

    total_links = 0
    mmm_links = 0
    non_mmm_links = 0

    exist_total = 0
    exist_keep = 0
    exist_remove = 0

    bytes_total = 0
    bytes_keep = 0
    bytes_remove = 0

    for lnk in review_root.rglob("*.lnk"):
        total_links += 1
        name = lnk.name
        keep_flag = is_mmm(name)
        if keep_flag:
            mmm_links += 1
        else:
            non_mmm_links += 1

        try:
            sc = shell.CreateShortcut(str(lnk))
            target = sc.TargetPath or ""
        except Exception:
            target = ""

        if not target:
            continue

        try:
            sz = os.path.getsize(target)
        except OSError:
            continue

        exist_total += 1
        bytes_total += sz

        if keep_flag:
            exist_keep += 1
            bytes_keep += sz
        else:
            exist_remove += 1
            bytes_remove += sz

    return {
        "total_links": total_links,
        "mmm_links": mmm_links,
        "non_mmm_links": non_mmm_links,
        "exist_total": exist_total,
        "bytes_total": bytes_total,
        "exist_keep": exist_keep,
        "bytes_keep": bytes_keep,
        "exist_remove": exist_remove,
        "bytes_remove": bytes_remove,
    }


def scan_root_stats(root_path: Path) -> dict | None:
    """
    ROOT 전체 파일 개수 / 총 용량 계산.
    """
    if not root_path.exists() or not root_path.is_dir():
        return None

    total_files = 0
    total_bytes = 0

    for r, _, files in os.walk(str(root_path)):
        for name in files:
            p = os.path.join(r, name)
            try:
                total_files += 1
                total_bytes += os.path.getsize(p)
            except OSError:
                continue

    return {
        "root_path": str(root_path),
        "total_files": total_files,
        "total_bytes": total_bytes,
    }


def get_disk_usage_for_path(p: Path) -> dict:
    """
    해당 경로가 속한 드라이브 기준 디스크 사용량 조회.
    """
    drive = p.drive
    if not drive:
        # 예: 상대 경로 등인 경우 run_dir 기준 C: 가정
        drive = os.path.splitdrive(str(p))[0] or "C:"
    root = drive + "\\"
    total, used, free = shutil.disk_usage(root)
    return {
        "drive_root": root,
        "total": total,
        "used": used,
        "free": free,
    }


def print_stats_and_confirm(
    review_stats: dict,
    root_stats: dict | None,
    disk_stats: dict,
) -> bool:
    """
    통계 출력 후 사용자에게 y/N 확인.
    True면 계속 진행, False면 중단.
    """
    total_links = review_stats["total_links"]
    mmm_links = review_stats["mmm_links"]
    non_mmm_links = review_stats["non_mmm_links"]

    exist_total = review_stats["exist_total"]
    bytes_total = review_stats["bytes_total"]
    exist_keep = review_stats["exist_keep"]
    bytes_keep = review_stats["bytes_keep"]
    exist_remove = review_stats["exist_remove"]
    bytes_remove = review_stats["bytes_remove"]

    # REVIEW STATS
    print()
    print("[REVIEW STATS]")
    print(f"  총 리뷰 링크 수               : {total_links:,} 개")
    print(f"  mmm(보존 체크) 링크 수        : {mmm_links:,} 개")
    print(f"  미체크(후보) 링크 수          : {non_mmm_links:,} 개")
    print(
        f"  실제 존재하는 대상 파일(keep+remove) : {exist_total:,} 개"
        f" ({human_bytes(bytes_total)})"
    )
    print(
        f"    └ 그 중 보존(mmm)          : {exist_keep:,} 개"
        f" ({human_bytes(bytes_keep)})"
    )
    print(
        f"    └ 그 중 remove 후보         : {exist_remove:,} 개"
        f" ({human_bytes(bytes_remove)})"
    )

    # GROUP STATS (이번 run이 실제로 다루는 대상 기준)
    print()
    print("[GROUP STATS] 이번 run에서 다루는 대상 기준")
    print(f"  대상 파일 수 (keep+remove)   : {exist_total:,} 개")
    print(f"  대상 파일 총 용량            : {human_bytes(bytes_total)}")
    print(f"  리무브 후 예상 남는 용량      : {human_bytes(bytes_keep)}")
    reduced_bytes = bytes_total - bytes_keep
    reduced_pct = (reduced_bytes / bytes_total * 100.0) if bytes_total > 0 else 0.0
    print(
        f"  예상 용량 감소               : {human_bytes(reduced_bytes)}"
        f" ({reduced_pct:.2f}%)"
    )

    # DISK CHECK: quarantine 복사 기준 (remove 후보만)
    total = disk_stats["total"]
    used = disk_stats["used"]
    free = disk_stats["free"]

    free_after_copy = free - bytes_remove
    print()
    print("[DISK CHECK] quarantine 복사 예상 용량")
    print(f"  remove 후보 파일 수          : {exist_remove:,} 개")
    print(f"  총 예상 복사 용량            : {human_bytes(bytes_remove)}")
    print(f"  현재 드라이브 총 용량        : {human_bytes(total)}")
    print(f"  현재 사용 중인 용량          : {human_bytes(used)}")
    print(f"  현재 남은 여유 공간          : {human_bytes(free)}")
    print(
        f"  [이론상] 복사 후 남을 여유 공간 : {human_bytes(max(0, free_after_copy))}"
    )

    # ROOT DATASET STATS (run_meta에서 ROOT가 잡혀 있을 때만)
    if root_stats is None:
        print()
        print("[DATASET (ROOT) STATS]")
        print("  run_meta.txt에서 ROOT 정보를 찾지 못해 전체 통계를 생략합니다.")
    else:
        root_total_bytes = root_stats["total_bytes"]
        root_total_files = root_stats["total_files"]
        root_after_bytes = max(0, root_total_bytes - bytes_remove)
        root_reduced_pct = (
            bytes_remove / root_total_bytes * 100.0
            if root_total_bytes > 0 else 0.0
        )

        print()
        print("[DATASET (ROOT) STATS]")
        print(f"  ROOT 경로                    : {root_stats['root_path']}")
        print(f"  ROOT 전체 파일 수           : {root_total_files:,} 개")
        print(f"  ROOT 전체 용량              : {human_bytes(root_total_bytes)}")
        print(f"  이번 run remove 후보 용량   : {human_bytes(bytes_remove)}")
        print(f"  제거 후 ROOT 예상 용량      : {human_bytes(root_after_bytes)}")
        print(
            f"  ROOT 기준 예상 용량 감소    : {human_bytes(bytes_remove)}"
            f" ({root_reduced_pct:.2f}%)"
        )

    # 사용자 확인
    print()
    ans = input(
        "위 정보를 확인했습니다. quarantine 복사 및 원본 이동을 진행할까요? [y/N]: "
    ).strip().lower()
    if ans not in ("y", "yes", "ㅛ"):
        print("사용자 취소: 아무 작업도 수행하지 않습니다.")
        return False

    return True


# ---------------- 1단계: review → candidates/broken ----------------

def extract_candidates(run_dir: Path, review_root: Path):
    post_review = run_dir / "02_post_review"
    post_review.mkdir(parents=True, exist_ok=True)

    shell = win32com.client.Dispatch("WScript.Shell")

    items = []
    broken = []

    for lnk in review_root.rglob("*.lnk"):
        if is_mmm(lnk.name):
            continue  # 보존 대상은 여기서 제외 (삭제/이동 대상 아님)

        sha_group = lnk.parent.name
        target = ""
        try:
            sc = shell.CreateShortcut(str(lnk))
            target = sc.TargetPath or ""
        except Exception:
            target = ""

        if target:
            target_exists = os.path.exists(target)
        else:
            target_exists = False

        row = {
            "SHA_GROUP": sha_group,
            "LINK_NAME": lnk.name,
            "LINK_PATH": str(lnk),
            "TARGET_PATH": target,
            "TARGET_EXISTS": str(bool(target_exists)),
        }
        items.append(row)

        if (not target) or (not target_exists):
            broken.append(row)

    remove_csv = post_review / "remove_candidates.csv"
    broken_csv = post_review / "broken_targets.csv"
    fieldnames = ["SHA_GROUP", "LINK_NAME", "LINK_PATH", "TARGET_PATH", "TARGET_EXISTS"]

    with remove_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(items)

    with broken_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(broken)

    print(f"OK: {len(items)} rows -> {remove_csv}")
    print(f"OK: {len(broken)} broken rows -> {broken_csv}")

    return items, broken


# ---------------- 2단계: quarantine(flat) 빌드 ----------------

def build_quarantine(run_dir: Path, items):
    post_review = run_dir / "02_post_review"
    q_root = run_dir / "03_quarantine"
    q_root.mkdir(parents=True, exist_ok=True)

    q_items = []
    copy_log = []

    for r in items:
        exists = str(r.get("TARGET_EXISTS", "")).strip().lower() == "true"
        if not exists:
            continue

        src = r.get("TARGET_PATH", "")
        sha_group = r.get("SHA_GROUP", "UNKNOWN")
        link_name = r.get("LINK_NAME", "UNKNOWN.lnk")  # 로그용으로만 사용

        if not src:
            status = "FAILED:NO_SRC"
            dst_path = q_root / "NO_SRC"
        else:
            src_path = Path(src)
            base_name = src_path.name
            dst_path = next_free_path(q_root, base_name)

            status = "COPIED"
            try:
                shutil.copy2(src_path, dst_path)
            except Exception as e:
                status = f"FAILED:{type(e).__name__}"

        q_row = dict(r)
        q_row["Q_PATH"] = str(dst_path)
        q_row["Q_STATUS"] = status
        q_items.append(q_row)

        copy_log.append(
            {
                "SHA_GROUP": sha_group,
                "SRC": src,
                "DST": str(dst_path),
                "STATUS": status,
            }
        )

    q_csv = post_review / "quarantine_candidates.csv"
    q_fields = [
        "SHA_GROUP",
        "LINK_NAME",
        "LINK_PATH",
        "TARGET_PATH",
        "TARGET_EXISTS",
        "Q_PATH",
        "Q_STATUS",
    ]
    with q_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=q_fields)
        w.writeheader()
        w.writerows(q_items)

    log_csv = post_review / "copy_log.csv"
    log_fields = ["SHA_GROUP", "SRC", "DST", "STATUS"]
    with log_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=log_fields)
        w.writeheader()
        w.writerows(copy_log)

    copied = sum(1 for r in copy_log if r["STATUS"] == "COPIED")
    failed = sum(1 for r in copy_log if r["STATUS"].startswith("FAILED"))
    print(f"OK: COPIED={copied} FAILED={failed} -> {log_csv}")
    print(f"OK: {len(q_items)} rows -> {q_csv}")

    return q_items


# ---------------- 3단계: 샘플 해시 검증 ----------------

def verify_hash_sample(q_items, sample_n: int) -> bool:
    valid = [
        r
        for r in q_items
        if r.get("Q_STATUS") == "COPIED"
        and r.get("TARGET_PATH")
        and r.get("Q_PATH")
        and Path(r["TARGET_PATH"]).exists()
        and Path(r["Q_PATH"]).exists()
    ]

    if not q_items:
        print("WARN: no quarantine candidates. Nothing to verify.")
        return True

    if not valid:
        print("ABORT: no valid (SRC, DST) pairs for hash check.")
        return False

    sample_count = min(sample_n, len(valid))
    sample_rows = random.sample(valid, sample_count)
    bad = 0

    for r in sample_rows:
        src_path = Path(r["TARGET_PATH"])
        dst_path = Path(r["Q_PATH"])

        try:
            h1 = sha256_of(src_path)
            h2 = sha256_of(dst_path)
        except Exception:
            bad += 1
            continue

        if h1 != h2:
            bad += 1

    if bad != 0:
        print(
            f"ABORT: hash sample mismatch or errors. "
            f"BAD={bad} / SAMPLE={sample_count}"
        )
        return False

    print(f"OK: hash sample verified. SAMPLE={sample_count}")
    return True


# ---------------- 4단계: 원본 이동(평탄화) ----------------

def move_originals(run_dir: Path, q_items):
    post_review = run_dir / "02_post_review"
    removed_root = run_dir / "04_removed_originals"
    removed_root.mkdir(parents=True, exist_ok=True)

    log_rows = []
    moved = 0
    failed = 0
    missing = 0

    for r in q_items:
        src = r.get("TARGET_PATH", "")
        sha_group = r.get("SHA_GROUP", "")

        if not src:
            status = "MISSING_SRC"
            dst = ""
            missing += 1
        else:
            src_path = Path(src)
            if not src_path.exists():
                status = "MISSING_SRC"
                dst = ""
                missing += 1
            else:
                dst_path = next_free_path(removed_root, src_path.name)
                try:
                    shutil.move(str(src_path), str(dst_path))
                    status = "MOVED"
                    dst = str(dst_path)
                    moved += 1
                except Exception as e:
                    status = f"FAILED:{type(e).__name__}"
                    dst = ""
                    failed += 1

        log_rows.append(
            {
                "SHA_GROUP": sha_group,
                "SRC": src,
                "DST": dst,
                "STATUS": status,
            }
        )

    log_csv = post_review / "finalize_move_log.csv"
    log_fields = ["SHA_GROUP", "SRC", "DST", "STATUS"]
    with log_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=log_fields)
        w.writeheader()
        w.writerows(log_rows)

    print(
        f"OK: MOVED={moved} FAILED={failed} MISSING_SRC={missing} -> {log_csv}"
    )
    print(f"REMOVED_DIR: {removed_root}")


# ---------------- 5+6단계: mmm KEEP 검증 + confirm 폴더 생성 ----------------

def check_mmm_integrity_and_build_confirm(run_dir: Path, review_root: Path):
    removed_root = (run_dir / "04_removed_originals").resolve()
    post_review = run_dir / "02_post_review"
    post_review.mkdir(parents=True, exist_ok=True)

    confirm_root = run_dir / "05_confirm_keep"
    confirm_root.mkdir(parents=True, exist_ok=True)

    shell = win32com.client.Dispatch("WScript.Shell")

    ok_rows = []
    problem_rows = []

    for lnk in review_root.rglob("*.lnk"):
        if not is_mmm(lnk.name):
            continue

        sha_group = lnk.parent.name
        try:
            sc = shell.CreateShortcut(str(lnk))
            target = sc.TargetPath or ""
        except Exception:
            target = ""

        exists = bool(target and os.path.exists(target))

        in_removed = False
        if target:
            try:
                target_path = Path(target).resolve()
                in_removed = (target_path == removed_root) or (removed_root in target_path.parents)
            except Exception:
                in_removed = False

        row = {
            "SHA_GROUP": sha_group,
            "LINK_NAME": lnk.name,
            "LINK_PATH": str(lnk),
            "TARGET_PATH": target,
            "EXISTS": "True" if exists else "False",
            "IN_REMOVED_DIR": "True" if in_removed else "False",
        }

        if exists and not in_removed:
            ok_rows.append(row)
        else:
            problem_rows.append(row)

    # CSV 저장
    fields = ["SHA_GROUP", "LINK_NAME", "LINK_PATH", "TARGET_PATH", "EXISTS", "IN_REMOVED_DIR"]

    ok_csv = post_review / "mmm_check_ok.csv"
    with ok_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(ok_rows)

    prob_csv = post_review / "mmm_check_problem.csv"
    with prob_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(problem_rows)

    print(f"MMM KEEP CHECK: OK={len(ok_rows)} PROBLEM={len(problem_rows)}")
    print(f"  OK CSV     : {ok_csv}")
    print(f"  PROBLEM CSV: {prob_csv}")

    # 05_confirm_keep: Explorer /select 링크 생성
    created_links = 0
    explorer_path = Path(os.environ.get("WINDIR", "C:\\Windows")) / "explorer.exe"

    for row in ok_rows:
        target = row.get("TARGET_PATH") or ""
        if not target:
            continue

        # 절대 경로로 정규화 (심볼릭 링크, 상대경로 등 정리)
        try:
            target_path = Path(target).resolve()
        except Exception:
            continue

        orig_name = target_path.name  # 원본 파일명
        safe_name = sanitize_filename(orig_name) + ".lnk"
        lnk_path = next_free_path(confirm_root, safe_name)

        try:
            sc2 = shell.CreateShortcut(str(lnk_path))
            # Explorer를 타깃으로, 새 창 + 해당 파일 선택
            #   예: explorer.exe /n,/select,"C:\path\file.ext"
            sc2.TargetPath = str(explorer_path)
            sc2.Arguments = f'/n,/select,"{target_path}"'

            # Explorer는 /select 인자로 경로를 받기 때문에
            # WorkingDirectory를 굳이 지정하지 않는 편이
            # "폴더만 열리고 선택은 안 되는" 케이스를 줄여준다.
            # sc2.WorkingDirectory = str(target_path.parent)

            sc2.Save()
            created_links += 1
        except Exception:
            continue

    print(f"CONFIRM KEEP LINKS: {created_links} -> {confirm_root}")

    return len(ok_rows), len(problem_rows)


# ---------------- main ----------------

def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        script = Path(sys.argv[0]).name
        print(
            f"Usage: python {script} <RunDir> [SampleN]\n"
            "  <RunDir>  : BASE\\Runs\\run_YYYYMMDD_HHMM 같은 run 폴더 경로\n"
            "  [SampleN] : 해시 샘플 개수 (기본 10)"
        )
        sys.exit(1)

    run_dir = Path(sys.argv[1]).resolve()
    if not run_dir.exists():
        print(f"ERROR: RunDir not found: {run_dir}")
        sys.exit(2)

    sample_n = 10
    if len(sys.argv) == 3:
        try:
            sample_n = int(sys.argv[2])
        except ValueError:
            print(f"WARN: invalid SampleN '{sys.argv[2]}', fallback to 10")

    review_root = find_review_root(run_dir)
    if review_root is None:
        print("ERROR: 리뷰 폴더를 찾지 못했습니다. (01_review_dup / 01_review_big / 01_review 없음)")
        sys.exit(3)

    print(f"RUN_DIR   : {run_dir}")
    print(f"REVIEW_DIR: {review_root}")

    # 리뷰 전체 통계
    review_stats = scan_review_links(review_root)

    # 삭제/이동 후보 목록 추출
    items, broken = extract_candidates(run_dir, review_root)
    if not any(r.get("TARGET_EXISTS", "").strip().lower() == "true" for r in items):
        print("OK: 삭제/이동 가능한 대상이 없습니다. (존재하는 TARGET_PATH 없음)")
        sys.exit(0)

    # 디스크 사용량: remove 후보 중 첫 번째 실제 대상 기준 드라이브
    disk_path = None
    for r in items:
        if r.get("TARGET_EXISTS", "").strip().lower() == "true":
            tp = r.get("TARGET_PATH", "")
            if tp:
                disk_path = Path(tp)
                break

    if disk_path is None:
        print("ERROR: 디스크 사용량을 계산할 대상 경로를 찾지 못했습니다.")
        sys.exit(4)

    disk_stats = get_disk_usage_for_path(disk_path)

    # ROOT 통계: run_meta.txt에서 ROOT 찾기
    root_path = resolve_root_from_meta(run_dir)
    root_stats = scan_root_stats(root_path) if root_path is not None else None

    # 통계 출력 + 사용자 확인
    if not print_stats_and_confirm(review_stats, root_stats, disk_stats):
        sys.exit(0)

    # 실제 작업 시작
    q_items = build_quarantine(run_dir, items)

    if not verify_hash_sample(q_items, sample_n):
        sys.exit(5)

    move_originals(run_dir, q_items)

    ok_cnt, bad_cnt = check_mmm_integrity_and_build_confirm(run_dir, review_root)
    if bad_cnt > 0:
        print("WARN: 일부 mmm keep 타깃이 누락되었거나 04_removed_originals 안에 들어갔습니다.")
        print("      mmm_check_problem.csv를 확인하세요.")
        sys.exit(6)


if __name__ == "__main__":
    main()

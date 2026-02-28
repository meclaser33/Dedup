# 01_Gui_dedup_pipe_2.0claud.py
# Mec_DB 통합 정리 시스템 v2.0
# ─────────────────────────────────────────────────────────────────────────────
# [v2.0 변경사항]
#   1. 중지 버튼 추가 (stop_event → 모든 작업 안전 중단)
#   2. 파일 목록 수집 중 1분마다 진행상황 표시
#   3. 창 크기 조절 가능 (resizable=True, 반응형 레이아웃)
#   4. 유사 폴더 3개 이상 그룹핑 (Union-Find → A-B-C 묶기)
#      ├── 쌍 → 그룹 클러스터로 변환
#      ├── CSV에 group_id 컬럼 추가
#      └── 리뷰 폴더도 그룹별로 생성 (01_폴더A, 02_폴더B, 03_폴더C ...)
# ─────────────────────────────────────────────────────────────────────────────

# --- Windows DPI 설정 (블러 방지) ---
import sys
if sys.platform == "win32":
    import ctypes
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass
# ------------------------------------

import subprocess
import threading
import queue
import os
import csv
import time
import json
import hashlib
from pathlib import Path
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed

import FreeSimpleGUI as sg

# ===== 설정 및 경로 관리 =====
SCRIPT_DIR  = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "gui_config.json"

DEFAULT_ROOT = r"C:\Users\Meclaser\Desktop\Mec_DB"
DEFAULT_BASE = r"C:\elice\Dedup"

SCRIPT_01   = "01_Dedup_pipe_CI_2.7.py"
SCRIPT_02   = "02_Full_pipe_CI_2.7.py"
PYTHON_EXE  = sys.executable


def load_settings():
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"ROOT": DEFAULT_ROOT, "BASE": DEFAULT_BASE}


def save_settings(root, base):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump({"ROOT": root, "BASE": base}, f, ensure_ascii=False, indent=4)
    except Exception:
        pass


saved_cfg = load_settings()

# 로그 전달용 큐
log_queue: "queue.Queue[str]" = queue.Queue()

def append_log(line: str):
    log_queue.put(line)


# ====== 전역 중지 이벤트 ======
_stop_event = threading.Event()


# ====== 유틸 ======

def find_latest_run_dir(base_path: str) -> "Path | None":
    runs_root = Path(base_path) / "Runs"
    if not runs_root.exists():
        return None
    candidates = [d for d in runs_root.iterdir()
                  if d.is_dir() and d.name.startswith("run_")]
    if not candidates:
        return None
    return max(candidates, key=lambda d: d.stat().st_mtime)


def safe_filename(s: str, max_len: int = 100) -> str:
    s = "".join("_" if ch in '<>:"/\\|?*\x00' else ch for ch in s)
    s = " ".join(s.split()).strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s or "NONAME"


def create_dir_shortcut(link_path: Path, target_dir: Path):
    link_path = link_path.with_suffix(".lnk")
    link_path.parent.mkdir(parents=True, exist_ok=True)
    lnk = str(link_path).replace("'", "''")
    tgt = str(target_dir).replace("'", "''")
    ps = (
        "$WshShell = New-Object -ComObject WScript.Shell;\n"
        f"$Shortcut = $WshShell.CreateShortcut('{lnk}');\n"
        f"$Shortcut.TargetPath = '{tgt}';\n"
        f"$Shortcut.WorkingDirectory = '{tgt}';\n"
        "$Shortcut.Save();\n"
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
             "-Command", ps],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except Exception as e:
        append_log(f"[FOLDER][WARN] 링크 생성 실패: {link_path} "
                   f"({type(e).__name__}: {e})")


# ====== Union-Find (그룹핑용) ======

class UnionFind:
    def __init__(self):
        self._parent: dict = {}

    def find(self, x):
        self._parent.setdefault(x, x)
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # 문자열 비교로 루트 결정 (결정론적)
            if ra < rb:
                self._parent[rb] = ra
            else:
                self._parent[ra] = rb

    def groups(self) -> "dict[str, list]":
        """루트 → [멤버 경로들] 딕셔너리 반환"""
        result: dict = {}
        for x in self._parent:
            root = self.find(x)
            result.setdefault(root, []).append(x)
        return result


# ====== 백그라운드 작업들 ======

def run_step01(root: str, base: str, mode: str, top_n: int):
    _stop_event.clear()
    save_settings(root, base)
    try:
        append_log("[STEP 01] 시작합니다...")
        cwd = str(SCRIPT_DIR)
        cmd = [PYTHON_EXE, SCRIPT_01]
        append_log(f"[CMD] {cmd}  (cwd={cwd})")

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True, cwd=cwd, bufsize=1
        )

        answers = [
            root.strip(),
            base.strip(),
            "1" if mode == "DUP" else "2",
            str(top_n),
        ]
        for ans in answers:
            proc.stdin.write(ans + "\n")
            proc.stdin.flush()
        proc.stdin.close()

        for line in proc.stdout:
            if _stop_event.is_set():
                proc.terminate()
                append_log("[STEP 01] ⛔ 사용자에 의해 중단되었습니다.")
                break
            append_log(line.rstrip("\n"))

        rc = proc.wait()
        if not _stop_event.is_set():
            append_log(f"[STEP 01] 종료 (returncode={rc})")
    except Exception as e:
        append_log(f"[ERROR][STEP 01] {type(e).__name__}: {e}")
    finally:
        append_log("__STEP01_DONE__")


def run_step02(base: str, run_dir: "str | None", sample_n: int):
    _stop_event.clear()
    try:
        base = base.strip() or DEFAULT_BASE
        if run_dir:
            target_run = Path(run_dir)
        else:
            latest = find_latest_run_dir(base)
            if latest is None:
                append_log("[ERROR][STEP 02] BASE\\Runs 안에 run_* 폴더가 없습니다.")
                append_log("__STEP02_DONE__")
                return
            target_run = latest

        if not target_run.exists():
            append_log(f"[ERROR][STEP 02] RunDir 경로가 존재하지 않습니다: {target_run}")
            append_log("__STEP02_DONE__")
            return

        append_log(f"[STEP 02] RunDir = {target_run}")
        append_log(f"[STEP 02] SampleN = {sample_n}")

        cwd = str(SCRIPT_DIR)
        cmd = [PYTHON_EXE, SCRIPT_02, str(target_run), str(sample_n)]
        append_log(f"[CMD] {cmd}  (cwd={cwd})")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=cwd, bufsize=1
        )
        for line in proc.stdout:
            if _stop_event.is_set():
                proc.terminate()
                append_log("[STEP 02] ⛔ 사용자에 의해 중단되었습니다.")
                break
            append_log(line.rstrip("\n"))

        rc = proc.wait()
        if not _stop_event.is_set():
            append_log(f"[STEP 02] 종료 (returncode={rc})")
    except Exception as e:
        append_log(f"[ERROR][STEP 02] {type(e).__name__}: {e}")
    finally:
        append_log("__STEP02_DONE__")


# ====== 폴더 해시 유사도 스캔 v2.0 (그룹핑 지원) ======

_CHUNK = 64 * 1024  # 64KB


def fast_fingerprint(fp: Path):
    """
    파일 앞 64KB + 뒤 64KB + 파일크기 → SHA1.
    전체 읽기 대비 100~1000배 빠름, 실용 정확도 99%+
    """
    try:
        sz = fp.stat().st_size
        h = hashlib.sha1(usedforsecurity=False)
        h.update(sz.to_bytes(8, "little"))
        with open(fp, "rb") as f:
            h.update(f.read(_CHUNK))
            if sz > _CHUNK * 2:
                f.seek(-_CHUNK, 2)
                h.update(f.read(_CHUNK))
        return h.hexdigest(), sz
    except Exception:
        return None


def _save_ckpt(path: Path, data: dict):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        pass


def _load_ckpt(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def run_folder_scan(root: str, base: str, depth_limit: int,
                    min_dir_mb: int, min_similarity: float, top_k: int):
    """
    폴더 유사도 스캔 v2.0
      1) 파일 목록 수집 (1분마다 진행상황 표시)
      2) 파일 지문 계산 1번 (앞뒤 64KB, 멀티스레드, 체크포인트)
      3) 폴더별 재귀 해시셋 구성
      4) 자카드 유사도 계산 → Union-Find 그룹핑
      5) CSV (group_id 포함) + .lnk 그룹별 저장
    """
    _stop_event.clear()
    save_settings(root, base)
    t0 = time.time()

    root_path = Path(root.strip() or DEFAULT_ROOT)
    base_path = Path(base.strip() or DEFAULT_BASE)
    min_bytes = min_dir_mb * 1024 * 1024

    ckpt_id   = hashlib.md5(str(root_path).encode()).hexdigest()[:8]
    ckpt_path = base_path / f"_ckpt_{ckpt_id}.json"

    append_log(f"[FOLDER] ▶ 스캔 시작: {root_path}")
    append_log(f"[FOLDER]   깊이≤{depth_limit} | 최소 {min_dir_mb}MB"
               f" | 유사도≥{min_similarity}% | Top {top_k}")
    append_log(f"[FOLDER]   체크포인트: {ckpt_path}")

    # ── Step 1: 파일 목록 수집 (1분마다 진행상황 표시) ──────────────────
    append_log("[1/4] 파일 목록 수집 중...")

    scan_dirs: "set[Path]" = set()
    all_files: "list[Path]" = []
    last_report = time.time()
    report_interval = 60  # 초

    for cur_root, dirs, files in os.walk(root_path, topdown=True):
        if _stop_event.is_set():
            append_log("[1/4] ⛔ 중단 요청 — 파일 목록 수집 중단")
            append_log("__FOLDER_DONE__")
            return

        cur = Path(cur_root)
        depth = len(cur.relative_to(root_path).parts)
        if depth >= depth_limit:
            dirs[:] = []
            continue
        scan_dirs.add(cur)
        for fname in files:
            all_files.append(cur / fname)

        # 1분마다 진행상황 출력
        now = time.time()
        if now - last_report >= report_interval:
            elapsed = now - t0
            append_log(f"    [진행중] {elapsed/60:.1f}분 경과"
                       f" | 폴더 {len(scan_dirs):,}개"
                       f" | 파일 {len(all_files):,}개"
                       f" | 현재 위치: ...{str(cur)[-60:]}")
            last_report = now

    if _stop_event.is_set():
        append_log("__FOLDER_DONE__")
        return

    append_log(f"[1/4] 완료 ({(time.time()-t0)/60:.1f}분)"
               f" | 폴더 {len(scan_dirs):,}개 | 파일 {len(all_files):,}개")

    # ── Step 2: 지문 계산 (체크포인트 지원) ──────────────────────────────
    append_log("[2/4] 빠른 지문 계산 중 (앞뒤 64KB)...")

    file_index: dict = _load_ckpt(ckpt_path)
    if file_index:
        append_log(f"    체크포인트 발견 → {len(file_index):,}개 이어서 진행")

    pending   = [fp for fp in all_files if str(fp) not in file_index]
    n_pending = len(pending)
    append_log(f"    미처리 파일 {n_pending:,}개 계산 시작 (스레드 6개)...")

    processed  = 0
    SAVE_EVERY = 5_000

    with ThreadPoolExecutor(max_workers=6) as ex:
        fmap = {ex.submit(fast_fingerprint, fp): fp for fp in pending}
        for fut in as_completed(fmap):
            if _stop_event.is_set():
                ex.shutdown(wait=False, cancel_futures=True)
                _save_ckpt(ckpt_path, file_index)
                append_log(f"[2/4] ⛔ 중단 — 체크포인트 저장됨 ({len(file_index):,}개)")
                append_log("__FOLDER_DONE__")
                return

            res = fut.result()
            if res:
                h, sz = res
                file_index[str(fmap[fut])] = [h, sz]
            processed += 1
            if processed % SAVE_EVERY == 0:
                elapsed = time.time() - t0
                rate    = processed / elapsed if elapsed > 0 else 1
                eta     = (n_pending - processed) / rate
                append_log(f"    {processed:,}/{n_pending:,}"
                           f" | 경과 {elapsed/60:.1f}분"
                           f" | 잔여 약 {eta/60:.1f}분")
                _save_ckpt(ckpt_path, file_index)

    _save_ckpt(ckpt_path, file_index)
    append_log(f"[2/4] 완료 ({(time.time()-t0)/60:.1f}분)"
               f" | 총 {len(file_index):,}개")

    # ── Step 3: 폴더별 재귀 해시셋 구성 ─────────────────────────────────
    append_log("[3/4] 폴더 해시셋 집계 중...")

    direct: "dict[Path, dict]" = {
        d: {"hashes": set(), "bytes": 0, "files": 0} for d in scan_dirs
    }

    for path_str, (h, sz) in file_index.items():
        parent = Path(path_str).parent
        if parent in direct:
            direct[parent]["hashes"].add(h)
            direct[parent]["bytes"] += sz
            direct[parent]["files"] += 1

    recursive: "dict[Path, dict]" = {
        d: {"hashes": set(v["hashes"]), "bytes": v["bytes"], "files": v["files"]}
        for d, v in direct.items()
    }

    for d in sorted(scan_dirs, key=lambda p: len(p.parts), reverse=True):
        parent = d.parent
        if parent in recursive:
            recursive[parent]["hashes"] |= recursive[d]["hashes"]
            recursive[parent]["bytes"]  += recursive[d]["bytes"]
            recursive[parent]["files"]  += recursive[d]["files"]

    candidates = [
        {"path": d, **v}
        for d, v in recursive.items()
        if v["bytes"] >= min_bytes and v["hashes"]
    ]
    candidates.sort(key=lambda x: x["bytes"], reverse=True)

    append_log(f"[3/4] 완료 ({(time.time()-t0)/60:.1f}분)"
               f" | 후보 {len(candidates)}개 (≥{min_dir_mb}MB)")

    if len(candidates) < 2:
        append_log("[FOLDER] 비교할 후보 폴더가 2개 미만입니다. 조건을 완화해 보세요.")
        append_log("__FOLDER_DONE__")
        return

    # ── Step 4: 유사도 계산 + Union-Find 그룹핑 ──────────────────────────
    n_cands     = len(candidates)
    total_pairs = n_cands * (n_cands - 1) // 2
    append_log(f"[4/4] 유사도 계산 + 그룹핑 중..."
               f" ({n_cands}개 폴더, {total_pairs:,}쌍)")

    uf       = UnionFind()
    pairs    = []   # 전체 유사 쌍 (CSV 상세 기록용)
    compared = 0

    for a, b in combinations(candidates, 2):
        if _stop_event.is_set():
            append_log("[4/4] ⛔ 중단 요청 — 유사도 계산 중단")
            append_log("__FOLDER_DONE__")
            return

        # 부모-자식 관계 제외
        try:
            a["path"].relative_to(b["path"])
            compared += 1
            continue
        except ValueError:
            pass
        try:
            b["path"].relative_to(a["path"])
            compared += 1
            continue
        except ValueError:
            pass

        inter = a["hashes"] & b["hashes"]
        union = a["hashes"] | b["hashes"]
        if not union:
            compared += 1
            continue

        score = len(inter) / len(union) * 100

        if score >= min_similarity:
            pa, pb = str(a["path"]), str(b["path"])
            pairs.append({
                "dir_a":        a["path"],
                "dir_b":        b["path"],
                "score":        round(score, 1),
                "shared_files": len(inter),
                "files_a":      a["files"],
                "files_b":      b["files"],
                "mb_a":         round(a["bytes"] / 1024 / 1024, 1),
                "mb_b":         round(b["bytes"] / 1024 / 1024, 1),
            })
            uf.union(pa, pb)

        compared += 1
        if compared % 100_000 == 0:
            append_log(f"    {compared:,}/{total_pairs:,}쌍"
                       f" | 현재 {len(pairs)}쌍 발견")

    # Union-Find → 그룹 목록 (크기 2 이상만)
    raw_groups = uf.groups()
    groups = [
        sorted(members)
        for members in raw_groups.values()
        if len(members) >= 2
    ]
    # 그룹 크기 내림차순 정렬 (큰 그룹 먼저)
    groups.sort(key=lambda g: len(g), reverse=True)

    # 각 폴더 경로 → 그룹 ID 매핑 (CSV에서 사용)
    path_to_gid: dict = {}
    for gid, members in enumerate(groups, 1):
        for m in members:
            path_to_gid[m] = gid

    pairs_with_gid = [
        {**p, "group_id": path_to_gid.get(str(p["dir_a"]), "-")}
        for p in pairs
    ]
    pairs_with_gid.sort(key=lambda x: (
        x["group_id"] if isinstance(x["group_id"], int) else 9999,
        -x["score"]
    ))
    top_pairs = pairs_with_gid[:top_k]

    append_log(f"[4/4] 완료 | {len(pairs)}쌍 / {len(groups)}그룹 발견 "
               f"(≥{min_similarity}%)")

    group_summary = ", ".join(
        f"G{i}:{len(g)}폴더" for i, g in enumerate(groups[:10], 1)
    )
    if groups:
        append_log(f"[4/4] 그룹 요약: {group_summary}"
                   + (" ..." if len(groups) > 10 else ""))

    # ── 결과 저장 ─────────────────────────────────────────────────────────
    run_id     = time.strftime("%Y%m%d_%H%M")
    run_dir    = base_path / "Runs" / f"run_{run_id}_fol"
    review_dir = run_dir / "01_review_fol"
    review_dir.mkdir(parents=True, exist_ok=True)

    # CSV 저장 (group_id 포함)
    csv_path = run_dir / "folder_similarity_groups.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["rank", "group_id", "score%", "shared", "files_a", "files_b",
                    "MB_a", "MB_b", "dir_a", "dir_b"])
        for i, p in enumerate(top_pairs, 1):
            w.writerow([i, p["group_id"], p["score"], p["shared_files"],
                        p["files_a"], p["files_b"],
                        p["mb_a"], p["mb_b"],
                        str(p["dir_a"]), str(p["dir_b"])])

    # 그룹별 리뷰 폴더 생성
    for gid, members in enumerate(groups[:top_k], 1):
        labels = [chr(ord("A") + i) for i in range(len(members))]
        # 그룹 폴더명: 01_G1_3folders_FolderA__FolderB__FolderC
        folder_names = "__".join(
            safe_filename(Path(m).name, max_len=30) for m in members[:4]
        )
        suffix = f"(+{len(members)-4}개)" if len(members) > 4 else ""
        grp_dir = review_dir / f"{gid:02d}_G{gid}_{len(members)}폴더_{folder_names}{suffix}"
        grp_dir.mkdir(parents=True, exist_ok=True)
        for idx, member in enumerate(members):
            lnk_name = f"{idx+1:02d}_폴더{labels[idx] if idx < 26 else str(idx+1)}"
            create_dir_shortcut(grp_dir / lnk_name, Path(member))

    total_t = time.time() - t0
    append_log("")
    append_log(f"[FOLDER] ✅ 완료! 총 소요: {total_t/60:.1f}분")
    append_log(f"[FOLDER] 유사 쌍: {len(pairs)}쌍 → {len(groups)}그룹")
    append_log(f"[FOLDER] 결과 → {run_dir}")
    append_log(f"[FOLDER] 체크포인트: {ckpt_path}")
    append_log(f"[FOLDER] (다음 실행 시 지문 재계산 없이 바로 비교 진행)")
    append_log("__FOLDER_DONE__")


# ====== GUI 레이아웃 ======

sg.theme("DarkBlue3")
sg.set_options(font=("맑은 고딕", 10))

BTN_STOP = sg.Button("⛔ 중지", key="-STOP-",
                      button_color=("white", "#8B0000"), size=(10, 1))

# --- 탭 01: Dedup ---
layout_tab_01 = [
    [sg.Text("파일 내용/이름 기반 중복 스캔", font=("맑은 고딕", 10, "bold"))],
    [sg.Text("모드:"),
     sg.Radio("DUP",  "M1", key="-MODE_DUP-", default=True),
     sg.Radio("BIG",  "M1", key="-MODE_BIG-")],
    [sg.Text("TOP N:"), sg.Input("50", size=(5, 1), key="-TOPN-")],
    [sg.Button("중복 스캔 실행", key="-RUN01-",
               button_color="firebrick", size=(20, 1))]
]

# --- 탭 02: BigFiles ---
layout_tab_02 = [
    [sg.Text("대용량 파일 리뷰 및 정리", font=("맑은 고딕", 10, "bold"))],
    [sg.Text("샘플링 수:"), sg.Input("10", size=(5, 1), key="-SAMPLEN-")],
    [sg.Button("최신 Run 실행",  key="-RUN02_LATEST-", size=(15, 1)),
     sg.Button("직접 지정 실행", key="-RUN02_MANUAL-", size=(15, 1))]
]

# --- 탭 03: Folder ---
layout_tab_03 = [
    [sg.Text("내용물 유사 폴더 스캔 v2.0 (그룹핑 지원)",
             font=("맑은 고딕", 10, "bold"))],
    [sg.Text("깊이:"),      sg.Input("3",    size=(3, 1), key="-F_DEPTH-"),
     sg.Text("최소MB:"),    sg.Input("1000", size=(6, 1), key="-F_MINMB-")],
    [sg.Text("유사도(%):"), sg.Input("85",   size=(3, 1), key="-F_MINSIM-"),
     sg.Text("Top K:"),     sg.Input("20",   size=(3, 1), key="-F_TOPK-")],
    [sg.Text("* 3개 이상 폴더는 자동으로 그룹(A-B-C...)으로 묶입니다",
             text_color="yellow", font=("맑은 고딕", 9))],
    [sg.Button("폴더 유사도 스캔 시작", key="-RUN_FOL-",
               button_color="darkgreen", size=(25, 1))]
]

# --- 전체 레이아웃 ---
layout = [
    [sg.Text("Mec_DB 통합 정리 시스템 v2.0",
             font=("Malgun Gothic", 16, "bold"), text_color="cyan",
             expand_x=True)],
    [sg.Frame("공통 경로 설정 (자동 저장됨)", [
        [sg.Text("ROOT:"),
         sg.Input(saved_cfg["ROOT"], key="-ROOT-", expand_x=True),
         sg.FolderBrowse("찾기")],
        [sg.Text("BASE:"),
         sg.Input(saved_cfg["BASE"], key="-BASE-", expand_x=True),
         sg.FolderBrowse("찾기")]
    ], expand_x=True)],
    [sg.TabGroup([[
        sg.Tab(" 01.Dedup ",   layout_tab_01),
        sg.Tab(" 02.BigFiles ", layout_tab_02),
        sg.Tab(" 03.Folder ",  layout_tab_03),
    ]], key="-TABS-", expand_x=True)],
    # ── 상태 표시줄 ──
    [sg.Text("대기 중", key="-STATUS-", text_color="lime",
             font=("맑은 고딕", 9), expand_x=True),
     BTN_STOP],
    # ── 로그 창 (크기 자동 확장) ──
    [sg.Multiline(size=(95, 20), key="-LOG-",
                  autoscroll=True, disabled=True,
                  font=("Consolas", 9),
                  expand_x=True, expand_y=True)],
    [sg.Button("로그 지우기", key="-CLEAR-", size=(12, 1)),
     sg.Push(),
     sg.Button("프로그램 종료", key="-EXIT-", size=(14, 1))]
]

window = sg.Window(
    "Mec_DB Cleaner v2.0",
    layout,
    resizable=True,          # ← 창 크기 조절 가능
    finalize=True
)
window.set_min_size((700, 500))


# ====== 스레드 헬퍼 ======
current_thread: "threading.Thread | None" = None

_DONE_TOKENS = {"__STEP01_DONE__", "__STEP02_DONE__", "__FOLDER_DONE__"}

def start_thread(target, *args):
    global current_thread
    if current_thread and current_thread.is_alive():
        sg.popup("이미 작업이 실행 중입니다.\n중지하려면 ⛔ 중지 버튼을 눌러주세요.")
        return
    _stop_event.clear()
    window["-LOG-"].update("")
    window["-STATUS-"].update("🔄 실행 중...", text_color="yellow")
    current_thread = threading.Thread(target=target, args=args, daemon=True)
    current_thread.start()


# ====== 메인 이벤트 루프 ======
while True:
    event, values = window.read(timeout=100)

    if event in (sg.WIN_CLOSED, "-EXIT-"):
        _stop_event.set()
        break

    # ── 중지 버튼 ──
    if event == "-STOP-":
        if current_thread and current_thread.is_alive():
            _stop_event.set()
            append_log("⛔ 중지 요청됨 — 현재 작업 완료 후 종료됩니다...")
            window["-STATUS-"].update("⛔ 중지 요청됨...", text_color="orange")
        else:
            sg.popup("현재 실행 중인 작업이 없습니다.")

    # ── 로그 지우기 ──
    elif event == "-CLEAR-":
        window["-LOG-"].update("")

    # ── 1번 탭 ──
    elif event == "-RUN01-":
        mode = "DUP" if values["-MODE_DUP-"] else "BIGFILE"
        start_thread(run_step01,
                     values["-ROOT-"], values["-BASE-"],
                     mode, int(values["-TOPN-"]))

    # ── 2번 탭 ──
    elif event == "-RUN02_LATEST-":
        start_thread(run_step02,
                     values["-BASE-"], None,
                     int(values["-SAMPLEN-"]))

    elif event == "-RUN02_MANUAL-":
        run_dir = sg.popup_get_text("RunDir 경로를 직접 입력하세요:")
        if run_dir:
            start_thread(run_step02,
                         values["-BASE-"], run_dir,
                         int(values["-SAMPLEN-"]))

    # ── 3번 탭 ──
    elif event == "-RUN_FOL-":
        start_thread(run_folder_scan,
                     values["-ROOT-"], values["-BASE-"],
                     int(values["-F_DEPTH-"]),
                     int(values["-F_MINMB-"]),
                     float(values["-F_MINSIM-"]),
                     int(values["-F_TOPK-"]))

    # ── 로그 출력 ──
    try:
        while True:
            line = log_queue.get_nowait()
            if line in _DONE_TOKENS:
                # 작업 완료 → 상태 업데이트
                if _stop_event.is_set():
                    window["-STATUS-"].update("⛔ 중단됨", text_color="orange")
                else:
                    window["-STATUS-"].update("✅ 완료", text_color="lime")
            else:
                window["-LOG-"].update(line + "\n", append=True)
    except queue.Empty:
        pass

window.close()
# 
# 
# 
# 
# 추가사항. hdd 기기 이상으로 인한 오류로 확인되어 점검 진행중.
# 
# 01_Gui_dedup_pipe_2.4.py
# Mec_DB 통합 정리 시스템 v2.4
# ─────────────────────────────────────────────────────────────────────────────
# [v2.3 변경사항 — 버그 수정]
#
#  🐛 BUG 1 (치명) — fast_fingerprint 대량 실패 → index 극소 문제
#     원인: D:\ 루트 같은 넓은 경로를 스캔 시, 시스템·숨김 파일 등
#            PermissionError / OSError 가 대량 발생하면 fast_fingerprint 가
#            None 을 반환하고 file_index 에 저장되지 않음.
#            343,341개 처리 → index 3,495개 = 약 99% 실패.
#     수정:
#       - fast_fingerprint 에 상세 예외 캐치 추가 (errno 포함)
#       - run_folder_scan Step2 에서 오류 카운터·샘플 로그 표시
#         (처음 5개 오류 경로/원인 출력 + 총 오류수 요약)
#       - hashlib.sha1(usedforsecurity=False) → Python 3.8 이하 호환
#         (try/except 로 양쪽 지원)
#
#  🐛 BUG 2 (치명) — os.walk 깊이 제한 로직 오류 → 사실상 제한 안 됨
#     원인: depth = len(cur.relative_to(root_path).parts)
#            root 자체에서 depth=0, 1단계 자식은 depth=1 …
#            그런데 "if depth >= depth_limit: dirs[:] = []" 만 하고
#            continue 를 쓰면 cur 폴더 자체는 scan_dirs 에 추가됨.
#            depth_limit=12 면 사실상 무제한 탐색과 동일.
#            → 파일 343,341개가 모두 수집되었지만 거의 다 권한 없는
#              시스템 파일이어서 지문 실패.
#     수정:
#       - 깊이 초과 시 continue + dirs[:]=[] 순서 정정
#       - scan_dirs 에 깊이 초과 폴더를 추가하지 않음
#
#  🐛 BUG 3 (중간) — pending 재계산 시 체크포인트 key 불일치 위험
#     원인: Windows 경로는 대소문자 혼용. str(fp) 와 체크포인트의
#            key 가 슬래시 방향이 다를 경우 pending 에 중복 계산됨.
#     수정: file_index key 를 Path(k).as_posix() 로 정규화,
#           조회도 Path(fp).as_posix() 로 통일.
#
#  🐛 BUG 4 (경미) — BASE 경로 끝 \Runs 제거 로직이 run_folder_scan
#     내부에만 있어 run_dir 이중 Runs 가능성 잔존.
#     수정: base_path 정규화를 함수 진입 직후로 이동, \Runs 뿐 아니라
#           \runs (대소문자) 도 처리.
#
#  ✨ 개선 — Step2 진행률 표시 주기를 5,000개 → 10,000개로 조정
#            (저장 빈도는 유지, 로그 과다 출력 방지)
#  ✨ 개선 — 스캔 전 ROOT 존재 여부 확인 및 경고
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
import socket
from pathlib import Path
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed

import FreeSimpleGUI as sg

# ===== 설정 및 경로 관리 =====
SCRIPT_DIR  = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "gui_config.json"

_HOSTNAME = socket.gethostname().upper()[:10]

def _get_ckpt_dir() -> Path:
    candidates = []
    if sys.platform == "win32":
        local_app = os.environ.get("LOCALAPPDATA", "")
        if local_app:
            candidates.append(Path(local_app) / "MecDBDedup")
        candidates.append(Path("C:/Temp/MecDBDedup"))
        candidates.append(Path("C:/MecDBDedup"))
    candidates.append(SCRIPT_DIR / "_ckpt_cache")
    for c in candidates:
        try:
            c.mkdir(parents=True, exist_ok=True)
            test = c / ".write_test"
            test.write_text("ok")
            test.unlink()
            return c
        except Exception:
            continue
    return SCRIPT_DIR

CKPT_DIR = _get_ckpt_dir()

DEFAULT_ROOT = r"C:\Users\Meclaser\Desktop\Mec_DB"
DEFAULT_BASE = r"C:\elice\Dedup"

SCRIPT_01  = "01_Dedup_pipe_CI_2.7.py"
SCRIPT_02  = "02_Full_pipe_CI_2.7.py"
PYTHON_EXE = sys.executable


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

log_queue: "queue.Queue[str]" = queue.Queue()

def append_log(line: str):
    log_queue.put(line)


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
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except Exception as e:
        append_log(f"[FOLDER][WARN] 링크 생성 실패: {link_path} ({type(e).__name__}: {e})")


# ====== Union-Find ======

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
            if ra < rb:
                self._parent[rb] = ra
            else:
                self._parent[ra] = rb

    def groups(self) -> "dict[str, list]":
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


# ====== 폴더 해시 유사도 스캔 v2.3 ======

_CHUNK = 64 * 1024  # 64KB

# D:\ 루트 스캔 시 자동으로 건너뛸 시스템/프로그램 폴더 (소문자 비교)
SKIP_DIRS: "set[str]" = {
    # Windows 시스템
    "windows", "boot", "perflogs",
    "$winreagent", "$recycle.bin", "$windows.~bt", "$windows.~ws",
    "system volume information", "recovery",
    # 프로그램
    "program files", "program files (x86)", "programdata",
    "drivers", "intel", "kwic",
    # 기타
    "onedrivetemp", "mame32v0120",
}


def _sha1_new():
    """Python 3.8 이하 호환 SHA1 생성 (BUG FIX #1-a)"""
    try:
        return hashlib.sha1(usedforsecurity=False)
    except TypeError:
        return hashlib.sha1()


def fast_fingerprint(fp: Path):
    """
    파일 앞 64KB + 뒤 64KB + 파일크기 → SHA1
    반환: (hex_digest, size_bytes) 또는 (None, error_str)
    (BUG FIX #1: 예외 시 None 대신 (None, reason) 반환 → 오류 집계 가능)
    """
    try:
        sz = fp.stat().st_size
        h = _sha1_new()
        h.update(sz.to_bytes(8, "little"))
        with open(fp, "rb") as f:
            h.update(f.read(_CHUNK))
            if sz > _CHUNK * 2:
                f.seek(-_CHUNK, 2)
                h.update(f.read(_CHUNK))
        return h.hexdigest(), sz
    except PermissionError as e:
        return None, f"PermissionError({e.errno})"
    except OSError as e:
        return None, f"OSError({e.errno}): {e.strerror}"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _path_key(fp: "Path | str") -> str:
    """경로를 정규화된 소문자 문자열로 변환 (BUG FIX #3: 대소문자/슬래시 통일)"""
    return str(Path(fp)).lower()


def _save_ckpt(path: Path, data: dict):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        try:
            tmp.replace(path)
        except Exception:
            path.write_text(json.dumps(data), encoding="utf-8")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception:
        pass


def _save_ckpt_both(local: Path, backup: Path, data: dict):
    _save_ckpt(local, data)
    try:
        backup.parent.mkdir(parents=True, exist_ok=True)
        _save_ckpt(backup, data)
    except Exception:
        pass


def _load_ckpt(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _normalize_base(base_path: Path) -> Path:
    """BUG FIX #4: BASE 끝에 Runs 또는 runs 가 붙어있으면 제거"""
    if base_path.name.lower() == "runs":
        return base_path.parent
    return base_path


def run_folder_scan(root: str, base: str, depth_limit: int,
                    min_dir_mb: int, min_similarity: float, top_k: int):
    """
    폴더 유사도 스캔 v2.3
    BUG FIX 목록:
      #1 fast_fingerprint 대량 실패 → 오류 카운터 + 샘플 로그
      #2 os.walk 깊이 제한 로직 오류 수정
      #3 체크포인트 key 대소문자 정규화
      #4 BASE 경로 이중 Runs 완전 제거
    """
    _stop_event.clear()
    save_settings(root, base)
    t0 = time.time()

    root_path = Path(root.strip() or DEFAULT_ROOT)
    base_path = _normalize_base(Path(base.strip() or DEFAULT_BASE))  # BUG FIX #4

    # ── ROOT 존재 확인 (개선) ────────────────────────────────────────────
    if not root_path.exists():
        append_log(f"[ERROR][FOLDER] ROOT 경로가 존재하지 않습니다: {root_path}")
        append_log("__FOLDER_DONE__")
        return

    min_bytes = min_dir_mb * 1024 * 1024

    ckpt_hash = hashlib.md5(str(root_path).lower().encode()).hexdigest()[:8]
    ckpt_id   = f"{_HOSTNAME}_{ckpt_hash}"
    ckpt_path = CKPT_DIR / f"_ckpt_{ckpt_id}.json"
    ckpt_bak  = base_path / f"_ckpt_{ckpt_id}.json"

    append_log(f"[FOLDER] ▶ 스캔 시작: {root_path}")
    append_log(f"[FOLDER]   PC: {_HOSTNAME} | 깊이≤{depth_limit} | 최소 {min_dir_mb}MB"
               f" | 유사도≥{min_similarity}% | Top {top_k}")
    append_log(f"[FOLDER]   체크포인트(주): {ckpt_path}")
    append_log(f"[FOLDER]   체크포인트(백업): {ckpt_bak}")

    # ── Step 1: 파일 목록 수집 ───────────────────────────────────────────
    # BUG FIX #2: 깊이 제한 로직 정정
    #   이전: if depth >= depth_limit: dirs[:]=[] / continue
    #         → continue 위치가 잘못되어 cur 폴더가 scan_dirs 에 추가됨
    #   수정: 깊이 초과 폴더는 scan_dirs 에 추가하지 않고
    #         dirs[:]=[] 로 하위 탐색만 차단
    append_log("[1/4] 파일 목록 수집 중...")
    append_log(f"    깊이 제한: {depth_limit}단계 (root=0단계 기준)")

    scan_dirs: "set[Path]" = set()
    all_files: "list[Path]" = []
    last_report = time.time()

    def _walk_onerror(e):
        pass  # PermissionError 등 조용히 무시

    for cur_root, dirs, files in os.walk(root_path, topdown=True, onerror=_walk_onerror):
        if _stop_event.is_set():
            append_log("[1/4] ⛔ 중단 요청 — 파일 목록 수집 중단")
            append_log("__FOLDER_DONE__")
            return

        cur = Path(cur_root)
        try:
            depth = len(cur.relative_to(root_path).parts)
        except ValueError:
            depth = 0

        # 깊이 초과 시 skip
        if depth >= depth_limit:
            dirs[:] = []
            continue

        # ★ 시스템/프로그램 폴더 제외 (depth=1 에서만 적용)
        if depth == 0:
            before = len(dirs)
            dirs[:] = [d for d in dirs if d.lower() not in SKIP_DIRS]
            skipped = before - len(dirs)
            if skipped:
                append_log(f"    [제외] 시스템 폴더 {skipped}개 스킵 "
                           f"(SKIP_DIRS 목록 기준)")

        scan_dirs.add(cur)
        for fname in files:
            all_files.append(cur / fname)

        now = time.time()
        if now - last_report >= 60:
            elapsed = now - t0
            append_log(f"    [진행중] {elapsed/60:.1f}분 경과"
                       f" | 폴더 {len(scan_dirs):,}개"
                       f" | 파일 {len(all_files):,}개"
                       f" | 현재: ...{str(cur)[-60:]}")
            last_report = now

    if _stop_event.is_set():
        append_log("__FOLDER_DONE__")
        return

    append_log(f"[1/4] 완료 ({(time.time()-t0)/60:.1f}분)"
               f" | 폴더 {len(scan_dirs):,}개 | 파일 {len(all_files):,}개")

    if not all_files:
        append_log("[FOLDER] ⚠ 수집된 파일이 없습니다. ROOT 경로·깊이 설정을 확인하세요.")
        append_log("__FOLDER_DONE__")
        return

    # ── Step 2: 지문 계산 (BUG FIX #1 #3) ──────────────────────────────
    append_log("[2/4] 빠른 지문 계산 중 (앞뒤 64KB)...")

    # BUG FIX #3: key 를 _path_key() 로 정규화하여 로드
    raw_ckpt   = _load_ckpt(ckpt_path)
    file_index: dict = {_path_key(k): v for k, v in raw_ckpt.items()}

    if not file_index:
        raw_bak = _load_ckpt(ckpt_bak)
        if raw_bak:
            file_index = {_path_key(k): v for k, v in raw_bak.items()}
            append_log(f"    체크포인트 백업에서 로드 → {len(file_index):,}개")
            _save_ckpt(ckpt_path, file_index)
    else:
        append_log(f"    체크포인트(로컬) 발견 → {len(file_index):,}개 이어서 진행")

    # BUG FIX #3: pending 판단도 _path_key() 로 비교
    pending   = [fp for fp in all_files if _path_key(fp) not in file_index]
    n_pending = len(pending)
    n_cached  = len(all_files) - n_pending
    append_log(f"    캐시 히트 {n_cached:,}개 | 미처리 {n_pending:,}개 계산 시작 (스레드 6개)...")

    processed   = 0
    err_count   = 0
    err_samples: list = []   # BUG FIX #1: 오류 샘플 저장
    SAVE_EVERY  = 5_000
    LOG_EVERY   = 10_000     # 개선: 로그 출력 주기 조정

    with ThreadPoolExecutor(max_workers=6) as ex:
        fmap = {ex.submit(fast_fingerprint, fp): fp for fp in pending}
        for fut in as_completed(fmap):
            if _stop_event.is_set():
                ex.shutdown(wait=False, cancel_futures=True)
                _save_ckpt_both(ckpt_path, ckpt_bak, file_index)
                append_log(f"[2/4] ⛔ 중단 — 체크포인트 저장됨 ({len(file_index):,}개)")
                append_log("__FOLDER_DONE__")
                return

            res = fut.result()
            fp  = fmap[fut]

            # BUG FIX #1: (None, reason) 또는 (hex, sz) 두 가지 반환값 처리
            if isinstance(res, tuple) and len(res) == 2:
                h, sz_or_err = res
                if h is not None:
                    # 성공
                    file_index[_path_key(fp)] = [h, sz_or_err]
                else:
                    # 실패
                    err_count += 1
                    if len(err_samples) < 5:
                        err_samples.append(f"  {fp.name}: {sz_or_err}")
            else:
                # 예상치 못한 반환값 — 무시
                err_count += 1

            processed += 1

            # 저장
            if processed % SAVE_EVERY == 0:
                _save_ckpt_both(ckpt_path, ckpt_bak, file_index)

            # 로그
            if processed % LOG_EVERY == 0:
                elapsed = time.time() - t0
                rate    = processed / elapsed if elapsed > 0 else 1
                eta     = (n_pending - processed) / rate
                append_log(f"    {processed:,}/{n_pending:,}"
                           f" | 성공 {len(file_index):,} | 실패 {err_count:,}"
                           f" | 경과 {elapsed/60:.1f}분 | 잔여 약 {eta/60:.1f}분")

    _save_ckpt_both(ckpt_path, ckpt_bak, file_index)

    # BUG FIX #1: 오류 요약 출력
    if err_count > 0:
        err_pct = err_count / n_pending * 100 if n_pending else 0
        append_log(f"[2/4] ⚠ 지문 실패 {err_count:,}개 ({err_pct:.1f}%)")
        if err_samples:
            append_log("    실패 샘플 (최대 5개):")
            for s in err_samples:
                append_log(s)
        if err_pct > 50:
            append_log("    ※ 실패율이 50% 초과 → ROOT 경로에 접근 불가 파일이 많습니다.")
            append_log("      권장: ROOT 를 사용자 폴더(예: D:\\Users\\...)로 좁혀서 재시도")

    # 진단: ROOT 하위 / 외부 파일 수
    root_key   = _path_key(root_path)
    n_in_root  = sum(1 for k in file_index if k.startswith(root_key))
    n_external = len(file_index) - n_in_root
    append_log(f"[2/4] 완료 ({(time.time()-t0)/60:.1f}분)"
               f" | index 총 {len(file_index):,}개"
               f" (현재ROOT={n_in_root:,} / 외부={n_external:,}개)")
    if n_external > 0:
        append_log(f"    ※ 외부경로 {n_external:,}개는 Step3 집계에서 자동 제외됩니다")

    if n_in_root == 0:
        append_log("[FOLDER] ⛔ ROOT 내 유효 파일이 0개입니다. 권한 문제 또는 경로 확인 필요.")
        append_log("__FOLDER_DONE__")
        return

    # ── Step 3: 폴더별 재귀 해시셋 구성 ─────────────────────────────────
    append_log("[3/4] 폴더 해시셋 집계 중...")

    direct: "dict[Path, dict]" = {
        d: {"hashes": set(), "bytes": 0, "files": 0} for d in scan_dirs
    }

    for path_key_str, (h, sz) in file_index.items():
        # BUG FIX #3: key 가 정규화된 소문자이므로 Path 로 복원
        parent = Path(path_key_str).parent
        # scan_dirs 도 소문자 key 로 비교
        matched = None
        for d in direct:
            if _path_key(d) == _path_key(parent):
                matched = d
                break
        if matched is not None:
            direct[matched]["hashes"].add(h)
            direct[matched]["bytes"] += sz
            direct[matched]["files"] += 1

    n_matched = sum(len(v["hashes"]) for v in direct.values())
    append_log(f"[3/4] 직속 파일 집계: {n_matched:,}개 해시 매칭됨")

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
        append_log("[FOLDER] 비교할 후보 폴더가 2개 미만입니다.")
        append_log("    → 최소MB 를 낮추거나 ROOT 범위를 넓혀 보세요.")
        append_log("__FOLDER_DONE__")
        return

    # ── Step 4: 유사도 계산 + Union-Find 그룹핑 ──────────────────────────
    n_cands     = len(candidates)
    total_pairs = n_cands * (n_cands - 1) // 2
    append_log(f"[4/4] 유사도 계산 + 그룹핑 중... ({n_cands}개 폴더, {total_pairs:,}쌍)")

    uf       = UnionFind()
    pairs    = []
    compared = 0

    for a, b in combinations(candidates, 2):
        if _stop_event.is_set():
            append_log("[4/4] ⛔ 중단 요청 — 유사도 계산 중단")
            append_log("__FOLDER_DONE__")
            return

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
            append_log(f"    {compared:,}/{total_pairs:,}쌍 | 현재 {len(pairs)}쌍 발견")

    raw_groups = uf.groups()
    groups = [
        sorted(members)
        for members in raw_groups.values()
        if len(members) >= 2
    ]
    groups.sort(key=lambda g: len(g), reverse=True)

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

    append_log(f"[4/4] 완료 | {len(pairs)}쌍 / {len(groups)}그룹 발견 (≥{min_similarity}%)")

    if groups:
        summary = ", ".join(f"G{i}:{len(g)}폴더" for i, g in enumerate(groups[:10], 1))
        append_log(f"[4/4] 그룹 요약: {summary}" + (" ..." if len(groups) > 10 else ""))

    # ── 결과 저장 ─────────────────────────────────────────────────────────
    run_id     = time.strftime("%Y%m%d_%H%M")
    run_dir    = base_path / "Runs" / f"run_{run_id}_fol"
    review_dir = run_dir / "01_review_fol"
    review_dir.mkdir(parents=True, exist_ok=True)

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

    for gid, members in enumerate(groups[:top_k], 1):
        labels = [chr(ord("A") + i) for i in range(len(members))]
        folder_names = "__".join(
            safe_filename(Path(m).name, max_len=30) for m in members[:4]
        )
        suffix  = f"(+{len(members)-4}개)" if len(members) > 4 else ""
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
    append_log(f"[FOLDER] 체크포인트(주): {ckpt_path}")
    append_log(f"[FOLDER] 체크포인트(백업): {ckpt_bak}")
    append_log(f"[FOLDER] (다음 실행 시 지문 재계산 없이 바로 비교 진행)")
    append_log("__FOLDER_DONE__")


# ====== GUI 레이아웃 ======

sg.theme("DarkBlue3")
sg.set_options(font=("맑은 고딕", 10))

BTN_STOP = sg.Button("⛔ 중지", key="-STOP-",
                      button_color=("white", "#8B0000"), size=(10, 1))

_TAB_FONT_BOLD = ("맑은 고딕", 10, "bold")

layout_tab_01 = [
    [sg.Text("파일 내용/이름 기반 중복 스캔", key="-T01_TITLE-",
             font=_TAB_FONT_BOLD, text_color="cyan")],
    [sg.Text("모드:"),
     sg.Radio("DUP", "M1", key="-MODE_DUP-", default=True),
     sg.Radio("BIG", "M1", key="-MODE_BIG-"),
     sg.Text("  TOP N:"), sg.Input("50", size=(5, 1), key="-TOPN-"),
     sg.Push(),
     sg.Button("중복 스캔 실행", key="-RUN01-",
               button_color="firebrick", size=(18, 1))],
]

layout_tab_02 = [
    [sg.Text("대용량 파일 리뷰 및 정리", key="-T02_TITLE-",
             font=_TAB_FONT_BOLD, text_color="cyan")],
    [sg.Text("샘플링 수:"), sg.Input("10", size=(5, 1), key="-SAMPLEN-"),
     sg.Push(),
     sg.Button("최신 Run 실행",  key="-RUN02_LATEST-", size=(14, 1)),
     sg.Button("직접 지정 실행", key="-RUN02_MANUAL-", size=(14, 1))],
]

layout_tab_03 = [
    [sg.Text("내용물 유사 폴더 스캔 v2.4 (그룹핑)", key="-T03_TITLE-",
             font=_TAB_FONT_BOLD, text_color="cyan")],
    [sg.Text("깊이:"),       sg.Input("3",    size=(3, 1), key="-F_DEPTH-"),
     sg.Text(" 최소MB:"),    sg.Input("1000", size=(6, 1), key="-F_MINMB-"),
     sg.Text(" 유사도(%):"), sg.Input("85",   size=(4, 1), key="-F_MINSIM-"),
     sg.Text(" Top K:"),     sg.Input("20",   size=(4, 1), key="-F_TOPK-"),
     sg.Push(),
     sg.Button("폴더 유사도 스캔 시작", key="-RUN_FOL-",
               button_color="darkgreen", size=(20, 1))],
    [sg.Text("* 3개 이상 폴더는 자동으로 그룹(A-B-C...)으로 묶입니다",
             text_color="yellow", font=("맑은 고딕", 9))],
    [sg.Text("* D:\\ 등 루트 직접 지정 시 권한 오류 다수 → 하위 폴더 지정 권장",
             text_color="orange", font=("맑은 고딕", 9))],
]

layout = [
    [sg.Text("Mec_DB 통합 정리 시스템 v2.4",
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
        sg.Tab(" 01.Dedup ",    layout_tab_01),
        sg.Tab(" 02.BigFiles ", layout_tab_02),
        sg.Tab(" 03.Folder ",   layout_tab_03),
    ]], key="-TABS-", expand_x=True,
       tab_background_color="#2B4F7E",
       selected_background_color="#1A2B4A",
       selected_title_color="cyan",
       title_color="#AAAAAA",
    )],
    [sg.Text("대기 중", key="-STATUS-", text_color="lime",
             font=("맑은 고딕", 9), expand_x=True),
     BTN_STOP],
    [sg.Multiline(size=(95, 20), key="-LOG-",
                  autoscroll=True, disabled=True,
                  font=("Consolas", 9),
                  expand_x=True, expand_y=True)],
    [sg.Button("로그 지우기", key="-CLEAR-", size=(12, 1)),
     sg.Push(),
     sg.Button("프로그램 종료", key="-EXIT-", size=(14, 1))]
]

window = sg.Window(
    "Mec_DB Cleaner v2.4",
    layout,
    resizable=True,
    finalize=True
)
window.set_min_size((700, 500))


# ====== 스레드 헬퍼 ======
current_thread: "threading.Thread | None" = None
_start_time_str: str = ""

_DONE_TOKENS = {"__STEP01_DONE__", "__STEP02_DONE__", "__FOLDER_DONE__"}

def start_thread(target, *args):
    global current_thread, _start_time_str
    if current_thread and current_thread.is_alive():
        sg.popup("이미 작업이 실행 중입니다.\n중지하려면 ⛔ 중지 버튼을 눌러주세요.")
        return
    _stop_event.clear()
    window["-LOG-"].update("")
    _start_time_str = time.strftime("%H:%M:%S")
    window["-STATUS-"].update(f"🔄 실행 중...  (시작 {_start_time_str})", text_color="yellow")
    current_thread = threading.Thread(target=target, args=args, daemon=True)
    current_thread.start()


# ====== 메인 이벤트 루프 ======
while True:
    event, values = window.read(timeout=100)

    if event in (sg.WIN_CLOSED, "-EXIT-"):
        _stop_event.set()
        break

    if event == "-TABS-":
        tab = values.get("-TABS-", "")
        window["-T01_TITLE-"].update(
            text_color="cyan" if " 01.Dedup "    in tab else "#888888")
        window["-T02_TITLE-"].update(
            text_color="cyan" if " 02.BigFiles " in tab else "#888888")
        window["-T03_TITLE-"].update(
            text_color="cyan" if " 03.Folder "   in tab else "#888888")

    elif event == "-STOP-":
        if current_thread and current_thread.is_alive():
            _stop_event.set()
            append_log("⛔ 중지 요청됨 — 현재 작업 완료 후 종료됩니다...")
            window["-STATUS-"].update("⛔ 중지 요청됨...", text_color="orange")
        else:
            sg.popup("현재 실행 중인 작업이 없습니다.")

    elif event == "-CLEAR-":
        window["-LOG-"].update("")

    elif event == "-RUN01-":
        mode = "DUP" if values["-MODE_DUP-"] else "BIGFILE"
        start_thread(run_step01,
                     values["-ROOT-"], values["-BASE-"],
                     mode, int(values["-TOPN-"]))

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

    elif event == "-RUN_FOL-":
        start_thread(run_folder_scan,
                     values["-ROOT-"], values["-BASE-"],
                     int(values["-F_DEPTH-"]),
                     int(values["-F_MINMB-"]),
                     float(values["-F_MINSIM-"]),
                     int(values["-F_TOPK-"]))

    try:
        while True:
            line = log_queue.get_nowait()
            if line in _DONE_TOKENS:
                end_time = time.strftime("%H:%M:%S")
                if _stop_event.is_set():
                    window["-STATUS-"].update(
                        f"⛔ 중단됨  ({_start_time_str} → {end_time})",
                        text_color="orange")
                else:
                    window["-STATUS-"].update(
                        f"✅ 완료  ({_start_time_str} → {end_time})",
                        text_color="lime")
            else:
                window["-LOG-"].update(line + "\n", append=True)
    except queue.Empty:
        pass

window.close()

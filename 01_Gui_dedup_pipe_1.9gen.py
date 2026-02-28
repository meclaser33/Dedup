# 01_Gui_dedup_pipe_1.9.py
# - 01_Dedup_pipe_CI_2.7.py / 02_Full_pipe_CI_2.7.py 래퍼 + 폴더 유사도 스캔(FOLDER) 통합 버전

# --- Windows DPI 설정 (블러 방지) ---
import sys
if sys.platform == "win32":
    import ctypes
    try:
        # 1 = SYSTEM_DPI_AWARE, 2 = PER_MONITOR_DPI_AWARE
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
from pathlib import Path
from itertools import combinations
from difflib import SequenceMatcher
from blake3 import blake3  # <--- 제미나이 : 이 줄을 추가하세요!

import FreeSimpleGUI as sg  # Free 버전


# ===== 설정값 =====
DEFAULT_ROOT = r"C:\Users\Meclaser\Desktop\Mec_DB"
DEFAULT_BASE = r"C:\elice\Dedup"

SCRIPT_01 = "01_Dedup_pipe_CI_2.7.py"   # 01 스크립트 파일명
SCRIPT_02 = "02_Full_pipe_CI_2.7.py"    # 02 스크립트 파일명

PYTHON_EXE = sys.executable             # 현재 venv / 파이썬 그대로 사용
SCRIPT_DIR = Path(__file__).resolve().parent

# 로그 전달용 큐
log_queue: "queue.Queue[str]" = queue.Queue()


def append_log(line: str):
    """백그라운드 스레드에서 GUI로 로그 넘길 때 사용."""
    log_queue.put(line)


# ====== 유틸 ======

def find_latest_run_dir(base_path: str) -> Path | None:
    """
    BASE\\Runs 아래에서 가장 최근에 수정된 run 폴더를 찾는다.
    없으면 None 리턴.
    """
    runs_root = Path(base_path) / "Runs"
    if not runs_root.exists():
        return None

    candidates = [d for d in runs_root.iterdir() if d.is_dir() and d.name.startswith("run_")]
    if not candidates:
        return None

    # 수정시간(st_mtime) 기준으로 가장 최근
    latest = max(candidates, key=lambda d: d.stat().st_mtime)
    return latest


def safe_filename(s: str, max_len: int = 160) -> str:
    s = "".join("_" if ch in '<>:"/\\|?*\x00' else ch for ch in s)
    s = " ".join(s.split()).strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s or "NONAME"


def create_dir_shortcut(link_path: Path, target_dir: Path):
    """
    폴더를 여는 .lnk 생성 (타깃은 폴더 경로 그대로).
    WScript.Shell COM + 파워쉘 사용.
    """
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
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except Exception as e:
        append_log(f"[FOLDER][WARN] 링크 생성 실패: {link_path} ({type(e).__name__}: {e})")


# ====== 백그라운드 작업들 ======

def run_step01(root: str, base: str, mode: str, top_n: int):
    """
    01_Dedup_pipe_CI_2.7.py 를 서브프로세스로 실행하고
    필요한 입력을 stdin으로 밀어 넣는다.
    """
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
            text=True,
            cwd=cwd,
            bufsize=1
        )

        answers = [
            root.strip(),                   # ROOT
            base.strip(),                   # BASE
            "1" if mode == "DUP" else "2",  # MODE
            str(top_n),                     # TOP_N
        ]

        for ans in answers:
            proc.stdin.write(ans + "\n")
            proc.stdin.flush()

        proc.stdin.close()

        for line in proc.stdout:
            append_log(line.rstrip("\n"))

        rc = proc.wait()
        append_log(f"[STEP 01] 종료 (returncode={rc})")

    except Exception as e:
        append_log(f"[ERROR][STEP 01] {type(e).__name__}: {e}")

    finally:
        append_log("__STEP01_DONE__")


def run_step02(base: str, run_dir: str | None, sample_n: int):
    """
    02_Full_pipe_CI_2.7.py 실행.
    run_dir 가 None이면 BASE\\Runs 에서 최신 run 자동 선택.
    """
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
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=cwd,
            bufsize=1
        )

        for line in proc.stdout:
            append_log(line.rstrip("\n"))

        rc = proc.wait()
        append_log(f"[STEP 02] 종료 (returncode={rc})")

    except Exception as e:
        append_log(f"[ERROR][STEP 02] {type(e).__name__}: {e}")

    finally:
        append_log("__STEP02_DONE__")


# 제미나이 : 130행 부근: run_folder_scan 함수 바로 위에 삽입
def get_folder_manifest(folder_path):
    manifest = {}
    try:
        for root, _, files in os.walk(folder_path):
            for file in files:
                fp = Path(root) / file
                if not fp.is_file(): continue
                try:
                    hasher = blake3()
                    with open(fp, "rb") as f:
                        while chunk := f.read(1024 * 1024):
                            hasher.update(chunk)
                    manifest[hasher.hexdigest()] = fp.stat().st_size
                except: continue
    except: pass
    return manifest

def calculate_folder_similarity(m1, m2):
    set1, set2 = set(m1.keys()), set(m2.keys())
    intersection = set1 & set2
    union = set1 | set2
    if not union: return 0.0, 0.0
    count_sim = (len(intersection) / len(union)) * 100
    all_dict = {**m1, **m2}
    total_size = sum(all_dict[h] for h in union)
    size_sim = (sum(all_dict[h] for h in intersection) / total_size * 100) if total_size > 0 else 0
    return count_sim, size_sim


# 제미나이 : ====== 폴더 해시 유사도 스캔 (FOLDER MODE) ======

def run_folder_scan(root: str, base: str, depth_limit: int, min_dir_mb: int, min_similarity: float, top_k: int):
    start_t = time.time()
    try:
        root_path, base_path = Path(root.strip() or DEFAULT_ROOT), Path(base.strip() or DEFAULT_BASE)
        append_log(f"[FOLDER] 지문 기반 스캔 시작: {root_path}")

        # 1. 대상 폴더 수집 및 지문(Manifest) 추출
        dirs_info = []
        for cur_root, dirs, _ in os.walk(root_path):
            cur_path = Path(cur_root)
            depth = len(cur_path.relative_to(root_path).parts)
            if depth >= depth_limit:
                dirs[:] = [] # 설정된 깊이보다 깊은 곳은 스캔 안 함
            
            # 2단계에서 만든 함수로 폴더 내 파일 지문들을 수집
            manifest = get_folder_manifest(cur_path)
            total_bytes = sum(manifest.values())
            
            if total_bytes >= (min_dir_mb * 1024 * 1024):
                dirs_info.append({
                    "path": cur_path, "manifest": manifest, 
                    "bytes": total_bytes, "files": len(manifest), "depth": depth
                })
                append_log(f"[SCAN] 후보 발견: {cur_path.name} ({len(manifest)}개 파일)")

        if len(dirs_info) < 2:
            append_log("[FOLDER] 비교할 후보 폴더가 부족합니다.")
            return

        # 2. 유사도 계산 (개수/용량 병행)
        pairs = []
        for a, b in combinations(dirs_info, 2):
            if a["path"] in b["path"].parents or b["path"] in a["path"].parents:
                continue # 부모-자식 폴더 관계는 제외
            
            # 2단계에서 만든 유사도 계산 함수 호출
            c_sim, s_sim = calculate_folder_similarity(a["manifest"], b["manifest"])
            final_score = max(c_sim, s_sim) # 두 기준 중 높은 점수 채택
            
            if final_score >= min_similarity:
                pairs.append({
                    "dir1": a["path"], "dir2": b["path"], "score": final_score,
                    "bytes1": a["bytes"], "bytes2": b["bytes"], "files1": a["files"], "files2": b["files"],
                    "c_sim": c_sim, "s_sim": s_sim # 상세 점수 기록
                })

        # 3. 결과 정렬 및 저장 (이 부분은 기존 02번 리뷰 앱과 호환을 위해 유지)
        pairs.sort(key=lambda x: x["score"], reverse=True)
        top_pairs = pairs[:top_k]

        run_id = time.strftime("%Y%m%d_%H%M")
        run_dir = base_path / "Runs" / f"run_{run_id}_fol"
        review_dir = run_dir / "01_review_fol"
        review_dir.mkdir(parents=True, exist_ok=True)

        # CSV 저장 (02번 앱이 읽는 형식 유지)
        csv_path = run_dir / "folder_similarity_pairs.csv"
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["score", "dir1", "dir2", "bytes1", "bytes2", "files1", "files2", "c_sim", "s_sim"])
            for p in top_pairs:
                w.writerow([f"{p['score']:.2f}", str(p['dir1']), str(p['dir2']), p['bytes1'], p['bytes2'], p['files1'], p['files2'], f"{p['c_sim']:.2f}", f"{p['s_sim']:.2f}"])

        # .lnk 바로가기 생성 (리뷰 편의성)
        for idx, p in enumerate(top_pairs, start=1):
            grp_name = f"{idx:02d}_{int(p['score'])}pct__{safe_filename(p['dir1'].name)}__{safe_filename(p['dir2'].name)}"
            pair_dir = review_dir / grp_name
            pair_dir.mkdir(parents=True, exist_ok=True)
            create_dir_shortcut(pair_dir / "01_원본폴더", p["dir1"])
            create_dir_shortcut(pair_dir / "02_대상폴더", p["dir2"])

        append_log(f"[FOLDER] 완료! {len(top_pairs)}개의 유사 그룹이 {run_dir}에 생성되었습니다.")
    except Exception as e:
        append_log(f"[FOLDER][ERROR] {e}")

# 제미나이 : --- 탭별 내용 정의 (기존 코드를 조각내서 변수에 담는 과정입니다) ---

# 1번 서랍: 중복 파일 찾기 (Dedup)
tab_dedup_layout = [
    [sg.Text("파일 내용이나 이름이 같은 중복 파일을 스캔합니다.")],
    [sg.Text("TOP N 그룹:"), sg.Input("50", size=(6, 1), key="-TOPN-")],
    [sg.Button("중복 파일 스캔 시작", key="-RUN_DEDUP-", button_color=("white", "firebrick"))]
]

# 2번 서랍: 큰 파일 찾기 (BigFiles)
tab_big_layout = [
    [sg.Text("용량이 큰 순서대로 파일을 나열하여 정리합니다.")],
    [sg.Text("추출할 파일 수:"), sg.Input("100", size=(6, 1), key="-TOPN_BIG-")],
    [sg.Button("대용량 파일 스캔 시작", key="-RUN_BIG-", button_color=("white", "navy"))]
]

# 3번 서랍: 폴더 유사도 (Folder) - 우리가 새로 만든 지문 방식
tab_folder_layout = [
    [sg.Text("내용물이 85% 이상 일치하는 폴더 쌍을 찾습니다.")],
    [sg.Text("최대 깊이:", size=(8,1)), sg.Input("4", size=(4,1), key="-F_DEPTH-"),
     sg.Text("최소 용량(MB):", size=(12,1)), sg.Input("500", size=(6,1), key="-F_MINMB-")],
    [sg.Text("최소 유사도(%):", size=(12,1)), sg.Input("85", size=(4,1), key="-F_MINSIM-"),
     sg.Text("Top K:", size=(7,1)), sg.Input("20", size=(4,1), key="-F_TOPK-")],
    [sg.Button("폴더 유사도 스캔 시작", key="-RUN_FOL-", button_color=("white", "darkgreen"))]
]


# 제미나이 : ====== 4단계: 신규 레이아웃 정의 (기존 90행을 대체함) ======
# ==========================================================
# 여기서부터 파일 끝까지 덮어쓰기 하세요
# ==========================================================

sg.theme("DarkBlue3")
sg.set_options(font=("맑은 고딕", 10))

# --- 탭 01: Dedup (기존 로직) ---
layout_tab_01 = [
    [sg.Text("파일 내용/이름 기반 중복 스캔", font=("맑은 고딕", 10, "bold"))],
    [sg.Text("모드:"), 
     sg.Radio("DUP", "M1", key="-MODE_DUP-", default=True), 
     sg.Radio("BIG", "M1", key="-MODE_BIG-")],
    [sg.Text("TOP N:"), sg.Input("50", size=(5, 1), key="-TOPN-")],
    [sg.Button("중복 스캔 실행", key="-RUN01-", button_color="firebrick", size=(20, 1))]
]

# --- 탭 02: BigFiles (기존 로직) ---
layout_tab_02 = [
    [sg.Text("대용량 파일 리뷰 및 정리", font=("맑은 고딕", 10, "bold"))],
    [sg.Text("샘플링 수:"), sg.Input("10", size=(5, 1), key="-SAMPLEN-")],
    [sg.Button("최신 Run 실행", key="-RUN02_LATEST-", size=(15, 1)),
     sg.Button("직접 지정 실행", key="-RUN02_MANUAL-", size=(15, 1))]
]

# --- 탭 03: Folder (신규 BLAKE3 지문 로직) ---
layout_tab_03 = [
    [sg.Text("내용물 85% 일치 폴더 스캔", font=("맑은 고딕", 10, "bold"))],
    [sg.Text("깊이:"), sg.Input("4", size=(3, 1), key="-F_DEPTH-"), 
     sg.Text("최소MB:"), sg.Input("500", size=(5, 1), key="-F_MINMB-")],
    [sg.Text("유사도(%):"), sg.Input("85", size=(3, 1), key="-F_MINSIM-"), 
     sg.Text("Top K:"), sg.Input("20", size=(3, 1), key="-F_TOPK-")],
    [sg.Button("폴더 유사도 스캔 시작", key="-RUN_FOL-", button_color="darkgreen", size=(25, 1))]
]

# --- 전체 레이아웃 구성 ---
layout = [
    [sg.Text("Mec_DB 통합 정리 시스템 v2.0", font=("Malgun Gothic", 16, "bold"), text_color="cyan")],
    [sg.Frame("공통 경로 설정", [
        [sg.Text("ROOT:"), sg.Input(DEFAULT_ROOT, key="-ROOT-", size=(45,1)), sg.FolderBrowse("찾기")],
        [sg.Text("BASE:"), sg.Input(DEFAULT_BASE, key="-BASE-", size=(45,1)), sg.FolderBrowse("찾기")]
    ])],
    [sg.TabGroup([[
        sg.Tab(" 01.Dedup ", layout_tab_01),
        sg.Tab(" 02.BigFiles ", layout_tab_02),
        sg.Tab(" 03.Folder ", layout_tab_03),
    ]], key="-TABS-")],
    [sg.Multiline(size=(90, 15), key="-LOG-", autoscroll=True, disabled=True, font=("Consolas", 9))],
    [sg.Button("프로그램 종료", key="-EXIT-")]
]

window = sg.Window("Mec_DB Cleaner v2.0", layout, finalize=True)

# ====== [원본 헬퍼 & 스레드 로직 보존] ======
current_thread = None

def start_thread(target, *args):
    global current_thread
    if current_thread and current_thread.is_alive():
        sg.popup("이미 작업이 실행 중입니다.")
        return
    window["-LOG-"].update("")
    current_thread = threading.Thread(target=target, args=args, daemon=True)
    current_thread.start()

# ====== [메인 이벤트 루프] ======
while True:
    event, values = window.read(timeout=100)
    if event in (sg.WIN_CLOSED, "-EXIT-"): break

    # 1번 탭 이벤트
    if event == "-RUN01-":
        mode = "DUP" if values["-MODE_DUP-"] else "BIGFILE"
        start_thread(run_step01, values["-ROOT-"], values["-BASE-"], mode, int(values["-TOPN-"]))

    # 2번 탭 이벤트
    elif event == "-RUN02_LATEST-":
        start_thread(run_step02, values["-BASE-"], None, int(values["-SAMPLEN-"]))

    # 3번 탭 이벤트 (신규 폴더 스캔 연결!)
    elif event == "-RUN_FOL-":
        start_thread(run_folder_scan, values["-ROOT-"], values["-BASE-"], 
                     int(values["-F_DEPTH-"]), int(values["-F_MINMB-"]), 
                     float(values["-F_MINSIM-"]), int(values["-F_TOPK-"]))

    # 로그 출력 처리 (기존 로직 유지)
    try:
        while True:
            line = log_queue.get_nowait()
            window["-LOG-"].update(line + "\n", append=True)
    except queue.Empty:
        pass

window.close()

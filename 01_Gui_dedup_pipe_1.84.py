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


# ====== 폴더 유사도 스캔 (FOLDER MODE) ======

def run_folder_scan(root: str,
                    base: str,
                    depth_limit: int,
                    min_dir_mb: int,
                    min_similarity: float,
                    top_k: int):
    """
    ROOT 아래 디렉터리들을 스캔해서
      - depth_limit 층위까지만
      - min_dir_mb MB 이상인 디렉터리만 후보
    로 모은 뒤, 폴더 유사도 점수를 계산해
    상위 top_k 쌍에 대해 fol 전용 run / review 폴더 + .lnk 생성.
    """
    start_t = time.time()
    try:
        root = root.strip() or DEFAULT_ROOT
        base = base.strip() or DEFAULT_BASE
        root_path = Path(root)
        base_path = Path(base)

        append_log(f"[FOLDER] ROOT = {root_path}")
        append_log(f"[FOLDER] BASE = {base_path}")
        append_log(
            f"[FOLDER] depth_limit={depth_limit}, "
            f"min_dir_mb={min_dir_mb}, min_similarity={min_similarity:.1f}%, top_k={top_k}"
        )

        if not root_path.is_dir():
            append_log(f"[FOLDER][ERROR] ROOT 폴더가 아닙니다: {root_path}")
            return

        min_bytes = max(0, min_dir_mb) * 1024 * 1024

        # 1) 디렉터리 스캔 (depth_limit 적용)
        dirs_info = []
        root_parts = len(root_path.parts)

        for cur_root, dirs, files in os.walk(root_path):
            cur_path = Path(cur_root)

            try:
                rel = cur_path.relative_to(root_path)
                depth = len(rel.parts)  # ROOT 자체는 depth=0, 하위는 1,2,...
            except ValueError:
                # 이론상 안 나와야 하지만 방어
                depth = 0

            # depth_limit 초과면 더 내려가지 않음
            if depth >= depth_limit:
                dirs[:] = []

            # 현재 디렉터리의 파일 용량 / 개수 집계
            total_bytes = 0
            file_count = 0
            for name in files:
                fp = cur_path / name
                try:
                    sz = fp.stat().st_size
                except OSError:
                    continue
                total_bytes += sz
                file_count += 1

            # 용량 기준 필터
            if total_bytes >= min_bytes:
                dirs_info.append(
                    {
                        "path": cur_path,
                        "bytes": total_bytes,
                        "files": file_count,
                        "depth": depth,  # ROOT=0, 자식=1 ...
                    }
                )

        append_log(f"[FOLDER] 디렉터리 수 (ROOT 포함): {len(set(d['path'] for d in dirs_info))}")
        append_log(f"[FOLDER] 후보 폴더 수 (min_dir_mb 필터 후): {len(dirs_info)}")

        if len(dirs_info) < 2:
            append_log("[FOLDER] 후보 폴더가 2개 미만입니다. 비교할 수 없습니다.")
            return

        # 2) 유사도 계산
        pairs = []

        for a, b in combinations(dirs_info, 2):
            pa = a["path"]
            pb = b["path"]

            # 같은 경로이거나, 상하위(조상/자식) 관계면 스킵
            if pa == pb:
                continue
            try:
                if pa in pb.parents or pb in pa.parents:
                    continue
            except Exception:
                pass

            bytes_a, bytes_b = a["bytes"], b["bytes"]
            files_a, files_b = a["files"], b["files"]

            if bytes_a == 0 or bytes_b == 0:
                continue  # 용량 0인 폴더끼리는 스킵

            # --- 1) 용량 유사도 ---
            size_ratio = min(bytes_a, bytes_b) / max(bytes_a, bytes_b)

            # --- 2) 파일 개수 유사도 ---
            if files_a == 0 and files_b == 0:
                file_ratio = 1.0
            elif files_a == 0 or files_b == 0:
                file_ratio = 0.0
            else:
                file_ratio = min(files_a, files_b) / max(files_a, files_b)

            # --- 3) 이름 유사도 ---
            name_a = pa.name or ""
            name_b = pb.name or ""
            if name_a and name_b:
                name_sim = SequenceMatcher(None, name_a, name_b).ratio()
            else:
                name_sim = 0.0

            # --- 기본 점수 (0~1) ---
            base_sim = 0.70 * size_ratio + 0.25 * file_ratio + 0.05 * name_sim

            # --- 깊이 패널티 ---
            depth_diff = abs(a["depth"] - b["depth"])
            depth_factor = max(0.0, 1.0 - 0.05 * depth_diff)  # 1단계 차이당 -5%
            adjusted = base_sim * depth_factor

            final_score = adjusted * 100.0
            if final_score < min_similarity:
                continue

            pairs.append(
                {
                    "dir1": pa,
                    "dir2": pb,
                    "bytes1": bytes_a,
                    "bytes2": bytes_b,
                    "files1": files_a,
                    "files2": files_b,
                    "depth1": a["depth"],
                    "depth2": b["depth"],
                    "size_ratio": size_ratio,
                    "file_ratio": file_ratio,
                    "name_sim": name_sim,
                    "depth_factor": depth_factor,
                    "score": final_score,
                }
            )

        if not pairs:
            append_log("[FOLDER] min_similarity 조건을 만족하는 폴더 쌍이 없습니다.")
            return

        # 점수 순 정렬
        pairs.sort(key=lambda x: x["score"], reverse=True)
        top_pairs = pairs[:top_k]
        append_log(f"[FOLDER] 유사 폴더 쌍 개수: {len(pairs)}")

        # 3) run_fol / review_fol 및 CSV / 링크 생성
        runs_root = base_path / "Runs"
        runs_root.mkdir(parents=True, exist_ok=True)

        run_id = time.strftime("%Y%m%d_%H%M")
        run_dir = runs_root / f"run_{run_id}_fol"
        run_dir.mkdir(parents=True, exist_ok=True)

        review_dir = run_dir / "01_review_fol"
        review_dir.mkdir(parents=True, exist_ok=True)

        csv_path = run_dir / "folder_similarity_pairs.csv"
        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "score",
                    "dir1",
                    "dir2",
                    "bytes1",
                    "bytes2",
                    "files1",
                    "files2",
                    "depth1",
                    "depth2",
                    "size_ratio",
                    "file_ratio",
                    "name_sim",
                    "depth_factor",
                ]
            )
            for p in top_pairs:
                w.writerow(
                    [
                        f"{p['score']:.2f}",
                        str(p["dir1"]),
                        str(p["dir2"]),
                        p["bytes1"],
                        p["bytes2"],
                        p["files1"],
                        p["files2"],
                        p["depth1"],
                        p["depth2"],
                        f"{p['size_ratio']:.4f}",
                        f"{p['file_ratio']:.4f}",
                        f"{p['name_sim']:.4f}",
                        f"{p['depth_factor']:.4f}",
                    ]
                )

        # 페어별 리뷰 폴더 + .lnk 생성
        for idx, p in enumerate(top_pairs, start=1):
            score_int = int(round(p["score"]))
            base1 = p["dir1"].name or "ROOT"
            base2 = p["dir2"].name or "ROOT"
            grp_name = f"{idx:02d}_{score_int:02d}pct__{safe_filename(base1)}__{safe_filename(base2)}"
            pair_dir = review_dir / grp_name
            pair_dir.mkdir(parents=True, exist_ok=True)

            # 두 폴더를 여는 링크
            create_dir_shortcut(pair_dir / "01_dirA", p["dir1"])
            create_dir_shortcut(pair_dir / "02_dirB", p["dir2"])

        append_log(f"[FOLDER] run 폴더: {run_dir}")
        append_log(f"[FOLDER] review 폴더: {review_dir}")
        append_log(f"[FOLDER] CSV 저장: {csv_path}")
        elapsed = time.time() - start_t
        append_log(f"[FOLDER] 완료 (elapsed={elapsed:.1f}s)")

    except Exception as e:
        append_log(f"[FOLDER][ERROR] {type(e).__name__}: {e}")


# ====== GUI 레이아웃 ======

sg.theme("DarkBlue3")
sg.set_options(font=("맑은 고딕", 10))   # 또는 ("Segoe UI", 10)

layout = [
    [sg.Text("중복파일 / 폴더 유사도 파이프라인 GUI", font=("Malgun Gothic", 14, "bold"))],

    # ROOT / BASE 선택
    [
        sg.Text("ROOT (대상 폴더)", size=(16, 1)),
        sg.Input(DEFAULT_ROOT, size=(60, 1), key="-ROOT-"),
        sg.FolderBrowse("찾기", target="-ROOT-"),
    ],
    [
        sg.Text("BASE (작업 폴더)", size=(16, 1)),
        sg.Input(DEFAULT_BASE, size=(60, 1), key="-BASE-"),
        sg.FolderBrowse("찾기", target="-BASE-"),
    ],

    [sg.HSeparator()],

    # STEP 01 영역
    [sg.Text("[STEP 01] 01_Dedup_pipe 실행 (파일 중복 DUP/BIGFILE)", font=("Malgun Gothic", 11, "bold"))],
    [
        sg.Text("모드:", size=(6, 1)),
        sg.Radio("DUP", "MODE01", key="-MODE_DUP-", default=True),
        sg.Radio("BIGFILE", "MODE01", key="-MODE_BIG-"),
        sg.Text("TOP N 그룹:", size=(10, 1)),
        sg.Input("50", size=(6, 1), key="-TOPN-"),
        sg.Button("STEP 01 실행", key="-RUN01-"),
    ],

    [sg.HSeparator()],

    # STEP 02 영역
    [sg.Text("[STEP 02] 02_Full_pipe 실행 (quarantine + removed_originals)", font=("Malgun Gothic", 11, "bold"))],
    [
        sg.Text("최근 run (자동 탐색):", size=(16, 1)),
        sg.Text("", size=(60, 1), key="-LATEST_RUN-"),
        sg.Button("새로고침", key="-REFRESH_RUN-"),
    ],
    [
        sg.Text("직접 RunDir 지정:", size=(16, 1)),
        sg.Input("", size=(60, 1), key="-RUNDIR_INPUT-"),
        sg.FolderBrowse("찾기", target="-RUNDIR_INPUT-"),
    ],
    [
        sg.Text("SampleN:", size=(16, 1)),
        sg.Input("10", size=(6, 1), key="-SAMPLEN-"),
        sg.Button("02 실행 (최근 run)", key="-RUN02_LATEST-"),
        sg.Button("02 실행 (위 RunDir)", key="-RUN02_MANUAL-"),
    ],

    [sg.HSeparator()],

    # FOLDER 영역
    [sg.Text("[FOLDER] 폴더 유사도 스캔 (용량/파일수/이름 기반)", font=("Malgun Gothic", 11, "bold"))],
    [
        sg.Text("최대 깊이:", size=(8, 1)),
        sg.Input("4", size=(4, 1), key="-F_DEPTH-"),
        sg.Text("최소 폴더 용량(MB):", size=(16, 1)),
        sg.Input("500", size=(6, 1), key="-F_MINMB-"),
        sg.Text("최소 유사도(%):", size=(13, 1)),
        sg.Input("85", size=(4, 1), key="-F_MINSIM-"),
        sg.Text("Top K:", size=(7, 1)),
        sg.Input("20", size=(4, 1), key="-F_TOPK-"),
        sg.Button("폴더 유사도 스캔", key="-RUN_FOL-"),
    ],

    [sg.HSeparator()],

    [sg.Text("로그 출력", font=("Malgun Gothic", 11, "bold"))],
    [
        sg.Multiline(
            size=(120, 25),
            key="-LOG-",
            autoscroll=True,
            disabled=True,
            font=("Consolas", 9),
        )
    ],

    [sg.Button("종료", key="-EXIT-")],
]

window = sg.Window("Dedup / Folder Similarity GUI 1.9", layout, finalize=True)


# ====== 헬퍼: 최신 run 표시 ======

def update_latest_run_label():
    base = window["-BASE-"].get().strip() or DEFAULT_BASE
    latest = find_latest_run_dir(base)
    if latest is None:
        window["-LATEST_RUN-"].update("없음 (BASE\\Runs 안에 run_* 폴더 없음)")
    else:
        window["-LATEST_RUN-"].update(str(latest))


update_latest_run_label()

# 현재 실행 중인 작업 스레드 (하나만 허용)
current_thread: threading.Thread | None = None


def start_thread(target, *args):
    global current_thread
    if current_thread is not None and current_thread.is_alive():
        sg.popup("이미 실행 중인 작업이 있습니다.", title="알림")
        return
    window["-LOG-"].update("")  # 새 작업 시작할 때 로그 비우기
    current_thread = threading.Thread(target=target, args=args, daemon=True)
    current_thread.start()


# ====== 메인 이벤트 루프 ======

while True:
    event, values = window.read(timeout=100)  # 100ms마다 큐 체크

    if event in (sg.WIN_CLOSED, "-EXIT-"):
        break

    if event == "-REFRESH_RUN-":
        update_latest_run_label()

    elif event == "-RUN01-":
        root = values["-ROOT-"].strip() or DEFAULT_ROOT
        base = values["-BASE-"].strip() or DEFAULT_BASE

        mode = "DUP" if values["-MODE_DUP-"] else "BIGFILE"

        try:
            top_n = int(values["-TOPN-"])
            if top_n <= 0:
                raise ValueError
        except ValueError:
            sg.popup("TOP N 은 1 이상의 정수여야 합니다.", title="입력 오류")
            continue

        start_thread(run_step01, root, base, mode, top_n)

    elif event == "-RUN02_LATEST-":
        base = values["-BASE-"].strip() or DEFAULT_BASE
        try:
            sample_n = int(values["-SAMPLEN-"])
            if sample_n <= 0:
                raise ValueError
        except ValueError:
            sg.popup("SampleN 은 1 이상의 정수여야 합니다.", title="입력 오류")
            continue

        start_thread(run_step02, base, None, sample_n)

    elif event == "-RUN02_MANUAL-":
        base = values["-BASE-"].strip() or DEFAULT_BASE
        run_dir = values["-RUNDIR_INPUT-"].strip()
        if not run_dir:
            sg.popup("직접 RunDir 을 입력하거나 찾기 버튼으로 지정하세요.", title="입력 오류")
            continue

        try:
            sample_n = int(values["-SAMPLEN-"])
            if sample_n <= 0:
                raise ValueError
        except ValueError:
            sg.popup("SampleN 은 1 이상의 정수여야 합니다.", title="입력 오류")
            continue

        start_thread(run_step02, base, run_dir, sample_n)

    elif event == "-RUN_FOL-":
        root = values["-ROOT-"].strip() or DEFAULT_ROOT
        base = values["-BASE-"].strip() or DEFAULT_BASE

        try:
            depth_limit = int(values["-F_DEPTH-"])
            if depth_limit <= 0:
                raise ValueError
        except ValueError:
            sg.popup("최대 깊이는 1 이상의 정수여야 합니다.", title="입력 오류")
            continue

        try:
            min_dir_mb = int(values["-F_MINMB-"])
            if min_dir_mb < 0:
                raise ValueError
        except ValueError:
            sg.popup("최소 폴더 용량(MB)은 0 이상의 정수여야 합니다.", title="입력 오류")
            continue

        try:
            min_similarity = float(values["-F_MINSIM-"])
            if not (0 <= min_similarity <= 100):
                raise ValueError
        except ValueError:
            sg.popup("최소 유사도(%)는 0~100 사이의 숫자여야 합니다.", title="입력 오류")
            continue

        try:
            top_k = int(values["-F_TOPK-"])
            if top_k <= 0:
                raise ValueError
        except ValueError:
            sg.popup("Top K 는 1 이상의 정수여야 합니다.", title="입력 오류")
            continue

        start_thread(
            run_folder_scan,
            root,
            base,
            depth_limit,
            min_dir_mb,
            min_similarity,
            top_k,
        )

    # === 로그 큐 비우기 ===
    try:
        while True:
            line = log_queue.get_nowait()
            if line in ("__STEP01_DONE__", "__STEP02_DONE__"):
                # 작업 종료 표시지만, 여기서는 별 처리 안 하고 무시
                continue
            window["-LOG-"].update(line + "\n", append=True)
    except queue.Empty:
        pass

window.close()

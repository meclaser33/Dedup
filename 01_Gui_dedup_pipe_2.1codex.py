# 01_Gui_dedup_pipe_1.9.2claud.py
# - 01_Dedup_pipe_CI_2.7.py / 02_Full_pipe_CI_2.7.py ?섑띁 + ?대뜑 ?좎궗???ㅼ틪(FOLDER) ?듯빀 踰꾩쟾
# - [Update] ?ㅼ젙媛??먮룞 ???JSON) 諛??ㅼ떆媛?濡쒓렇 理쒖쟻???곸슜
# - [Update v2.1] ?대뜑 ?ㅼ틪 ?뚭퀬由ъ쬁 ?꾨㈃ 媛쒖꽑
#     - ?뚯씪 吏臾? ?꾩껜 ?쎄린 ???욌뮘 64KB SHA1 (100~1000諛?鍮좊쫫)
#     - ?뚯씪???댁떆 怨꾩궛 ??1踰?(以묐났 ?쎄린 ?쒓굅)
#     - ?꾨옒?믪쐞 ?ш? 吏묎퀎 (I/O ?놁쓬)
#     - 泥댄겕?ъ씤??????ш컻 吏??
#     - 硫?곗뒪?덈뱶 蹂묐젹 ?댁떆 怨꾩궛

# --- Windows DPI ?ㅼ젙 (釉붾윭 諛⑹?) ---
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

# ===== ?ㅼ젙 諛?寃쎈줈 愿由?=====
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "gui_config.json"

DEFAULT_ROOT = r"C:\Users\Meclaser\Desktop\Mec_DB"
DEFAULT_BASE = r"C:\elice\Dedup"

SCRIPT_01 = "01_Dedup_pipe_CI_2.7.py"
SCRIPT_02 = "02_Full_pipe_CI_2.7.py"
PYTHON_EXE = sys.executable

def load_settings():
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {"ROOT": DEFAULT_ROOT, "BASE": DEFAULT_BASE}

def save_settings(root, base):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump({"ROOT": root, "BASE": base}, f, ensure_ascii=False, indent=4)
    except:
        pass

saved_cfg = load_settings()

# 濡쒓렇 ?꾨떖????
log_queue: "queue.Queue[str]" = queue.Queue()

pause_event = threading.Event()

def append_log(line: str):
    log_queue.put(line)


def _wait_if_paused():
    while pause_event.is_set():
        time.sleep(0.2)


# ====== ?좏떥 ======

def find_latest_run_dir(base_path: str) -> Path | None:
    runs_root = Path(base_path) / "Runs"
    if not runs_root.exists():
        return None
    candidates = [d for d in runs_root.iterdir() if d.is_dir() and d.name.startswith("run_")]
    if not candidates:
        return None
    return max(candidates, key=lambda d: d.stat().st_mtime)


def safe_filename(s: str, max_len: int = 160) -> str:
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
        append_log(f"[FOLDER][WARN] 留곹겕 ?앹꽦 ?ㅽ뙣: {link_path} ({type(e).__name__}: {e})")


# ====== 諛깃렇?쇱슫???묒뾽??======

def run_step01(root: str, base: str, mode: str, top_n: int):
    save_settings(root, base)
    try:
        append_log("[STEP 01] ?쒖옉?⑸땲??..")
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
            _wait_if_paused()
            append_log(line.rstrip("\n"))

        rc = proc.wait()
        append_log(f"[STEP 01] 醫낅즺 (returncode={rc})")
    except Exception as e:
        append_log(f"[ERROR][STEP 01] {type(e).__name__}: {e}")
    finally:
        append_log("__STEP01_DONE__")


def run_step02(base: str, run_dir: str | None, sample_n: int):
    try:
        base = base.strip() or DEFAULT_BASE
        if run_dir:
            target_run = Path(run_dir)
        else:
            latest = find_latest_run_dir(base)
            if latest is None:
                append_log("[ERROR][STEP 02] BASE\\Runs ?덉뿉 run_* ?대뜑媛 ?놁뒿?덈떎.")
                append_log("__STEP02_DONE__")
                return
            target_run = latest

        if not target_run.exists():
            append_log(f"[ERROR][STEP 02] RunDir 寃쎈줈媛 議댁옱?섏? ?딆뒿?덈떎: {target_run}")
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
            _wait_if_paused()
            append_log(line.rstrip("\n"))

        rc = proc.wait()
        append_log(f"[STEP 02] 醫낅즺 (returncode={rc})")
    except Exception as e:
        append_log(f"[ERROR][STEP 02] {type(e).__name__}: {e}")
    finally:
        append_log("__STEP02_DONE__")


# ====== ?대뜑 ?댁떆 ?좎궗???ㅼ틪 v2 (理쒖쟻?? ======

_CHUNK = 64 * 1024  # 64KB


def fast_fingerprint(fp: Path):
    """
    ?뚯씪 ??64KB + ??64KB + ?뚯씪?ш린 ??SHA1.
    ?꾩껜 ?쎄린 ?鍮?100~1000諛?鍮좊쫫, ?ㅼ슜 ?뺥솗??99%+
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


def _ckpt_unpack(item):
    """Backward-compatible checkpoint entry parser."""
    if isinstance(item, dict):
        h = item.get("h")
        sz = item.get("sz")
        mt = item.get("mt")
    elif isinstance(item, (list, tuple)) and len(item) >= 2:
        h = item[0]
        sz = item[1]
        mt = item[2] if len(item) >= 3 else None
    else:
        return None

    try:
        sz = int(sz)
    except Exception:
        return None

    if mt is not None:
        try:
            mt = int(mt)
        except Exception:
            mt = None

    if not isinstance(h, str) or not h:
        return None

    return h, sz, mt


def run_folder_scan(root: str, base: str, depth_limit: int,
                    min_dir_mb: int, min_similarity: float, top_k: int):
    """
    媛쒖꽑???대뜑 ?좎궗???ㅼ틪.
      1) ?뚯씪 紐⑸줉 ?섏쭛 (depth_limit ?댄븯)
      2) ?뚯씪 吏臾?怨꾩궛 ??1踰?(?욌뮘 64KB, 硫?곗뒪?덈뱶, 泥댄겕?ъ씤??
      3) ?대뜑蹂??ш? ?댁떆??援ъ꽦 (I/O ?놁쓬, ?꾨옒?믪쐞 蹂묓빀)
      4) ?먯뭅???좎궗??怨꾩궛
      5) CSV + .lnk ???
    """
    save_settings(root, base)
    t0 = time.time()

    root_path = Path(root.strip() or DEFAULT_ROOT)
    base_path = Path(base.strip() or DEFAULT_BASE)
    min_bytes = min_dir_mb * 1024 * 1024

    ckpt_id   = hashlib.md5(str(root_path).encode()).hexdigest()[:8]
    ckpt_path = base_path / f"_ckpt_{ckpt_id}.json"

    append_log(f"[FOLDER] ???ㅼ틪 ?쒖옉: {root_path}")
    append_log(f"[FOLDER]   depth {depth_limit} | min {min_dir_mb}MB"
               f" | similarity >= {min_similarity}% | Top {top_k}")

    # ?? Step 1: ?뚯씪 紐⑸줉 ?섏쭛 ???????????????????????????????????????????
    append_log("[1/4] ?뚯씪 紐⑸줉 ?섏쭛 以?..")

    scan_dirs: set[Path] = set()
    all_files: list[Path] = []

    for cur_root, dirs, files in os.walk(root_path, topdown=True):
        _wait_if_paused()
        cur = Path(cur_root)
        depth = len(cur.relative_to(root_path).parts)
        if depth >= depth_limit:
            dirs[:] = []
            continue
        scan_dirs.add(cur)
        for fname in files:
            all_files.append(cur / fname)

    append_log(f"    dirs {len(scan_dirs):,} | files {len(all_files):,}")

    # ?? Step 2: 吏臾?怨꾩궛 (泥댄겕?ъ씤??吏?? ??????????????????????????????
    append_log("[2/4] 鍮좊Ⅸ 吏臾?怨꾩궛 以?(?욌뮘 64KB)...")

    file_index: dict[str, list] = _load_ckpt(ckpt_path)
    if file_index:
        append_log(f"    ????? ??: {len(file_index):,}? ??")

    # Revalidate cached entries using current size/mtime; drop deleted files.
    current_meta: dict[str, tuple[int, int]] = {}
    pending: list[Path] = []

    for fp in all_files:
        _wait_if_paused()
        sp = str(fp)
        try:
            st = fp.stat()
        except Exception:
            continue

        sz_now = int(st.st_size)
        mt_now = int(st.st_mtime_ns)
        current_meta[sp] = (sz_now, mt_now)

        old = _ckpt_unpack(file_index.get(sp))
        if old is None:
            pending.append(fp)
            continue

        _, old_sz, old_mt = old
        if old_sz != sz_now or old_mt != mt_now:
            pending.append(fp)

    removed = 0
    for k in list(file_index.keys()):
        if k not in current_meta:
            file_index.pop(k, None)
            removed += 1

    if removed:
        append_log(f"    ????? ??: ??? ?? {removed:,}? ??")

    n_pending = len(pending)
    append_log(f"    ???/?? ?? {n_pending:,}? ?? ?? (??? 6?)...")

    processed = 0
    SAVE_EVERY = 5_000
    LOG_EVERY_SEC = 60
    last_progress_log = time.time()

    with ThreadPoolExecutor(max_workers=6) as ex:
        fmap = {ex.submit(fast_fingerprint, fp): fp for fp in pending}
        for fut in as_completed(fmap):
            _wait_if_paused()
            res = fut.result()
            if res:
                h, sz = res
                sp = str(fmap[fut])
                mt = current_meta.get(sp, (sz, None))[1]
                file_index[sp] = {"h": h, "sz": sz, "mt": mt}

            processed += 1
            now = time.time()
            by_count = (processed % SAVE_EVERY == 0)
            by_time = (now - last_progress_log) >= LOG_EVERY_SEC

            if by_count or by_time:
                elapsed = now - t0
                rate = processed / elapsed if elapsed > 0 else 1
                eta = (n_pending - processed) / rate
                append_log(
                    f"    {processed:,}/{n_pending:,} | elapsed {elapsed/60:.1f}m | eta {eta/60:.1f}m"
                )
                last_progress_log = now

                if by_count:
                    _save_ckpt(ckpt_path, file_index)

    _save_ckpt(ckpt_path, file_index)
    append_log(f"[2/4] done ({(time.time()-t0)/60:.1f}m | total {len(file_index):,})")

    # ?? Step 3: ?대뜑蹂??ш? ?댁떆??援ъ꽦 (I/O ?놁쓬) ???????????????????????
    append_log("[3/4] ?대뜑 ?댁떆??吏묎퀎 以?..")

    # 吏곸냽 ?뚯씪留?癒쇱? 吏묎퀎
    direct: dict[Path, dict] = {
        d: {"hashes": set(), "bytes": 0, "files": 0} for d in scan_dirs
    }

    for path_str, item in file_index.items():
        unpacked = _ckpt_unpack(item)
        if unpacked is None:
            continue
        h, sz, _ = unpacked
        parent = Path(path_str).parent
        if parent in direct:
            direct[parent]["hashes"].add(h)
            direct[parent]["bytes"] += sz
            direct[parent]["files"] += 1

    # 源딆? ?대뜑遺??遺紐⑤줈 蹂묓빀 (?ш? 吏묎퀎)
    recursive: dict[Path, dict] = {
        d: {"hashes": set(v["hashes"]), "bytes": v["bytes"], "files": v["files"]}
        for d, v in direct.items()
    }

    for d in sorted(scan_dirs, key=lambda p: len(p.parts), reverse=True):
        _wait_if_paused()
        parent = d.parent
        if parent in recursive:
            recursive[parent]["hashes"] |= recursive[d]["hashes"]
            recursive[parent]["bytes"]  += recursive[d]["bytes"]
            recursive[parent]["files"]  += recursive[d]["files"]

    # 理쒖냼 ?⑸웾 ?꾪꽣
    candidates = [
        {"path": d, **v}
        for d, v in recursive.items()
        if v["bytes"] >= min_bytes and v["hashes"]
    ]
    candidates.sort(key=lambda x: x["bytes"], reverse=True)

    append_log(f"[3/4] done ({(time.time()-t0)/60:.1f}m | candidates {len(candidates)} >= {min_dir_mb}MB)")


    if len(candidates) < 2:
        append_log("[FOLDER] 鍮꾧탳???꾨낫 ?대뜑媛 2媛?誘몃쭔?낅땲?? 議곌굔???꾪솕??蹂댁꽭??")
        append_log("__FOLDER_DONE__")
        return

    # ?? Step 4: ?좎궗??怨꾩궛 ??????????????????????????????????????????????
    n_cands     = len(candidates)
    total_pairs = n_cands * (n_cands - 1) // 2
    append_log(f"[4/4] ?좎궗??怨꾩궛 以?.. ({n_cands}媛??대뜑, {total_pairs:,}??")

    pairs    = []
    compared = 0

    for a, b in combinations(candidates, 2):
        if compared % 2000 == 0:
            _wait_if_paused()

        # 遺紐??먯떇 愿怨??쒖쇅
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
            pairs.append({
                "dir1":         a["path"],
                "dir2":         b["path"],
                "score":        round(score, 1),
                "shared_files": len(inter),
                "files1":       a["files"],
                "files2":       b["files"],
                "mb1":          round(a["bytes"] / 1024 / 1024, 1),
                "mb2":          round(b["bytes"] / 1024 / 1024, 1),
            })

        compared += 1
        if compared % 100_000 == 0:
            append_log(f"    {compared:,}/{total_pairs:,} pairs | hits {len(pairs)}")


    pairs.sort(key=lambda x: x["score"], reverse=True)
    append_log(f"[4/4] ?? | {len(pairs)}? ?? (?{min_similarity}%)")

    # Build folder groups from similarity graph (connected components).
    cand_by_path = {c["path"]: c for c in candidates}
    adjacency: dict[Path, set[Path]] = {p: set() for p in cand_by_path}
    edge_score: dict[frozenset, float] = {}

    for p in pairs:
        a = p["dir1"]
        b = p["dir2"]
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)
        edge_score[frozenset((a, b))] = float(p["score"])

    groups: list[dict] = []
    seen: set[Path] = set()

    for start_path in adjacency:
        if start_path in seen or not adjacency[start_path]:
            continue

        stack = [start_path]
        comp: set[Path] = set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            comp.add(cur)
            for nxt in adjacency.get(cur, ()): 
                if nxt not in seen:
                    stack.append(nxt)

        if len(comp) < 2:
            continue

        paths = sorted(comp, key=lambda x: str(x).lower())
        scores = []
        for i in range(len(paths)):
            for j in range(i + 1, len(paths)):
                sc = edge_score.get(frozenset((paths[i], paths[j])))
                if sc is not None:
                    scores.append(sc)

        avg_score = (sum(scores) / len(scores)) if scores else 0.0
        max_score = max(scores) if scores else 0.0
        total_mb = sum(cand_by_path[p]["bytes"] for p in paths) / 1024 / 1024

        groups.append({
            "paths": paths,
            "size": len(paths),
            "avg_score": round(avg_score, 1),
            "max_score": round(max_score, 1),
            "total_mb": round(total_mb, 1),
        })

    groups.sort(key=lambda g: (g["size"], g["avg_score"], g["total_mb"]), reverse=True)
    top_groups = groups[:top_k]

    run_id     = time.strftime("%Y%m%d_%H%M")
    run_dir    = base_path / "Runs" / f"run_{run_id}_fol"
    review_dir = run_dir / "01_review_fol"
    review_dir.mkdir(parents=True, exist_ok=True)

    csv_path = run_dir / "folder_similarity_groups.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["rank", "group_size", "avg_score%", "max_score%", "total_mb", "folders"])
        for i, g in enumerate(top_groups, 1):
            w.writerow([
                i,
                g["size"],
                g["avg_score"],
                g["max_score"],
                g["total_mb"],
                " | ".join(str(p) for p in g["paths"]),
            ])

    for i, g in enumerate(top_groups, 1):
        grp_name = f"{i:02d}_{g['size']}dirs_{int(g['avg_score'])}pct"
        grp_dir = review_dir / grp_name
        grp_dir.mkdir(parents=True, exist_ok=True)
        for j, folder_path in enumerate(g["paths"], 1):
            link_name = f"{j:02d}_{safe_filename(folder_path.name)}"
            create_dir_shortcut(grp_dir / link_name, folder_path)

    total_t = time.time() - t0
    append_log("")
    append_log(f"[FOLDER] done! total: {total_t/60:.1f}m")
    append_log(f"[FOLDER] result groups: {len(top_groups)} -> {run_dir}")
    append_log(f"[FOLDER] 泥댄겕?ъ씤?? {ckpt_path}")
    append_log(f"[FOLDER] (?ㅼ쓬 ?ㅽ뻾 ??吏臾??ш퀎???놁씠 諛붾줈 鍮꾧탳 吏꾪뻾)")
    append_log("__FOLDER_DONE__")


# ====== GUI ?덉씠?꾩썐 ======

sg.theme("DarkBlue3")
sg.set_options(font=("留묒? 怨좊뵓", 10))

# --- ??01: Dedup ---
layout_tab_01 = [
    [sg.Text("?뚯씪 ?댁슜/?대쫫 湲곕컲 以묐났 ?ㅼ틪", font=("留묒? 怨좊뵓", 10, "bold"))],
    [sg.Text("紐⑤뱶:"),
     sg.Radio("DUP",  "M1", key="-MODE_DUP-", default=True),
     sg.Radio("BIG",  "M1", key="-MODE_BIG-")],
    [sg.Text("TOP N:"), sg.Input("50", size=(5, 1), key="-TOPN-")],
    [sg.Button("以묐났 ?ㅼ틪 ?ㅽ뻾", key="-RUN01-", button_color="firebrick", size=(20, 1))]
]

# --- ??02: BigFiles ---
layout_tab_02 = [
    [sg.Text("??⑸웾 ?뚯씪 由щ럭 諛??뺣━", font=("留묒? 怨좊뵓", 10, "bold"))],
    [sg.Text("?섑뵆留???"), sg.Input("10", size=(5, 1), key="-SAMPLEN-")],
    [sg.Button("理쒖떊 Run ?ㅽ뻾",  key="-RUN02_LATEST-", size=(15, 1)),
     sg.Button("吏곸젒 吏???ㅽ뻾", key="-RUN02_MANUAL-", size=(15, 1))]
]

# --- ??03: Folder ---
layout_tab_03 = [
    [sg.Text("?댁슜臾??좎궗 ?대뜑 ?ㅼ틪 (?욌뮘 64KB 吏臾?", font=("留묒? 怨좊뵓", 10, "bold"))],
    [sg.Text("源딆씠:"),      sg.Input("3",  size=(3, 1), key="-F_DEPTH-"),
     sg.Text("理쒖냼MB:"),    sg.Input("1000", size=(6, 1), key="-F_MINMB-")],
    [sg.Text("?좎궗??%):"), sg.Input("85", size=(3, 1), key="-F_MINSIM-"),
     sg.Text("Top K:"),     sg.Input("20", size=(3, 1), key="-F_TOPK-")],
    [sg.Button("?대뜑 ?좎궗???ㅼ틪 ?쒖옉", key="-RUN_FOL-",
               button_color="darkgreen", size=(25, 1))]
]

# --- ?꾩껜 ?덉씠?꾩썐 ---
layout = [
    [sg.Text("Mec_DB ?듯빀 ?뺣━ ?쒖뒪??v2.1",
             font=("Malgun Gothic", 16, "bold"), text_color="cyan")],
    [sg.Frame("怨듯넻 寃쎈줈 ?ㅼ젙 (?먮룞 ??λ맖)", [
        [sg.Text("ROOT:"), sg.Input(saved_cfg["ROOT"], key="-ROOT-", size=(50, 1)),
         sg.FolderBrowse("李얘린")],
        [sg.Text("BASE:"), sg.Input(saved_cfg["BASE"], key="-BASE-", size=(50, 1)),
         sg.FolderBrowse("李얘린")]
    ])],
    [sg.TabGroup([[
        sg.Tab(" 01.Dedup ",   layout_tab_01),
        sg.Tab(" 02.BigFiles ", layout_tab_02),
        sg.Tab(" 03.Folder ",  layout_tab_03),
    ]], key="-TABS-")],
    [sg.Multiline(size=(95, 18), key="-LOG-",
                  autoscroll=True, disabled=True, font=("Consolas", 9))],
    [sg.Button("Pause", key="-PAUSE-"), sg.Button("Exit", key="-EXIT-")]
]

window = sg.Window("Mec_DB Cleaner v2.1", layout, finalize=True, resizable=True)
window["-TABS-"].expand(expand_x=True, expand_y=True)
window["-LOG-"].expand(expand_x=True, expand_y=True)

# ====== ?ㅻ젅???ы띁 ======
current_thread = None
current_task = ""

def _parse_int(values, key, label, min_value=0):
    try:
        v = int(str(values[key]).strip())
    except Exception:
        raise ValueError(f"{label} must be an integer.")
    if v < min_value:
        raise ValueError(f"{label} must be >= {min_value}.")
    return v


def _parse_float(values, key, label, min_value=0.0, max_value=100.0):
    try:
        v = float(str(values[key]).strip())
    except Exception:
        raise ValueError(f"{label} must be a number.")
    if v < min_value or v > max_value:
        raise ValueError(f"{label} must be between {min_value} and {max_value}.")
    return v


def start_thread(target, *args):
    global current_thread, current_task
    if current_thread and current_thread.is_alive():
        sg.popup("Task already running.")
        return
    pause_event.clear()
    window["-PAUSE-"].update("Pause")
    window["-LOG-"].update("")
    current_task = target.__name__
    current_thread = threading.Thread(target=target, args=args, daemon=True)
    current_thread.start()

# ====== 硫붿씤 ?대깽??猷⑦봽 ======
while True:
    event, values = window.read(timeout=100)
    if event in (sg.WIN_CLOSED, "-EXIT-"):
        break

    if event == "-PAUSE-":
        if not (current_thread and current_thread.is_alive()):
            sg.popup("No running task.")
            continue
        if pause_event.is_set():
            pause_event.clear()
            window["-PAUSE-"].update("Pause")
            append_log("[CTRL] resumed")
        else:
            pause_event.set()
            window["-PAUSE-"].update("Resume")
            append_log("[CTRL] paused")
        continue

    # 1踰???
    if event == "-RUN01-":
        try:
            top_n = _parse_int(values, "-TOPN-", "TOP N", 1)
        except ValueError as e:
            sg.popup_error(str(e))
            continue

        mode = "DUP" if values["-MODE_DUP-"] else "BIGFILE"
        start_thread(run_step01,
                     values["-ROOT-"], values["-BASE-"],
                     mode, top_n)

    # 2踰???
    elif event == "-RUN02_LATEST-":
        try:
            sample_n = _parse_int(values, "-SAMPLEN-", "Sample N", 1)
        except ValueError as e:
            sg.popup_error(str(e))
            continue

        start_thread(run_step02,
                     values["-BASE-"], None,
                     sample_n)
    elif event == "-RUN02_MANUAL-":
        run_dir = sg.popup_get_text("Enter RunDir path")
        if run_dir:
            try:
                sample_n = _parse_int(values, "-SAMPLEN-", "Sample N", 1)
            except ValueError as e:
                sg.popup_error(str(e))
                continue

            start_thread(run_step02,
                         values["-BASE-"], run_dir,
                         sample_n)

    # 3踰???
    elif event == "-RUN_FOL-":
        try:
            depth = _parse_int(values, "-F_DEPTH-", "Depth", 1)
            min_mb = _parse_int(values, "-F_MINMB-", "Min MB", 1)
            min_sim = _parse_float(values, "-F_MINSIM-", "Similarity %", 0.0, 100.0)
            top_k = _parse_int(values, "-F_TOPK-", "Top K", 1)
        except ValueError as e:
            sg.popup_error(str(e))
            continue

        start_thread(run_folder_scan,
                     values["-ROOT-"], values["-BASE-"],
                     depth,
                     min_mb,
                     min_sim,
                     top_k)

    # 濡쒓렇 異쒕젰
    try:
        while True:
            line = log_queue.get_nowait()
            if line in {"__STEP01_DONE__", "__STEP02_DONE__", "__FOLDER_DONE__"}:
                pause_event.clear()
                window["-PAUSE-"].update("Pause")
                current_task = ""
                continue
            window["-LOG-"].update(line + "\n", append=True)
    except queue.Empty:
        pass

window.close()

import os
import csv
import time
import sys
import threading
import queue
import subprocess
from pathlib import Path
from itertools import combinations
from blake3 import blake3
import FreeSimpleGUI as sg

# --- Windows DPI Awareness (To prevent blurry text) ---
if sys.platform == "win32":
    import ctypes
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass

# ===== Global Constants & Shared Queue =====
DEFAULT_ROOT = r"D:/"
DEFAULT_BASE = r"C:/elice/Dedup"
log_queue = queue.Queue()

def append_log(line: str):
    log_queue.put(line)

def update_prog(val: int):
    log_queue.put(("PROG", val))

# ====== [Utility Functions] ======

def safe_filename(s: str, max_len: int = 150) -> str:
    """Sanitize strings for Windows file system"""
    s = "".join("_" if ch in '<>:"/\\|?*\x00' else ch for ch in s)
    return " ".join(s.split()).strip()[:max_len]

def create_dir_shortcut(link_path: Path, target_dir: Path):
    """Create Windows .lnk folder shortcut via PowerShell"""
    link_path = link_path.with_suffix(".lnk")
    lnk, tgt = str(link_path).replace("'", "''"), str(target_dir).replace("'", "''")
    ps = (f"$WshShell = New-Object -ComObject WScript.Shell; "
          f"$Shortcut = $WshShell.CreateShortcut('{lnk}'); "
          f"$Shortcut.TargetPath = '{tgt}'; $Shortcut.Save();")
    subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True)

def find_latest_run_dir(base_path: Path):
    """Identify the most recent run folder in the BASE directory"""
    runs_dir = base_path / "Runs"
    if not runs_dir.exists(): return None
    run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and d.name.startswith("run_")]
    if not run_dirs: return None
    return max(run_dirs, key=lambda d: d.stat().st_mtime)

# ====== [Engine 01: Duplicate & Large File Scanner] ======

def run_step01(root, base, mode, top_n):
    try:
        root_p, base_p = Path(root.strip()), Path(base.strip())
        run_id = time.strftime("%Y%m%d_%H%M")
        run_dir = base_p / "Runs" / f"run_{run_id}_{mode.lower()}"
        review_dir = run_dir / "01_review"
        review_dir.mkdir(parents=True, exist_ok=True)

        append_log(f"🚀 [Step 01] Scanning {root_p} in {mode} mode...")
        
        all_files = []
        for r, _, fs in os.walk(root_p):
            for f in fs:
                all_files.append(Path(r) / f)
        
        total = len(all_files)
        groups = {}
        for i, fp in enumerate(all_files):
            try:
                # DUP identifies identical files; BIG focuses on overall file size
                key = f"{fp.name}_{fp.stat().st_size}" if mode == "DUP" else "SINGLE_FILE"
                groups.setdefault(key, []).append(fp)
            except: continue
            if i % 100 == 0: update_prog(int((i/total)*100))

        # Filter and sort results by total impact
        sorted_groups = []
        for key, fps in groups.items():
            if mode == "DUP" and len(fps) < 2: continue
            total_size = sum(f.stat().st_size for f in fps)
            sorted_groups.append((total_size, fps))
        
        sorted_groups.sort(key=lambda x: x[0], reverse=True)
        top_groups = sorted_groups[:top_n]

        # Export Results to CSV and create shortcuts
        csv_path = run_dir / f"{mode.lower()}_results.csv"
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["Rank", "Size_MB", "Count", "Sample_Path"])
            for idx, (size, fps) in enumerate(top_groups, 1):
                writer.writerow([idx, f"{size/(1024*1024):.2f}", len(fps), str(fps[0])])
                # Generate physical review structure
                label = f"{idx:03d}_{size/(1024*1024):.0f}MB_{len(fps)}ea"
                pair_path = review_dir / label
                pair_path.mkdir(exist_ok=True)
                for f_idx, f_item in enumerate(fps, 1):
                    create_dir_shortcut(pair_path / f"Source_{f_idx:02d}", f_item.parent)

        append_log(f"✅ [Step 01] Success! Check folder: {run_dir.name}")
        update_prog(100)
    except Exception as e:
        append_log(f"❌ Error in Step 01: {e}")

# ====== [Engine 02: Review Generator (Metadata Check)] ======

def run_step02(base, sample_n):
    try:
        base_p = Path(base.strip())
        latest_run = find_latest_run_dir(base_p)
        if not latest_run:
            append_log("❌ Error: No previous run found.")
            return

        append_log(f"🚀 [Step 02] Creating Review for: {latest_run.name}")
        review_check_dir = latest_run / "02_review_check"
        review_check_dir.mkdir(parents=True, exist_ok=True)
        
        # Original Logic: Extract data from Step 01 CSV and sample it
        csv_file = next(latest_run.glob("*_results.csv"), None)
        if not csv_file:
            append_log("❌ Error: Result CSV not found in latest run.")
            return

        with open(csv_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)[:sample_n]
            
        for i, row in enumerate(rows, 1):
            sample_p = Path(row["Sample_Path"])
            shortcut_name = f"Sample_{i:02d}_{row['Size_MB']}MB"
            create_dir_shortcut(review_check_dir / shortcut_name, sample_p.parent)
            update_prog(int((i/len(rows))*100))

        append_log(f"✅ [Step 02] Done! Created {len(rows)} samples.")
        update_prog(100)
    except Exception as e:
        append_log(f"❌ Error in Step 02: {e}")

# ====== [Engine 03: Folder Similarity (Fast & Deep)] ======

def get_folder_stats(p, fast_mode):
    """Retrieve folder metrics (size, count) and optional hashes"""
    bytes_sum, files_count, finger_print = 0, 0, {}
    try:
        for r, _, fs in os.walk(p):
            for f in fs:
                try:
                    fp = Path(r) / f
                    size = fp.stat().st_size
                    bytes_sum += size
                    files_count += 1
                    if not fast_mode: # Only compute hashes for Deep mode (SSD)
                        with open(fp, "rb") as fb:
                            # Sample 64KB for faster deep scanning
                            h = blake3(fb.read(1024*64)).hexdigest()
                            finger_print[h] = size
                except: continue
    except: pass
    return bytes_sum, files_count, finger_print

def run_folder_scan_v2(root, base, depth, min_mb, min_sim, top_k, fast_mode):
    try:
        root_p, base_p = Path(root.strip()), Path(base.strip())
        mode_str = "Fast(Metadata)" if fast_mode else "Deep(BLAKE3)"
        append_log(f"🚀 [{mode_str}] Folder Scan Initialized: {root_p}")

        # Scan for candidate folders within depth limit
        candidates = []
        for r, ds, _ in os.walk(root_p):
            p = Path(r)
            if len(p.relative_to(root_p).parts) >= depth:
                ds[:] = []
                continue
            candidates.append(p)
        
        total_c = len(candidates)
        data_pool = []
        for i, p in enumerate(candidates):
            sz, cnt, mft = get_folder_stats(p, fast_mode)
            if sz / (1024*1024) >= min_mb:
                data_pool.append({"path": p, "bytes": sz, "files": cnt, "manifest": mft})
                if i % 10 == 0: append_log(f"   [Processing] {p.name}")
            update_prog(int((i/total_c)*100))

        # Perform Cross-Comparison
        append_log(f"📊 Calculating similarity for {len(data_pool)} folders...")
        results = []
        for a, b in combinations(data_pool, 2):
            # Skip nested folders (Parent vs Child)
            if a["path"] in b["path"].parents or b["path"] in a["path"].parents: continue
            
            if fast_mode:
                # Similarity based on size and file count ratio
                s_ratio = min(a['bytes'], b['bytes']) / max(a['bytes'], b['bytes'])
                c_ratio = min(a['files'], b['files']) / max(a['files'], b['files'])
                score = (s_ratio * 0.7 + c_ratio * 0.3) * 100
            else:
                # Jaccard Similarity for file hashes
                set_a, set_b = set(a['manifest'].keys()), set(b['manifest'].keys())
                score = (len(set_a & set_b) / len(set_a | set_b)) * 100 if (set_a | set_b) else 0

            if score >= min_sim:
                results.append({"dir1": a['path'], "dir2": b['path'], "score": score, "size": a['bytes']})

        results.sort(key=lambda x: x["score"], reverse=True)
        top_results = results[:top_k]

        # Generate output directory and CSV
        run_id = time.strftime("%Y%m%d_%H%M")
        run_dir = base_p / "Runs" / f"run_{run_id}_folder_sim"
        fol_review = run_dir / "01_folder_review"
        fol_review.mkdir(parents=True, exist_ok=True)

        csv_path = run_dir / "folder_pairs.csv"
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["Score", "Folder_A", "Folder_B", "Size_MB"])
            for p in top_results:
                writer.writerow([f"{p['score']:.2f}", str(p['dir1']), str(p['dir2']), f"{p['size']/(1024*1024):.1f}"])
                
                # Visual structure for the user
                pair_name = f"{int(p['score'])}pct_{safe_filename(p['dir1'].name)}_vs_{safe_filename(p['dir2'].name)}"
                p_path = fol_review / pair_name
                p_path.mkdir(exist_ok=True)
                create_dir_shortcut(p_path / "01_Original", p["dir1"])
                create_dir_shortcut(p_path / "02_Comparison", p["dir2"])

        append_log(f"✅ [Step 03] Finished! Found {len(top_results)} similar pairs.")
        update_prog(100)
    except Exception as e:
        append_log(f"❌ Error in Step 03: {e}")

# ====== [Graphical User Interface] ======

sg.theme("DarkBlue3")

layout = [
    [sg.Text("Mec_DB Cleaner v2.0 Professional", font=("Malgun Gothic", 16, "bold"), text_color="cyan")],
    [sg.Frame("Path Settings", [
        [sg.Text("ROOT:"), sg.Input(DEFAULT_ROOT, key="-ROOT-", size=(40,1)), sg.FolderBrowse()],
        [sg.Text("BASE:"), sg.Input(DEFAULT_BASE, key="-BASE-", size=(40,1)), sg.FolderBrowse()]
    ])],
    [sg.TabGroup([[
        sg.Tab(" 01.FileScan ", [
            [sg.Text("Scan for Duplicates or Large Files")],
            [sg.Radio("Duplicate (DUP)", "M1", key="-MODE_DUP-", default=True), sg.Radio("Large (BIG)", "M1", key="-MODE_BIG-")],
            [sg.Text("Top N Results:"), sg.Input("50", size=(5, 1), key="-TOPN-")],
            [sg.Button("Start File Scan", key="-RUN01-", button_color="firebrick", size=(30, 1))]
        ]),
        sg.Tab(" 02.ReviewGen ", [
            [sg.Text("Generate Review Shortcuts from Latest Scan")],
            [sg.Text("Sample Count:"), sg.Input("10", size=(5, 1), key="-SAMPLEN-")],
            [sg.Button("Create Review Folders", key="-RUN02-", button_color="navy", size=(30, 1))]
        ]),
        sg.Tab(" 03.FolderScan ", [
            [sg.Text("Compare Folders for Similarity")],
            [sg.Radio("Fast (Metadata)", "M2", key="-F_FAST-", default=True), sg.Radio("Deep (Hash)", "M2", key="-F_DEEP-")],
            [sg.Text("Depth:"), sg.Input("3", size=(3,1), key="-F_D-"), sg.Text("Min Size(MB):"), sg.Input("1000", size=(5,1), key="-F_MB-")],
            [sg.Button("Start Folder Scan", key="-RUN_FOL-", button_color="darkgreen", size=(30, 1))]
        ])
    ]])],
    [sg.ProgressBar(100, orientation='h', size=(46, 15), key="-PROG-", bar_color=("lightgreen", "white"))],
    [sg.Multiline(size=(75, 12), key="-LOG-", autoscroll=True, disabled=True, font=("Consolas", 9))],
    [sg.Button("Exit", key="-EXIT-", size=(10, 1))]
]

window = sg.Window("Mec_DB Cleaner v2.0", layout, finalize=True)

# ====== [Main Event Loop] ======

while True:
    event, values = window.read(timeout=100)
    if event in (sg.WIN_CLOSED, "-EXIT-"): break

    if event == "-RUN01-":
        m = "DUP" if values["-MODE_DUP-"] else "BIG"
        threading.Thread(target=run_step01, args=(values["-ROOT-"], values["-BASE-"], m, int(values["-TOPN-"])), daemon=True).start()

    if event == "-RUN02-":
        threading.Thread(target=run_step02, args=(values["-BASE-"], int(values["-SAMPLEN-"])), daemon=True).start()

    if event == "-RUN_FOL-":
        threading.Thread(target=run_folder_scan_v2, 
                         args=(values["-ROOT-"], values["-BASE-"], int(values["-F_D-"]), 
                               int(values["-F_MB-"]), 90.0, 20, values["-F_FAST-"]), daemon=True).start()

    # Process Queue for Log and Progress Bar updates
    try:
        while True:
            item = log_queue.get_nowait()
            if isinstance(item, tuple) and item[0] == "PROG":
                window["-PROG-"].update(item[1])
            else:
                window["-LOG-"].update(item + "\n", append=True)
    except queue.Empty:
        pass

window.close()
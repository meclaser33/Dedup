import os
import sys
from pathlib import Path

# ---------------- DPI / 폰트 설정 ----------------
try:
    import ctypes
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    pass

import FreeSimpleGUI as sg  # Free 버전

sg.set_options(font=("맑은 고딕", 10))
sg.theme("DarkBlue3")
# -------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
RUNS_ROOT = BASE_DIR / "Runs"


def guess_latest_review_root() -> str:
    """
    BASE\\Runs 아래에서 가장 최근 run_*_dup\\01_review_dup 폴더를 찾는다.
    """
    if not RUNS_ROOT.is_dir():
        return ""
    candidates = sorted(
        RUNS_ROOT.glob("run_*_dup/01_review_dup"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return str(candidates[0]) if candidates else ""


def load_group_files(group_path: Path):
    """
    그룹 폴더 안의 .lnk 파일 목록 로드.
    - is_mmm : 파일명이 mmm_ 로 시작하면 True
    - checked: 처음엔 is_mmm 상태와 동일
    """
    files = []
    for i, p in enumerate(sorted(group_path.glob("*.lnk"))):
        name = p.name
        is_mmm = name.startswith("mmm_")
        files.append(
            {
                "idx": i,
                "path": p,
                "name": name,
                "is_mmm": is_mmm,
                "checked": is_mmm,
            }
        )
    return files


def load_groups_with_files(review_root: Path):
    """
    REVIEW_ROOT 아래 그룹 + 각 그룹의 파일들을 모두 한 번에 로드한다.
    (그룹 하이라이트를 초기에 바로 반영하기 위해)
    """
    groups = []
    if not review_root.is_dir():
        return groups
    for idx, p in enumerate(sorted(review_root.iterdir())):
        if p.is_dir():
            files = load_group_files(p)
            groups.append(
                {
                    "idx": idx,
                    "name": p.name,
                    "path": p,
                    "files": files,
                }
            )
    return groups


def group_has_checked(group: dict) -> bool:
    files = group.get("files") or []
    return any(f["checked"] for f in files)


def make_group_list_values(groups):
    """
    Listbox 에 넣을 텍스트:
      - 체크 파일 있는 그룹: "★ 003 | 그룹이름"
      - 없는 그룹:          "  003 | 그룹이름"
    """
    vals = []
    for i, g in enumerate(groups):
        mark = "★" if group_has_checked(g) else " "
        vals.append(f"{mark} {i+1:03d} | {g['name']}")
    return vals


def make_file_table_values(file_rows):
    """
    오른쪽 Table 값:
      [체크표시, 번호, 링크이름]
    """
    vals = []
    for i, row in enumerate(file_rows, start=1):
        mark = "■" if row["checked"] else "□"
        vals.append([mark, f"{i:02d}", row["name"]])
    return vals


def log_print(window, text):
    window["-LOG-"].print(text)


def apply_mmm_for_files(window, file_rows):
    """
    file_rows 의 checked 상태를 실제 파일명(mmm_ prefix)으로 반영.
    """
    changed = 0
    for row in file_rows:
        want = row["checked"]
        cur = row["is_mmm"]
        if want == cur:
            continue  # 변화 없음

        old_path = row["path"]
        old_name = old_path.name

        if want and not cur:
            new_name = "mmm_" + old_name
        elif cur and not want:
            if old_name.startswith("mmm_"):
                new_name = old_name[4:]
            else:
                new_name = old_name
        else:
            continue

        new_path = old_path.with_name(new_name)
        try:
            old_path.rename(new_path)
            row["path"] = new_path
            row["name"] = new_name
            row["is_mmm"] = want
            changed += 1
            log_print(window, f"[OK] rename: {old_name} -> {new_name}")
        except Exception as e:
            log_print(window, f"[ERROR] rename 실패: {old_name} -> {new_name} ({e})")

    return changed


def build_window(default_root: str):
    layout = [
        [
            sg.Text("REVIEW ROOT (run_xxx_dup/01_review_dup):"),
            sg.Input(default_root, size=(80, 1), key="-ROOT-"),
            sg.FolderBrowse("찾기", target="-ROOT-", key="-BROWSE-"),
            sg.Button("로드", key="-LOAD-"),
        ],
        [sg.HorizontalSeparator()],
        [
            # 왼쪽: 그룹 목록 (Listbox)
            sg.Column(
                [
                    [sg.Text("그룹 목록")],
                    [
                        sg.Listbox(
                            values=[],
                            size=(30, 20),
                            key="-GROUP_LIST-",
                            enable_events=True,
                            expand_y=True,
                        )
                    ],
                ],
                expand_y=True,
            ),
            # 오른쪽: 파일 테이블
            sg.Column(
                [
                    [sg.Text("선택 그룹의 링크 목록")],
                    [
                        sg.Table(
                            values=[],
                            headings=["✓", "No", "링크 이름"],
                            key="-FILE_TABLE-",
                            enable_events=True,
                            auto_size_columns=False,
                            col_widths=[3, 4, 80],
                            num_rows=20,
                            justification="left",
                            expand_x=True,
                            expand_y=True,
                            select_mode=sg.TABLE_SELECT_MODE_BROWSE,
                        )
                    ],
                ],
                expand_x=True,
                expand_y=True,
            ),
        ],
        [sg.HorizontalSeparator()],
        [
            sg.Button("선택 그룹 mmm 토글 적용", key="-APPLY-MMM-"),
            sg.Button("체크된 전체 그룹 mmm 적용", key="-APPLY-MMM-ALL-"),
            sg.Button("링크 실행", key="-OPEN-LINK-"),
            sg.Button("폴더 열기", key="-OPEN-FOLDER-"),
        ],
        [sg.Text("로그 출력")],
        [
            sg.Multiline(
                size=(120, 6),
                key="-LOG-",
                autoscroll=True,
                expand_x=True,
                expand_y=False,
                disabled=True,
            )
        ],
        [sg.Button("종료")],
    ]

    window = sg.Window(
        "Review GUI 1.8 (mmm 토글 + 그룹 표시)",
        layout,
        resizable=True,
        finalize=True,
    )
    return window


def main():
    default_root = guess_latest_review_root()
    window = build_window(default_root)

    state = {
        "review_root": Path(default_root) if default_root else None,
        "groups": [],
        "current_group_idx": None,
        "current_files": [],
        "last_selected_row": None,
    }

    # 초기 자동 로드
    if default_root:
        review_root = Path(default_root)
        state["review_root"] = review_root
        state["groups"] = load_groups_with_files(review_root)
        window["-GROUP_LIST-"].update(values=make_group_list_values(state["groups"]))
        log_print(window, f"[INFO] 최신 REVIEW ROOT 자동 로드: {review_root}")

    while True:
        event, values = window.read()

        if event in (sg.WIN_CLOSED, "종료"):
            break

        # REVIEW ROOT 로드
        if event == "-LOAD-":
            path_str = values["-ROOT-"].strip()
            if not path_str:
                log_print(window, "[WARN] REVIEW ROOT 경로가 비어 있습니다.")
                continue
            root_path = Path(path_str)
            if not root_path.is_dir():
                log_print(window, f"[ERROR] 폴더가 아닙니다: {root_path}")
                continue

            state["review_root"] = root_path
            state["groups"] = load_groups_with_files(root_path)
            state["current_group_idx"] = None
            state["current_files"] = []
            state["last_selected_row"] = None

            window["-GROUP_LIST-"].update(values=make_group_list_values(state["groups"]))
            window["-FILE_TABLE-"].update(values=[])
            log_print(window, f"[INFO] REVIEW ROOT 로드: {root_path}")
            continue

        # 그룹 선택
        if event == "-GROUP_LIST-":
            if not state["groups"]:
                continue
            sel_list = values["-GROUP_LIST-"]
            if not sel_list:
                continue
            text = sel_list[0]  # 예: "★ 003 | 그룹명" 또는 "  003 | 그룹명"
            try:
                # 첫 글자(★ 또는 공백) 제거 후, 앞쪽 번호 파싱
                no_mark = text[1:].lstrip()
                idx_str = no_mark.split("|", 1)[0].strip()
                idx = int(idx_str) - 1
            except Exception:
                continue

            if not (0 <= idx < len(state["groups"])):
                continue

            group = state["groups"][idx]
            state["current_group_idx"] = idx
            state["current_files"] = group["files"]
            state["last_selected_row"] = None

            table_vals = make_file_table_values(state["current_files"])
            window["-FILE_TABLE-"].update(values=table_vals)
            log_print(window, f"[INFO] 그룹 선택: {group['name']} ({group['path']})")
            continue

        # 파일 테이블에서 행 클릭 -> 체크 토글
        if event == "-FILE_TABLE-":
            files = state["current_files"]
            if not files:
                continue
            rows = values["-FILE_TABLE-"]
            if not rows:
                continue
            row_idx = rows[0]
            if not (0 <= row_idx < len(files)):
                continue

            state["last_selected_row"] = row_idx
            row = files[row_idx]
            row["checked"] = not row["checked"]

            # 파일 테이블 갱신
            window["-FILE_TABLE-"].update(values=make_file_table_values(files))

            # 그룹 리스트 다시 표시 (★ 업데이트)
            if state["current_group_idx"] is not None:
                idx = state["current_group_idx"]
                state["groups"][idx]["files"] = files
            group_vals = make_group_list_values(state["groups"])
            window["-GROUP_LIST-"].update(values=group_vals)
            # 선택 유지
            if state["current_group_idx"] is not None:
                window["-GROUP_LIST-"].set_value([group_vals[state["current_group_idx"]]])

            return_state = "ON" if row["checked"] else "OFF"
            log_print(
                window,
                f"[UI] {row_idx+1:02d}번 링크 체크 상태: {return_state} ({row['name']})",
            )
            continue

        # 현재 선택된 그룹만 mmm 토글 적용
        if event == "-APPLY-MMM-":
            idx = state["current_group_idx"]
            if idx is None or not state["groups"]:
                log_print(window, "[WARN] 선택된 그룹이 없습니다.")
                continue

            files = state["groups"][idx]["files"]
            if not files:
                log_print(window, "[WARN] 해당 그룹에 파일이 없습니다.")
                continue

            changed = apply_mmm_for_files(window, files)
            state["groups"][idx]["files"] = files
            window["-FILE_TABLE-"].update(values=make_file_table_values(files))

            group_vals = make_group_list_values(state["groups"])
            window["-GROUP_LIST-"].update(values=group_vals)
            window["-GROUP_LIST-"].set_value([group_vals[idx]])

            log_print(window, f"[SUMMARY] 선택 그룹 mmm 토글 완료: {changed}개 변경")
            continue

        # 체크 상태가 반영된 전체 그룹에 대해 mmm 토글 적용
        if event == "-APPLY-MMM-ALL-":
            if not state["groups"]:
                log_print(window, "[WARN] 그룹이 없습니다.")
                continue

            total_changed = 0
            for g in state["groups"]:
                files = g["files"]
                if not files:
                    continue
                # 실제 변경 필요한 것들만 rename (함수 내부에서 판단)
                total_changed += apply_mmm_for_files(window, files)

            # 현재 보고 있는 그룹 테이블 갱신
            if state["current_group_idx"] is not None:
                idx = state["current_group_idx"]
                cur_files = state["groups"][idx]["files"]
                window["-FILE_TABLE-"].update(values=make_file_table_values(cur_files))

            # 그룹 리스트 다시 표시 (★ 재계산)
            group_vals = make_group_list_values(state["groups"])
            window["-GROUP_LIST-"].update(values=group_vals)
            if state["current_group_idx"] is not None:
                window["-GROUP_LIST-"].set_value([group_vals[state["current_group_idx"]]])

            log_print(window, f"[SUMMARY] 전체 그룹 mmm 토글 적용: 총 {total_changed}개 변경")
            continue

        # 선택된 링크 실행
        if event == "-OPEN-LINK-":
            files = state["current_files"]
            idx = state["last_selected_row"]
            if idx is None or not files:
                log_print(window, "[WARN] 선택된 링크가 없습니다.")
                continue
            path = files[idx]["path"]
            try:
                os.startfile(str(path))
                log_print(window, f"[OPEN] 링크 실행: {path}")
            except Exception as e:
                log_print(window, f"[ERROR] 링크 실행 실패: {path} ({e})")
            continue

        # 선택된 링크의 폴더 열기
        if event == "-OPEN-FOLDER-":
            files = state["current_files"]
            idx = state["last_selected_row"]
            if idx is None or not files:
                log_print(window, "[WARN] 선택된 링크가 없습니다.")
                continue
            folder = files[idx]["path"].parent
            try:
                os.startfile(str(folder))
                log_print(window, f"[OPEN] 폴더 열기: {folder}")
            except Exception as e:
                log_print(window, f"[ERROR] 폴더 열기 실패: {folder} ({e})")
            continue

    window.close()


if __name__ == "__main__":
    main()

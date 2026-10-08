"""Microsoft Planner import using your ALREADY OPEN Chrome window (Windows).

NO new browser is opened. Uses screen coordinates via pyautogui; fragile if
Planner layout / zoom / scroll changes. Never use unattended.

Install: py -m pip install openpyxl pyautogui pyperclip
Preview: py import_connect_to_work_planner_existing_chrome.py
One task: py import_connect_to_work_planner_existing_chrome.py --execute --limit 1

SAFETY: Title-only; no guaranteed Planner-side duplicate detection. Tasks
logged as submitted to local CSV, not verified created in Planner. Review one
manually, and do not use full import unless user has verified the interface.
"""
import argparse
import csv
import json
import sys
import time
from pathlib import Path

from openpyxl import load_workbook

HERE = Path(__file__).resolve().parent
XLSX = HERE / 'Connect_to_Work_Planner_Task_Import.xlsx'
POINTS = HERE / 'planner_existing_chrome_positions.json'
LOG = HERE / 'planner_desktop_import_log.csv'
BUCKETS = ['Mobilisation & Partners', 'Lot 1 – Saheli Lead', 'Lot 3 – Witton Lodge Lead',
           'Finance & MI Reporting', 'Compliance & Data Quality', 'Performance & Follow-ups']


def norm(v):
    return ' '.join(str(v or '').casefold().split())


def rows_from_excel(path):
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb['Planner Import']
    it = ws.values
    headers = next(it)
    rows = [dict(zip(headers, r)) for r in it]
    wb.close()
    return [r for r in rows if str(r.get('TaskTitle') or '').strip()
            and norm(r.get('ImportStatus')) in ('', 'pending')
            and not str(r.get('PlannerTaskID') or '').strip()]


def previous_submissions():
    if not LOG.exists():
        return set()
    with LOG.open(encoding='utf-8-sig', newline='') as f:
        return {r['TaskKey'] for r in csv.DictReader(f) if r['Result'] == 'SUBMITTED'}


def record(row, result):
    exists = LOG.exists()
    with LOG.open('a', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(['TaskKey', 'Bucket', 'TaskTitle', 'Result'])
        w.writerow([row.get('TaskKey'), row.get('Bucket'), row.get('TaskTitle'), result])


def validate_coordinates(coords):
    """Reject missing, identical or suspiciously close Add-task targets."""
    missing = [b for b in BUCKETS if b not in coords]
    if missing:
        raise ValueError('Missing bucket coordinates: ' + ', '.join(missing))
    points = []
    for b in BUCKETS:
        p = coords[b]
        if not isinstance(p, (list, tuple)) or len(p) != 2:
            raise ValueError(f'Invalid saved coordinates for {b}: {p!r}')
        x, y = int(p[0]), int(p[1])
        for other, ox, oy in points:
            if abs(x - ox) < 55 and abs(y - oy) < 40:
                raise ValueError(
                    f'Bucket positions overlap: {other} ({ox},{oy}) and {b} ({x},{y}). '
                    'Do not execute. Re-run --setup and hover over each DIFFERENT Add task button.'
                )
        points.append((b, x, y))


def capture_coordinates(gui, seconds=8):
    print('\nSETUP: Open the existing Connect to Work plan in Chrome, BOARD view.')
    print('Keep Chrome maximised and at the same zoom throughout setup and import.')
    print('You do NOT need to press Enter while hovering.')
    print(f'For EACH bucket, you get {seconds} seconds to move to Chrome and hover')
    print('over the correct Add task button. The position is captured automatically.')
    print('Do not click. If a bucket is off screen, scroll horizontally to it.')
    print('NOTE: If you scroll after setup, saved positions may no longer work.\n')
    coords = {}
    for index, bucket in enumerate(BUCKETS, 1):
        input(f'\n[{index}/6] Press ENTER to BEGIN countdown for [{bucket}]... ')
        print(f'Now switch to Chrome and HOVER over [{bucket}] Add task!', flush=True)
        for left in range(seconds, 0, -1):
            print(f'Capturing in {left}... ', end='\r', flush=True)
            time.sleep(1)
        p = gui.position()
        coords[bucket] = [p.x, p.y]
        print(f'\n  Captured [{bucket}]: ({p.x}, {p.y})', flush=True)
        if index < len(BUCKETS):
            print('Return to PowerShell for the next bucket. Do not move/resize Chrome.')
    try:
        validate_coordinates(coords)
    except ValueError as exc:
        print(f'\nSETUP REJECTED: {exc}')
        print('No coordinates were saved. Please rerun --setup.')
        return None
    POINTS.write_text(json.dumps(coords, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'\nVALID setup saved: {POINTS}')
    print('IMPORTANT: Before running --execute, keep all six button positions unchanged.')
    return coords


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--excel', type=Path, default=XLSX)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--limit', type=int, default=1)
    parser.add_argument('--setup', action='store_true', help='Record 6 on-screen Add task positions')
    parser.add_argument('--countdown', type=int, default=8, help='Seconds to hover before capturing each bucket (default 8)')
    args = parser.parse_args()
    if not args.excel.is_file():
        sys.exit(f'Excel not found: {args.excel} (put the script and workbook in the same folder)')
    rows = rows_from_excel(args.excel)
    already = previous_submissions()
    # Previously manually-created Planner task: avoid duplicate using known exact title.
    manually_created = {'arrange mi reporting training with partners'}
    rows = [r for r in rows if str(r.get('TaskKey')) not in already and norm(r.get('TaskTitle')) not in manually_created]
    print(f'Pending tasks eligible for this local run: {len(rows)}')
    for r in rows:
        print(f"  [{r['Bucket']}] {r['TaskTitle']}")
    if not args.execute and not args.setup:
        print('\nPREVIEW ONLY. Your browser was not touched.')
        print('To test one task: py import_connect_to_work_planner_existing_chrome.py --execute --limit 1')
        return

    try:
        import pyautogui as gui
        import pyperclip
    except ImportError:
        sys.exit('Install dependencies: py -m pip install openpyxl pyautogui pyperclip')
    gui.FAILSAFE = True  # move mouse to top-left corner to abort
    gui.PAUSE = 0.35
    if args.setup:
        if args.countdown < 3:
            sys.exit('--countdown must be at least 3 seconds')
        capture_coordinates(gui, seconds=args.countdown)
        return
    if not rows:
        return
    if args.limit < 1:
        sys.exit('For safety, --limit must be at least 1.')
    invalid = {str(r['Bucket']).strip() for r in rows} - set(BUCKETS)
    if invalid:
        sys.exit('Unexpected bucket(s): ' + ', '.join(invalid))
    if not POINTS.exists():
        print('First capture bucket positions with --setup; then rerun.')
        return
    coords = json.loads(POINTS.read_text(encoding='utf-8'))
    try:
        validate_coordinates(coords)
    except ValueError as exc:
        sys.exit(f'UNSAFE saved setup: {exc}')
    print('\nCAUTION: Uses mouse + keyboard in your EXISTING screen, Chrome must stay in foreground.')
    print('This version creates TASK TITLES ONLY, not descriptions, dates or priorities.')
    print('It cannot reliably detect existing Planner tasks. Do NOT import all tasks without checking duplicates.')
    print('Move your mouse to the very TOP-LEFT corner to stop at any time.')
    if input('Type TEST to confirm that the correct Connect to Work BOARD is open: ').strip() != 'TEST':
        return
    print('Switch to your existing Planner Chrome window NOW. Starting in 5 seconds...')
    time.sleep(5)
    attempted = 0
    for row in rows:
        if attempted >= args.limit:
            break
        name = str(row['Bucket']).strip()
        title = str(row['TaskTitle']).strip()
        x, y = coords[name]
        gui.click(x, y)
        time.sleep(0.55)
        # Clicked Add task; browser versions may not focus text input. Stop safely
        # for first item to let user verify visually before typing.
        if attempted == 0:
            print(f'\nI clicked Add task for: {name}')
            print('Check Chrome: is the new-task TITLE input focused?')
            if input('Type YES to paste the title, otherwise press ENTER to stop: ').strip() != 'YES':
                print('Stopped without submitting a task.')
                return
            print('Focus Chrome again. Pasting in 4 seconds...')
            time.sleep(4)
        pyperclip.copy(title)
        gui.hotkey('ctrl', 'v')
        time.sleep(0.3)
        # Create via Enter in the inline card. If UI differs, stop; do not retry
        # blindly because this could accidentally create duplicates.
        gui.press('enter')
        attempted += 1
        record(row, 'SUBMITTED')
        print(f'SUBMITTED (not remotely verified): {title}')
        print('Verify the card is visible in Planner before any further run.')
        if attempted < args.limit:
            if input('Type NEXT only after checking the previous task was created correctly: ').strip() != 'NEXT':
                break
            print('Return to Planner. Continuing in 3 seconds...')
            time.sleep(3)
    print(f'Finished. Submission log: {LOG}')
    print('IMPORTANT: If the task was NOT created, inspect the log before retrying.')


if __name__ == '__main__':
    main()

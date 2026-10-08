"""Import Connect to Work Excel tasks using the existing Microsoft Planner web UI.

This is browser automation, not a Microsoft Graph/API integration. Microsoft
changes the Planner UI; if a selector changes, the script STOPS and saves a
screenshot rather than guessing where to click.

Commands:
  py -m pip install openpyxl playwright
  py -m playwright install chromium
  py import_connect_to_work_planner.py
  py import_connect_to_work_planner.py --execute --limit 1
  py import_connect_to_work_planner.py --execute
"""
import argparse
import csv
import re
import sys
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

HERE = Path(__file__).resolve().parent
DEFAULT_XLSX = HERE / 'Connect_to_Work_Planner_Task_Import.xlsx'
BROWSER_PROFILE = HERE / 'planner_browser_profile'  # local login session
LOG_FILE = HERE / 'planner_import_log.csv'
BUCKETS = [
    'Mobilisation & Partners', 'Lot 1 – Saheli Lead',
    'Lot 3 – Witton Lodge Lead', 'Finance & MI Reporting',
    'Compliance & Data Quality', 'Performance & Follow-ups'
]


def get_rows(path):
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb['Planner Import']
    it = ws.values
    headers = next(it)
    rows = [dict(zip(headers, values)) for values in it]
    wb.close()
    return [r for r in rows if str(r.get('TaskTitle') or '').strip()]


def scrub(text):
    return ' '.join(str(text or '').casefold().split())


def date_as_text(value):
    if isinstance(value, (datetime, date)):
        return value.strftime('%Y-%m-%d')
    return str(value or '').strip()


def log(row, result, note=''):
    existed = LOG_FILE.exists()
    with LOG_FILE.open('a', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        if not existed:
            writer.writerow(['TaskKey', 'Bucket', 'TaskTitle', 'Result', 'Note'])
        writer.writerow([row.get('TaskKey'), row.get('Bucket'), row.get('TaskTitle'), result, note])


def first_visible(*locators):
    for locator in locators:
        try:
            if locator.count() and locator.first.is_visible():
                return locator.first
        except Exception:
            pass
    return None


def find_bucket(page, name):
    # Match an exact bucket title in a board column. Avoid selecting a similarly
    # named task or an item in a sidebar.
    heading = first_visible(
        page.get_by_role('heading', name=name, exact=True),
        page.get_by_text(name, exact=True),
    )
    if heading is None:
        raise RuntimeError(f'Cannot find Planner bucket: {name!r}. Switch to Board view and ensure buckets exist.')
    # Planner renders columns in several ways depending on its release.
    # Scope to the nearest container that has an Add task control.
    for level in range(1, 7):
        column = heading.locator('xpath=' + '/..' * level)
        button = first_visible(
            column.get_by_role('button', name=re.compile(r'add (a )?task', re.I)),
            column.get_by_text(re.compile(r'^add (a )?task$', re.I)),
        )
        if button is not None:
            return column, button
    raise RuntimeError(f'Could not find an Add task button within bucket {name!r}.')


def existing_titles_in_bucket(column):
    # Only exact titles are checked via DOM text within the correct column. The
    # UI might lazy-load cards; for safety, visible content is not sufficient
    # when the board contains more tasks than can be rendered.
    return scrub(column.inner_text(timeout=10000))


def is_existing_anywhere(page, title):
    # NOTE: A title might be in a collapsed / virtualized bucket. A separate
    # operator confirmation is required before bulk import.
    return bool(page.get_by_text(title, exact=True).count())


def create_task(page, row):
    bucket_name = str(row['Bucket']).strip()
    title = str(row['TaskTitle']).strip()
    column, add = find_bucket(page, bucket_name)
    if is_existing_anywhere(page, title):
        return 'SKIP', 'Exact title already present in visible Planner board'
    add.click()
    page.wait_for_timeout(350)
    input_title = first_visible(
        page.get_by_role('textbox', name=re.compile('task name|task title|name', re.I)),
        page.get_by_placeholder(re.compile('task name|task title|enter a task|add a task|name your task', re.I)),
        page.locator('input[aria-label*="Task"]'),
        page.locator('input[placeholder*="task"]'),
    )
    if input_title is None:
        raise RuntimeError('Could not locate the new-task title input. Planner UI may have changed.')
    input_title.fill(title)
    # Prefer explicitly named creation buttons. ENTER fallback only when
    # one is not visible, as some Planner versions use inline quick add.
    create = first_visible(
        page.get_by_role('button', name=re.compile(r'^(add task|add|create task|create)$', re.I)),
    )
    if create is not None:
        create.click()
    else:
        input_title.press('Enter')
    page.wait_for_timeout(800)
    if not is_existing_anywhere(page, title):
        raise RuntimeError('Task was not confirmed on Planner board after submission; verify manually before retrying.')
    return 'CREATED', 'Created title only. Add description, deadline, priority and checklist manually if required.'


def main():
    parser = argparse.ArgumentParser(description='Safe browser importer for Microsoft Planner')
    parser.add_argument('--excel', type=Path, default=DEFAULT_XLSX)
    parser.add_argument('--execute', action='store_true', help='Actually create tasks (default is preview)')
    parser.add_argument('--limit', type=int, default=0, help='Maximum new tasks to create; try 1 first')
    args = parser.parse_args()
    if not args.excel.exists():
        sys.exit(f'Workbook not found: {args.excel}')
    rows = get_rows(args.excel)
    pending = [r for r in rows if scrub(r.get('ImportStatus')) in ('', 'pending') and not str(r.get('PlannerTaskID') or '').strip()]
    print(f'Workbook: {args.excel.name} | Total: {len(rows)} | Pending: {len(pending)}')
    for r in pending:
        print(f"  {r.get('TaskKey')}: [{r.get('Bucket')}] {r.get('TaskTitle')} (due: {date_as_text(r.get('DueDateISO')) or 'not set'})")
    if not args.execute:
        print('\nPREVIEW ONLY. No browser was opened and no tasks were created.')
        print('To test just ONE task: py import_connect_to_work_planner.py --execute --limit 1')
        return
    bad = sorted({str(r['Bucket']).strip() for r in pending} - set(BUCKETS))
    if bad:
        sys.exit('Unexpected bucket names: ' + ', '.join(bad))
    print('\nIMPORTANT: this script creates TITLES in the correct buckets only.')
    print('It does not currently transfer task descriptions, priorities or due dates.')
    print('Check the existing MI training task and other duplicates before running the full import.')
    if input('Type IMPORT to open Planner and continue: ').strip() != 'IMPORT':
        print('Cancelled.'); return
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(str(BROWSER_PROFILE), headless=False, viewport={'width': 1450, 'height': 940})
        page = context.pages[0] if context.pages else context.new_page()
        page.goto('https://planner.cloud.microsoft/', wait_until='domcontentloaded')
        print('\nLog in to Microsoft 365 if needed. Open the existing Connect to Work plan in BOARD view.')
        input('When its six buckets are visible, press ENTER here... ')
        if not first_visible(page.get_by_text('Connect to Work', exact=True)):
            print('WARNING: Could not verify the plan name. Double-check the opened plan.')
            if input('Type YES if the correct plan is open: ') != 'YES':
                context.close(); return
        created = 0
        for row in pending:
            if args.limit and created >= args.limit:
                break
            try:
                result, note = create_task(page, row)
                print(f"{result}: {row['TaskTitle']} — {note}")
                log(row, result, note)
                if result == 'CREATED':
                    created += 1
            except Exception as exc:
                screenshot = HERE / 'planner_import_error.png'
                page.screenshot(path=str(screenshot), full_page=True)
                print(f'IMPORT STOPPED: {exc}\nScreenshot: {screenshot}')
                log(row, 'ERROR', str(exc))
                break
        print(f'Created: {created}. Log: {LOG_FILE}')
        input('Press ENTER to close the automated browser... ')
        context.close()


if __name__ == '__main__':
    main()

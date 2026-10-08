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
ALREADY_CREATED_TITLES = {
    'arrange mi reporting training with partners',
    'request partner delivery-area and activity breakdown',
    'confirm partner names and contact details',
}
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


def choose_planner_tab(context):
    """Select the open plan tab, not the initial landing page."""
    pages = [p for p in context.pages if not p.is_closed()]
    print('\nDetected browser tabs:')
    for i, p in enumerate(pages, 1):
        try:
            print(f'  {i}. {p.title()} | {p.url}')
        except Exception:
            print(f'  {i}. {p.url}')
    candidates = [p for p in pages if 'planner' in p.url.lower() or 'tasks.office.com' in p.url.lower()]
    if not candidates:
        raise RuntimeError('No Microsoft Planner browser tab is open. Open your Connect to Work plan first.')
    if len(candidates) == 1:
        page = candidates[0]
    else:
        print('Multiple Planner tabs are open. Choose the number showing Connect to Work.')
        while True:
            answer = input('Planner tab number: ').strip()
            if answer.isdigit() and 1 <= int(answer) <= len(pages) and pages[int(answer)-1] in candidates:
                page = pages[int(answer)-1]
                break
            print('Please enter a Planner tab number from the list.')
    page.bring_to_front()
    page.wait_for_load_state('domcontentloaded')
    print(f'Using tab: {page.url}')
    return page


def find_bucket(page, name):
    # Match an exact bucket title in a board column. Avoid selecting a similarly
    # named task or an item in a sidebar.
    heading = first_visible(
        page.get_by_role('heading', name=name, exact=True),
        page.get_by_text(name, exact=True),
    )
    if heading is None:
        try:
            text = page.locator('body').inner_text(timeout=5000)
            print('Planner visible page text (first 1200 chars):', repr(text[:1200]))
        except Exception:
            pass
        raise RuntimeError(f'Cannot find Planner bucket: {name!r}. Check the selected browser tab and Board view; attach planner_import_error.png if needed.')
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
    # The Planner board is updated asynchronously; 0.8 seconds is often too short.
    # Wait for an exact-title element to appear, not for an arbitrary fixed delay.
    # Note: failure here is ambiguous: Planner may already have saved the task.
    try:
        page.get_by_text(title, exact=True).first.wait_for(state='visible', timeout=12000)
    except PlaywrightTimeout:
        raise RuntimeError(
            'Task submission was not confirmed within 12 seconds. It MAY have been created. '
            'Check Planner for this exact title before restarting. Do not retry blindly: ' + title
        )
    return 'CREATED', 'Exact task title appeared on Planner board after submission (title only).'


def main():
    parser = argparse.ArgumentParser(description='Safe browser importer for Microsoft Planner')
    parser.add_argument('--excel', type=Path, default=DEFAULT_XLSX)
    parser.add_argument('--execute', action='store_true', help='Actually create tasks (default is preview)')
    parser.add_argument('--limit', type=int, default=0, help='Maximum task rows to attempt; try 1 first')
    args = parser.parse_args()
    if not args.excel.exists():
        sys.exit(f'Workbook not found: {args.excel}')
    rows = get_rows(args.excel)
    logged_created = set()
    if LOG_FILE.exists():
        with LOG_FILE.open('r', encoding='utf-8-sig', newline='') as handle:
            for event in csv.DictReader(handle):
                if scrub(event.get('Result')) == 'created':
                    logged_created.add(scrub(event.get('TaskTitle')))
    pending = [r for r in rows if scrub(r.get('ImportStatus')) in ('', 'pending')
               and not str(r.get('PlannerTaskID') or '').strip()
               and scrub(r.get('TaskTitle')) not in ALREADY_CREATED_TITLES
               and scrub(r.get('TaskTitle')) not in logged_created]
    already_created = [r for r in rows if scrub(r.get('TaskTitle')) in ALREADY_CREATED_TITLES]
    if already_created:
        print('Skipping known existing Planner task(s): ' + ', '.join(str(r['TaskTitle']) for r in already_created))
    if logged_created:
        print(f'Skipping {len(logged_created)} title(s) previously confirmed CREATED in the local log.')
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
    print('Known existing tasks are skipped; verify other possible duplicates before importing.')
    if input('Type IMPORT to open Planner and continue: ').strip() != 'IMPORT':
        print('Cancelled.'); return
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(str(BROWSER_PROFILE), headless=False, viewport={'width': 1450, 'height': 940})
        page = context.pages[0] if context.pages else context.new_page()
        if not any('planner' in p.url.lower() for p in context.pages):
            page.goto('https://planner.cloud.microsoft/', wait_until='domcontentloaded')
        print('\nLog in to Microsoft 365 if needed. Open the existing Connect to Work plan in BOARD view.')
        input('When you have opened Connect to Work in BOARD view, press ENTER here... ')
        page = choose_planner_tab(context)
        if not first_visible(page.get_by_text('Connect to Work', exact=True), page.get_by_role('heading', name='Connect to Work', exact=True)):

            print('WARNING: Could not verify the plan name. Double-check the opened plan.')
            if input('Type YES if the correct plan is open: ') != 'YES':
                context.close(); return
        created = 0
        processed = 0
        for row in pending:
            if args.limit and processed >= args.limit:
                break
            try:
                processed += 1
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

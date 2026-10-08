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
import os
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


def automate_microsoft_signin(context, email):
    """Best-effort email and passkey METHOD selection, never passkey approval."""
    import time
    deadline = time.monotonic() + 150
    email_sent = False
    sign_in_clicked = False
    passkey_selected = False
    print('Waiting for Microsoft sign-in and the passkey choice...')
    while time.monotonic() < deadline:
        pages = [p for p in context.pages if not p.is_closed()]
        for page in pages:
            try:
                url = page.url.lower()
                if '/webui/plan/' in url and '/view/board' in url:
                    print('Planner Board view is already open.')
                    return
                if not any(x in url for x in ('microsoft', 'office', 'planner', 'login.live.com')):
                    continue
                # Sign-in methods can appear on the identity-provider page or
                # in a separate browser tab. Never interact with Windows
                # Security/OS passkey dialogs, which require user presence.
                method = first_visible(
                    page.get_by_role('button', name=re.compile(r'^(sign in with a passkey|use a passkey|passkey)$', re.I)),
                    page.get_by_role('link', name=re.compile(r'^(sign in with a passkey|use a passkey|passkey)$', re.I)),
                    page.get_by_text(re.compile(r'^(sign in with a passkey|use a passkey)$', re.I)),
                )
                if method is not None and not passkey_selected:
                    method.click(timeout=5000)
                    passkey_selected = True
                    print('Selected passkey sign-in. Finish Windows Security / Bluetooth / phone approval manually.')
                    return
                if 'login.microsoftonline.com' in url or 'login.live.com' in url:
                    field = first_visible(page.locator('input[type="email"]'), page.locator('input[name="loginfmt"]'), page.locator('input#i0116'))
                    if field is not None and email and not email_sent:
                        field.fill(email)
                        next_button = first_visible(page.locator('input#idSIButton9'), page.get_by_role('button', name=re.compile(r'^(next|continue|sign in)$', re.I)))
                        if next_button is not None:
                            next_button.click(timeout=5000)
                        else:
                            field.press('Enter')
                        email_sent = True
                        print('Email submitted. Looking for the passkey selection...')
                        continue
                    # Some Microsoft tenants first offer the alternate-method chooser.
                    if email_sent and not passkey_selected:
                        other = first_visible(
                            page.get_by_role('link', name=re.compile(r'^(sign-in options|other ways to sign in|use another method)$', re.I)),
                            page.get_by_role('button', name=re.compile(r'^(sign-in options|other ways to sign in|use another method)$', re.I)),
                        )
                        if other is not None:
                            other.click(timeout=5000)
                            print('Opened sign-in options; looking for passkey...')
                            page.wait_for_timeout(900)
                            continue
                elif 'planner' in url and not sign_in_clicked:
                    signin = first_visible(
                        page.get_by_role('button', name=re.compile(r'^sign in$', re.I)),
                        page.get_by_role('link', name=re.compile(r'^sign in$', re.I)),
                    )
                    if signin is not None:
                        signin.click(timeout=5000)
                        sign_in_clicked = True
                        print('Clicked Sign in.')
            except Exception:
                # Microsoft frequently replaces elements during navigation.
                pass
        if pages:
            pages[0].wait_for_timeout(700)
    print('Could not automatically find the passkey option. Select it manually in the browser.')

def wait_for_planner_board(context):
    """Wait for the user to finish the passkey ceremony, without pretending to approve it."""
    print('Complete passkey authentication in the browser (enable Bluetooth in Windows if asked).')
    print('Waiting up to 5 minutes for the Connect to Work Board view...')
    import time
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        for page in context.pages:
            if not page.is_closed() and '/webui/plan/' in page.url.lower() and '/view/board' in page.url.lower():
                return page
        context.pages[0].wait_for_timeout(1000)
    raise RuntimeError('Planner Board view was not opened within 5 minutes. Sign in and navigate there, then retry.')


def find_bucket(page, name):
    """Locate the exact Planner bucket; columns may mount after navigation."""
    import time
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        columns = page.locator('[data-testid="task-board-column"]')
        heading = page.get_by_role('heading', name=name, exact=True)
        matches = columns.filter(has=heading)
        if matches.count() == 1:
            return matches.first
        matches = columns.filter(has=page.get_by_text(name, exact=True))
        if matches.count() == 1:
            return matches.first
        # Planner sometimes exposes the accessible column name before headings.
        matches = columns.filter(has=page.locator('[aria-label]')).filter(
            has_text=re.compile(re.escape(name), re.I)
        )
        if matches.count() == 1:
            return matches.first
        page.wait_for_timeout(500)
    count = page.locator('[data-testid="task-board-column"]').count()
    raise RuntimeError(
        f"Cannot find bucket {name!r} after waiting. Board column count={count}. "
        "The Planner Board may still be loading, or a login/modal may be covering it. "
        "Check the saved screenshot before another import attempt."
    )


def wait_until_board_ready(page):
    """URL alone is not readiness: Planner often renders columns later."""
    import time
    print('Waiting for the actual Planner board columns to finish loading...')
    deadline = time.monotonic() + 75
    reloaded = False
    while time.monotonic() < deadline:
        try:
            columns = page.locator('[data-testid="task-board-column"]')
            # Require a known bucket, not just an arbitrary board-like element.
            if columns.count() and any(
                columns.filter(has_text=re.compile(re.escape(name), re.I)).count()
                for name in BUCKETS
            ):
                print(f'Planner board loaded: {columns.count()} visible bucket columns.')
                return
            if not reloaded and time.monotonic() > deadline - 45:
                print('Columns not visible yet; reloading Planner once...')
                page.reload(wait_until='domcontentloaded', timeout=30000)
                reloaded = True
        except PlaywrightTimeout:
            pass
        page.wait_for_timeout(1000)
    raise RuntimeError(
        'The Planner URL opened, but task-board columns never appeared. '
        'Please check the error screenshot for a sign-in prompt, loading screen, '
        'or a different Planner view. No tasks were submitted.'
    )


def open_quick_add(page, column, bucket_name):
    """Open quick-add within THIS bucket; Planner can mount controls on hover.

    A conventional Playwright click on the inner Add task span was intercepted
    by its surrounding card/neighboring column after adding the first task.
    Dispatching a DOM click on the scoped control avoids misdirected screen clicks.
    """
    column.scroll_into_view_if_needed(timeout=15000)
    column.hover(timeout=10000)
    page.wait_for_timeout(250)

    control = column.locator('[data-testid="task-board-add-card-control"]')
    label = column.get_by_text(re.compile(r'^add (a )?task$', re.I))
    if control.count():
        # The card itself is the event target, not its overlapping text label.
        control.first.evaluate('(el) => el.click()')
    elif label.count():
        # Some Planner builds add the control only on hover or omit its test id.
        # Click the *scoped* label via DOM events (not force-click coordinates).
        label.first.evaluate('(el) => el.click()')
    else:
        raise RuntimeError(
            f'No Add task control or text found inside {bucket_name!r} even after hover. '
            'Planner UI may have changed; screenshot saved. No click attempted.'
        )


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
    column = find_bucket(page, bucket_name)
    if is_existing_anywhere(page, title):
        return 'SKIP', 'Exact title already present in visible Planner board'
    open_quick_add(page, column, bucket_name)
    page.wait_for_timeout(350)
    # Only use an actual editable control in the requested column.
    # Existing Planner task titles have role="textbox" on non-editable DIVs,
    # so page-wide get_by_role("textbox", name="...") is unsafe here.
    editable_selector = (
        'input:visible, textarea:visible, '
        '[contenteditable="true"]:visible, [contenteditable=""]:visible'
    )
    input_title = None
    for attempt in range(15):
        candidates = column.locator(editable_selector)
        for idx in range(candidates.count()):
            candidate = candidates.nth(idx)
            try:
                if candidate.get_attribute('data-testid') == 'task-card-title':
                    continue
                if candidate.is_enabled() and candidate.is_visible():
                    input_title = candidate
                    break
            except Exception:
                continue
        if input_title is not None:
            break
        page.wait_for_timeout(300)
    if input_title is None:
        raise RuntimeError(
            f'Quick-add was clicked in {bucket_name!r}, but there is no editable '
            'input in that column. Stopping to protect existing task cards.'
        )
    input_title.fill(title)
    # Limit the Create/Add button to the same bucket. Never submit a button
    # in another column or task details pane.
    create = first_visible(
        column.get_by_role('button', name=re.compile(r'^(add task|add|create task|create)$', re.I)),
    )
    if create is not None:
        create.click()
    else:
        input_title.press('Enter')
    # The Planner board is updated asynchronously; 0.8 seconds is often too short.
    # Wait for an exact-title element to appear, not for an arbitrary fixed delay.
    # Note: failure here is ambiguous: Planner may already have saved the task.
    try:
        column.get_by_text(title, exact=True).first.wait_for(state='visible', timeout=12000)
    except PlaywrightTimeout:
        raise RuntimeError(
            'Task submission was not confirmed within 12 seconds. It MAY have been created. '
            'Check Planner for this exact title before restarting. Do not retry blindly: ' + title
        )
    return 'CREATED', f'Exact task title appeared in {bucket_name} after submission (title only).'


def main():
    parser = argparse.ArgumentParser(description='Safe browser importer for Microsoft Planner')
    parser.add_argument('--excel', type=Path, default=DEFAULT_XLSX)
    parser.add_argument('--execute', action='store_true', help='Actually create tasks (default is preview)')
    parser.add_argument('--email', default=os.getenv('PLANNER_LOGIN_EMAIL', ''), help='Microsoft sign-in email; no passwords or passkeys are stored')
    parser.add_argument('--login-only', action='store_true', help='Only navigate to Microsoft sign-in/passkey; never create tasks')
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
    if not args.execute and not args.login_only:
        print('\nPREVIEW ONLY. No browser was opened and no tasks were created.')
        print('To test just ONE task: py import_connect_to_work_planner.py --execute --limit 1')
        return
    bad = sorted({str(r['Bucket']).strip() for r in pending} - set(BUCKETS))
    if bad:
        sys.exit('Unexpected bucket names: ' + ', '.join(bad))
    print('\nIMPORTANT: this script creates TITLES in the correct buckets only.')
    print('It does not currently transfer task descriptions, priorities or due dates.')
    print('Known existing tasks are skipped; verify other possible duplicates before importing.')
    print('Starting browser automatically. Task creation will begin only once Planner Board view is open.')
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(str(BROWSER_PROFILE), headless=False, viewport={'width': 1450, 'height': 940})
        page = context.pages[0] if context.pages else context.new_page()
        if not any('planner' in p.url.lower() for p in context.pages):
            page.goto('https://planner.cloud.microsoft/', wait_until='domcontentloaded')
        print('\nStarting Microsoft sign-in automation (passkey approval remains manual).')
        if not args.email:
            print('Tip: add --email your@company.co.uk to prefill the sign-in email.')
        automate_microsoft_signin(context, args.email)
        if args.login_only:
            print('LOGIN ONLY: No Planner tasks will be created. Complete the passkey yourself.')
            input('Press ENTER to close this browser when you are finished... ')
            context.close()
            return
        page = wait_for_planner_board(context)
        page = choose_planner_tab(context)
        if 'hoF-jE4uqkqoh39k06WAH5gAHu5t' not in page.url and not first_visible(page.get_by_text('Connect to Work', exact=True), page.get_by_role('heading', name='Connect to Work', exact=True)):

            print('WARNING: Could not verify the plan name. Double-check the opened plan.')
            if input('Type YES if the correct plan is open: ') != 'YES':
                context.close(); return
        try:
            wait_until_board_ready(page)
        except Exception as exc:
            screenshot = HERE / 'planner_import_error.png'
            page.screenshot(path=str(screenshot), full_page=True)
            print(f'IMPORT STOPPED: {exc}\nScreenshot: {screenshot}')
            input('Press ENTER to close the automated browser... ')
            context.close()
            return
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

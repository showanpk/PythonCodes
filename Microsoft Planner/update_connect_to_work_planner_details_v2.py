"""Fill details of ALREADY EXISTING tasks in the user's Connect to Work Planner board.

Preview: py update_connect_to_work_planner_details.py
Inspect: py update_connect_to_work_planner_details.py --inspect --limit 1
Test:    py update_connect_to_work_planner_details.py --execute --limit 1

No tasks are created or deleted. Existing nonempty description/checklist are preserved.
Due dates from Excel are date-only (no fabricated hours). Owner names are not
assigned automatically (ambiguous Microsoft 365 identities).
"""
import argparse
import csv
import json
import re
import time
from datetime import date, datetime
from pathlib import Path
from openpyxl import load_workbook
from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
EXCEL = HERE / 'Connect_to_Work_Planner_Task_Import.xlsx'
PROFILE = HERE / 'planner_browser_profile'
LOG = HERE / 'planner_details_update_log.csv'
PLAN_ID = 'hoF-jE4uqkqoh39k06WAH5gAHu5t'


def safe_name(s): return ' '.join(str(s or '').split()).casefold()

def date_text(v):
    if isinstance(v, (datetime,date)): return v.strftime('%Y-%m-%d')
    return str(v or '').strip()

def load():
    wb=load_workbook(EXCEL,read_only=True,data_only=True)
    ws=wb['Planner Import']; iterator=iter(ws.values); keys=next(iterator)
    rows=[dict(zip(keys,values)) for values in iterator]
    wb.close()
    return [r for r in rows if r.get('TaskTitle')]

def record(row,field,status,reason):
    exists=LOG.exists()
    with LOG.open('a',encoding='utf-8-sig',newline='') as f:
        w=csv.writer(f)
        if not exists: w.writerow(['TaskKey','TaskTitle','Field','Status','Detail'])
        w.writerow([row['TaskKey'],row['TaskTitle'],field,status,reason[:250]])

def visible(loc):
    try:
        return loc.count()>0 and loc.first.is_visible()
    except Exception: return False

def choose_page(context):
    for _ in range(300):
        for p in context.pages:
            if PLAN_ID in p.url and '/view/board' in p.url:
                cols=p.locator('[data-testid="task-board-column"]')
                if cols.count()>0: return p
        time.sleep(1)
    raise RuntimeError('Connect to Work board not loaded after 5 minutes; finish sign-in and navigate there.')

def get_card(page,row):
    bucket=str(row['Bucket']).strip(); title=str(row['TaskTitle']).strip()
    columns=page.locator('[data-testid="task-board-column"]')
    col=columns.filter(has=page.get_by_role('heading',name=bucket,exact=True))
    if col.count()!=1: col=columns.filter(has_text=re.compile(re.escape(bucket)))
    if col.count()!=1: raise RuntimeError('Cannot uniquely locate bucket '+bucket)
    # Use the actual card-title test ID to avoid matching input fields or descriptions.
    titles=col.first.locator('[data-testid="task-card-title"]')
    found=titles.filter(has_text=re.compile('^\\s*'+re.escape(title)+'\\s*$',re.I))
    if found.count()!=1: return None
    return found.first

def get_panel(page):
    # The inspection proves Planner uses this exact outer task dialog.
    # Never select a nested task-details/editor element as the panel.
    dialogs=page.locator('[data-testid="task-details-dialog"]')
    for idx in range(dialogs.count()-1,-1,-1):
        if dialogs.nth(idx).is_visible(): return dialogs.nth(idx)
    return None


def inspect_panel(page,panel,row):
    path=HERE/'planner_details_inspection.json'
    data=panel.evaluate('''el => Array.from(el.querySelectorAll('input,textarea,button,[role="button"],[role="textbox"],[contenteditable],select,[data-testid]')).slice(0,250).map(e=>({tag:e.tagName,role:e.getAttribute('role'),testId:e.getAttribute('data-testid'),label:e.getAttribute('aria-label'),placeholder:e.getAttribute('placeholder'),type:e.getAttribute('type'),text:(e.innerText||e.value||'').slice(0,130)}))''')
    path.write_text(json.dumps({'task':row['TaskTitle'],'elements':data},indent=2,ensure_ascii=False),encoding='utf-8')
    img=HERE/'planner_details_inspection.png'; page.screenshot(path=str(img),full_page=False)
    print(f'Inspection saved: {path} and {img}')

def update_description(page,panel,row):
    value=str(row.get('Description') or '').strip()
    if not value: return 'SKIP','No description in Excel'
    # The inspected Planner UI has a rich-text Notes editor, separate from Task chat.
    root=panel.locator('[data-testid="task-notes"] [data-testid="rich-text-editor"]')
    if root.count()!=1: return 'REVIEW','Could not uniquely find Notes rich-text editor'
    editor=root.locator('[role="textbox"][contenteditable="true"]')
    if editor.count()!=1: return 'REVIEW','Notes textbox is not contenteditable; no change'
    old=editor.inner_text().strip()
    if old: return 'SKIP','Existing Notes preserved; no overwrite'
    editor.fill(value)
    editor.press('Tab')
    page.wait_for_timeout(750)
    return 'UPDATED','Notes entered; verify saved after reopening'


def update_due(page,panel,row):
    value=date_text(row.get('DueDateISO'))
    if not value: return 'SKIP','No due date in Excel'
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}",value): return 'REVIEW','Invalid due date in workbook'
    field=panel.locator('[data-testid="task-editor-due-date"] input[data-testid="date-picker"]')
    if field.count()!=1: return 'REVIEW','Due date picker not unique'
    old=field.input_value().strip()
    target=datetime.strptime(value,'%Y-%m-%d').strftime('%d/%m/%Y')
    if old:
        if old==target: return 'SKIP','Due date already matches Excel'
        return 'REVIEW',f'Existing due date {old} differs from Excel {target}; preserved'
    # Confirmed inspection showed a plain text date-picker input displaying dd/mm/yyyy.
    field.fill(target)
    field.press('Tab')
    page.wait_for_timeout(750)
    after=field.input_value().strip()
    if after!=target:return 'REVIEW',f'Date entry not confirmed (input now {after!r}); check manually'
    return 'UPDATED',f'Due date entered {target}; verify after reopening'


def update_priority(page,panel,row):
    excel_priority=str(row.get('Priority') or '').strip()
    if not excel_priority: return 'SKIP','No priority in Excel'
    # The Planner UI displays Urgent / Important / Medium / Low, while Excel
    # uses Urgent / High / Normal. Do not search for Excel-specific labels.
    mapping={'urgent':'Urgent','high':'Important','important':'Important',
             'normal':'Medium','medium':'Medium','low':'Low'}
    wanted=mapping.get(safe_name(excel_priority))
    if not wanted: return 'REVIEW',f'Unrecognized Excel priority {excel_priority!r}; preserved'
    root=panel.locator('[data-testid="task-editor-priority"]')
    select=root.get_by_role('combobox',name='Priority')
    if select.count()!=1: return 'REVIEW','Could not uniquely identify Priority dropdown'
    original=select.inner_text().strip()
    if safe_name(original)==safe_name(wanted):
        return 'SKIP',f'Priority already {wanted} (Excel: {excel_priority})'
    # Only change Planner's default Medium. Preserve any other existing priority.
    if original and safe_name(original) not in ('medium','normal','not set','none'):
        return 'REVIEW',f'Existing Planner priority {original!r} differs from Excel {excel_priority!r} -> {wanted!r}; preserved'
    select.click()
    page.wait_for_timeout(450)
    selectors=[
        page.get_by_role('option',name=wanted,exact=True),
        page.get_by_role('menuitem',name=wanted,exact=True),
        page.locator('[role="listbox"] [role="option"]').filter(has_text=re.compile(r'^\s*'+re.escape(wanted)+r'\s*$',re.I)),
    ]
    matched=None
    for locator in selectors:
        shown=[locator.nth(i) for i in range(locator.count()) if locator.nth(i).is_visible()]
        if len(shown)==1: matched=shown[0];break
        if len(shown)>1:
            page.keyboard.press('Escape')
            return 'REVIEW',f'Ambiguous Planner priority choice {wanted!r}; preserved'
    if matched is None:
        (HERE/'planner_priority_options.txt').write_text(repr(page.locator('[role="listbox"], [role="menu"], [data-testid*="dropdown"]').all_inner_texts()),encoding='utf-8')
        page.keyboard.press('Escape')
        return 'REVIEW',f'Could not safely identify Planner option {wanted!r}; see planner_priority_options.txt'
    matched.click()
    page.wait_for_timeout(700)
    actual=select.inner_text().strip()
    if safe_name(actual)!=safe_name(wanted):
        return 'REVIEW',f'Selected {wanted!r}, but Planner displays {actual!r}; verify manually'
    return 'UPDATED',f'Priority set to {wanted} (Excel: {excel_priority})'


def update_checklist(page,panel,row):
    values=[x.strip() for x in str(row.get('Checklist (pipe-separated)') or '').split('|') if x.strip()]
    if not values:return 'SKIP','No checklist in Excel'
    root=panel.locator('[data-testid="checklist-container"]')
    if root.count()!=1:return 'REVIEW','Checklist container missing or ambiguous'
    existing=[safe_name(root.locator('input[data-testid="checklist-item-title-input"]').nth(i).input_value())
              for i in range(root.locator('input[data-testid="checklist-item-title-input"]').count())]
    missing=[v for v in values if safe_name(v) not in existing]
    if not missing:return 'SKIP',f'All {len(values)} Excel checklist items are present'
    entry=panel.locator('input[data-testid="checklist-new-item-input"]')
    if entry.count()!=1:return 'REVIEW','Add checklist item input not unique'
    added=0
    for value in missing:
        entry.fill(value)
        entry.press('Enter')
        page.wait_for_timeout(500)
        now=[safe_name(root.locator('input[data-testid="checklist-item-title-input"]').nth(i).input_value())
             for i in range(root.locator('input[data-testid="checklist-item-title-input"]').count())]
        if safe_name(value) not in now:
            return 'REVIEW',f'Added {added}; cannot confirm checklist item {value!r}; stopped'
        added+=1
    return 'UPDATED',f'Added {added} missing checklist items; existing ones preserved'



def enable_board_view(page,panel,row):
    """Enable checklist preview only; never modify the Notes preview setting."""
    label='Show Checklist in board view'
    checkbox=panel.locator('input[data-testid="task-editor-show-on-card-checkbox"][aria-label="'+label+'"]')
    if checkbox.count()!=1:
        checkbox=panel.get_by_role('checkbox',name=label,exact=True)
    if checkbox.count()!=1:
        return 'REVIEW',f'Checklist board-view checkbox not uniquely located ({checkbox.count()}); Notes preview untouched'
    try:
        if checkbox.is_checked():
            return 'SKIP','Checklist preview already enabled; Notes preview untouched'
        checkbox.check(timeout=6000)
        page.wait_for_timeout(550)
        if checkbox.is_checked():
            return 'UPDATED','Checklist preview enabled; Notes preview untouched'
        return 'REVIEW','Checklist preview did not remain enabled; Notes preview untouched'
    except Exception as exc:
        return 'REVIEW',f'Checklist preview could not be enabled: {type(exc).__name__}: {str(exc)[:100]}; Notes untouched'


def close_panel(page,panel):
    button=panel.get_by_role('button',name=re.compile('^(close|dismiss)$',re.I))
    if visible(button): button.first.click()
    else: page.keyboard.press('Escape')
    page.wait_for_timeout(300)

def main():
    a=argparse.ArgumentParser()
    a.add_argument('--execute',action='store_true',help='Update existing task details; never creates new tasks')
    a.add_argument('--inspect',action='store_true',help='Open one task and save UI diagnostics; no edits')
    a.add_argument('--limit',type=int,default=1,help='Maximum tasks (default 1 for safety, 0 means all)')
    a.add_argument('--start-key',default='',help='Start from Excel TaskKey such as CTW-008')
    args=a.parse_args()
    rows=load()
    if args.start_key:
        rows=[r for r in rows if str(r['TaskKey'])>=args.start_key]
    print(f'Excel: {len(rows)} task records selected'+(f' from {args.start_key} onward' if args.start_key else '')+'. Existing Planner tasks only; NO creation/deletion.')
    print('Checklist preview only; Notes preview untouched. Owners are not automatically assigned.')
    if not (args.execute or args.inspect):
        for r in rows:
            checks=[s.strip() for s in str(r.get('Checklist (pipe-separated)') or '').split('|') if s.strip()]
            print(f"{r['TaskKey']} | {r['Bucket']} | {r['TaskTitle']} | due={date_text(r.get('DueDateISO')) or '-'} | priority={r.get('Priority') or '-'} | checklist={len(checks)}")
        print('PREVIEW ONLY. Run --inspect --limit 1 to inspect Planner details; then --execute --limit 1.')
        return
    with sync_playwright() as pw:
        context=pw.chromium.launch_persistent_context(str(PROFILE),headless=False,viewport={'width':1450,'height':940})
        if not context.pages:context.new_page()
        if not any('planner' in p.url for p in context.pages):context.pages[0].goto('https://planner.cloud.microsoft/',wait_until='domcontentloaded')
        print('Finish Microsoft sign-in/passkey yourself, then open Connect to Work Board view. Waiting...')
        try:page=choose_page(context)
        except Exception as ex:
            print(ex);context.close();return
        count=0
        for row in rows:
            if args.limit and count>=args.limit:break
            try:
                card=get_card(page,row)
                if card is None:
                    print('NOT FOUND; skipping without creating:',row['TaskTitle']);continue
                # Close any leftover task editor before selecting this specific card.
                prior=get_panel(page)
                if prior is not None:
                    close_panel(page,prior)
                card.click()
                page.locator('[data-testid="task-details-dialog"] [data-testid="task-editor-title-input"]').first.wait_for(state='visible',timeout=12000)
                page.wait_for_timeout(400)
                panel=get_panel(page)
                if panel is None:raise RuntimeError('Could not identify task details dialog; no edits performed')
                task_title=panel.locator('input[data-testid="task-editor-title-input"]')
                actual=[task_title.nth(i).input_value() for i in range(task_title.count())]
                if len(actual)!=1 or safe_name(actual[0])!=safe_name(row['TaskTitle']):
                    raise RuntimeError(f'Expected {row["TaskTitle"]!r}; opened {actual!r}. No edits performed. Please upload planner_details_error.png')
                if args.inspect:
                    inspect_panel(page,panel,row);close_panel(page,panel);break
                count+=1
                print(f"Updating {row['TaskKey']}: {row['TaskTitle']}")
                for field,method in [('Description',update_description),('DueDate',update_due),('Priority',update_priority),('Checklist',update_checklist),('BoardView',enable_board_view)]:
                    try: status,reason=method(page,panel,row)
                    except Exception as ex: status,reason='REVIEW',f'{type(ex).__name__}: {ex}'
                    print(' ',field,status,reason);record(row,field,status,reason)
                close_panel(page,panel)
            except Exception as ex:
                print('STOPPED:',ex)
                page.screenshot(path=str(HERE/'planner_details_error.png'))
                break
        print(f'Processed {count} existing task(s). Update log: {LOG}')
        input('Press ENTER to close browser... ')
        context.close()

if __name__=='__main__':main()

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
    # Task details open as a dialog or pane; prefer the last visible dialog.
    dialogs=page.get_by_role('dialog')
    for idx in range(dialogs.count()-1,-1,-1):
        if dialogs.nth(idx).is_visible(): return dialogs.nth(idx)
    pane=page.locator('[data-testid*="task-detail"], [data-testid*="task-pane"]')
    if visible(pane): return pane.last
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
    desc=panel.get_by_role('textbox',name=re.compile('description|notes',re.I))
    if not visible(desc):
        desc=panel.locator('textarea[placeholder*="description" i], [contenteditable="true"][aria-label*="description" i], [data-testid*="description"] textarea')
    if not visible(desc): return 'REVIEW','Description editor not identifiable'
    editor=desc.first
    editable=editor.evaluate('el=>el.matches("textarea,input,[contenteditable=true]")')
    if not editable: return 'REVIEW','Description locator was not editable'
    prior=editor.input_value() if editor.evaluate('el=>el.matches("textarea,input")') else editor.inner_text()
    if prior.strip(): return 'SKIP','Existing description preserved'
    editor.fill(value)
    editor.press('Tab')
    page.wait_for_timeout(700)
    return 'UPDATED','Description entered (verify Planner saved it)'

def update_due(page,panel,row):
    value=date_text(row.get('DueDateISO'))
    if not value: return 'SKIP','No due date specified'
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value): return 'REVIEW','Invalid date in workbook'
    field=panel.locator('input[type="date"]')
    if field.count()!=1 or not field.first.is_visible():
        return 'REVIEW','Planner uses a custom date picker; inspect before modifying (safe stop)'
    old=field.input_value().strip()
    if old: return 'SKIP',f'Existing due date preserved: {old}'
    field.fill(value);field.press('Tab');page.wait_for_timeout(500)
    return 'UPDATED',f'Due date entered: {value} (no invented time)'

def update_priority(page,panel,row):
    p=str(row.get('Priority') or '').strip()
    if not p:return 'SKIP','No priority specified'
    # Planner priority menus vary by tenant. Never click a vaguely named control.
    selector=panel.get_by_role('combobox',name=re.compile('^priority$',re.I))
    if not visible(selector): return 'REVIEW',f'Priority ({p}) needs selector mapping'
    if selector.first.evaluate('el=>el.tagName')!='SELECT':
        return 'REVIEW',f'Custom priority dropdown ({p}); inspect first'
    options=selector.first.locator('option')
    mapping=[(options.nth(i).get_attribute('value'),options.nth(i).inner_text().strip()) for i in range(options.count())]
    matched=[val for val,label in mapping if safe_name(label)==safe_name(p)]
    if len(matched)!=1:return 'REVIEW',f'Priority {p} missing in dropdown'
    selector.first.select_option(matched[0]);return 'UPDATED',f'Priority set to {p}'

def update_checklist(page,panel,row):
    values=[x.strip() for x in str(row.get('Checklist (pipe-separated)') or '').split('|') if x.strip()]
    if not values:return 'SKIP','No checklist in workbook'
    # Only add when a distinctly labelled add-checklist control exists.
    existing=panel.get_by_text(re.compile('^'+re.escape(values[0])+'$',re.I))
    if existing.count():return 'SKIP','Checklist appears to exist; preserved'
    add=panel.get_by_role('button',name=re.compile(r'^(add checklist item|add item)$',re.I))
    if not visible(add):return 'REVIEW',f'{len(values)} checklist items waiting: cannot safely identify Add item control'
    for value in values:
        add.first.click()
        field=panel.locator('input[placeholder*="checklist" i], input[placeholder*="item" i]')
        if not visible(field):return 'REVIEW',f'Cannot identify checklist text field; stopped before: {value}'
        field.last.fill(value);field.last.press('Enter');page.wait_for_timeout(300)
    return 'UPDATED',f'Entered {len(values)} checklist items (verify saved)'

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
    print(f'Excel: {len(rows)} task records. Existing Planner tasks only; NO creation/deletion.')
    print('Owners are NOT automatically assigned without verified Microsoft 365 identity.')
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
                card.click();page.wait_for_timeout(850)
                panel=get_panel(page)
                if panel is None:raise RuntimeError('Could not identify task details dialog/pane; no edits performed')
                if args.inspect:
                    inspect_panel(page,panel,row);close_panel(page,panel);break
                count+=1
                print(f"Updating {row['TaskKey']}: {row['TaskTitle']}")
                for field,method in [('Description',update_description),('DueDate',update_due),('Priority',update_priority),('Checklist',update_checklist)]:
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

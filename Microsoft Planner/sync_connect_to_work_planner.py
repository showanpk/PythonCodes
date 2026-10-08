"""Connect to Work: cautious Excel ↔ Planner *change-aware* synchronizer.

First run: --baseline captures current Planner fields, DOES NOT change Planner.
Subsequent runs: default --preview compares live Planner and Excel; --apply changes
only fields changed in Excel since baseline and unchanged in Planner.
Conflicts, task deletion, checklist removal/reordering, assignment and recurrence
are NEVER performed automatically. Manually changed Planner fields are preserved.

Requires the two provided scripts in this folder, openpyxl and playwright.
Login/passkey approval is always performed by the user.
"""
import argparse
import csv
import json
import re
import time
from datetime import datetime, date
from pathlib import Path
from playwright.sync_api import sync_playwright
import import_connect_to_work_planner as imp
import update_connect_to_work_planner_details_v2 as detail

HERE=Path(__file__).resolve().parent
STATE=HERE/'planner_sync_baseline.json'
REPORT=HERE/'planner_sync_preview.csv'
AUDIT=HERE/'planner_sync_audit.csv'
EXCEL=HERE/'Connect_to_Work_Planner_Task_Import.xlsx'
FIELDS=['TaskTitle','Bucket','Description','DueDateISO','Priority','Checklist (pipe-separated)']
PRIORITY={'urgent':'Urgent','high':'Important','important':'Important','normal':'Medium','medium':'Medium','low':'Low'}
REVERSE={'Urgent':'Urgent','Important':'High','Medium':'Normal','Low':'Low'}

def norm(v):return ' '.join(str(v or '').split()).casefold()
def dateiso(v):
    if isinstance(v,(date,datetime)):return v.strftime('%Y-%m-%d')
    v=str(v or '').strip()
    if not v:return ''
    for fmt in ('%Y-%m-%d','%d/%m/%Y','%m/%d/%Y'):
        try:return datetime.strptime(v,fmt).strftime('%Y-%m-%d')
        except ValueError:pass
    return v

def checks(v):return [s.strip() for s in str(v or '').split('|') if s.strip()]
def canonical(field,value):
    if field=='DueDateISO':return dateiso(value)
    if field=='Priority':return PRIORITY.get(norm(value),str(value or '').strip())
    if field=='Checklist (pipe-separated)':return checks(value) if isinstance(value,str) else list(value or [])
    return str(value or '').strip()
def canonical_row(row):return {field:canonical(field,row.get(field)) for field in FIELDS}
def equals(field,a,b):
    if field=='Checklist (pipe-separated)':return [norm(x) for x in a]==[norm(x) for x in b]
    return norm(a)==norm(b)

def find_card(page,bucket,title):
    row={'Bucket':bucket,'TaskTitle':title}
    card=detail.get_card(page,row)
    return card

def get_live(page,row):
    card=find_card(page,row['Bucket'],row['TaskTitle'])
    if card is None:return None,None
    card.scroll_into_view_if_needed(timeout=10000)
    card.click()
    title=page.locator('[data-testid="task-details-dialog"] input[data-testid="task-editor-title-input"]')
    title.first.wait_for(state='visible',timeout=12000)
    panel=detail.get_panel(page)
    if panel is None:raise RuntimeError('Task details dialog not opened')
    actual=title.first.input_value().strip()
    if norm(actual)!=norm(row['TaskTitle']):
        detail.close_panel(page,panel)
        raise RuntimeError(f'Task title mismatch: expected {row["TaskTitle"]!r}, found {actual!r}')
    def val(loc):return loc.first.input_value().strip() if loc.count()==1 else ''
    due=val(panel.locator('[data-testid="task-editor-due-date"] input[data-testid="date-picker"]'))
    pr=panel.locator('[data-testid="task-editor-priority"]').get_by_role('combobox',name='Priority')
    pri=pr.inner_text().strip() if pr.count()==1 else ''
    notes=panel.locator('[data-testid="task-notes"] [data-testid="rich-text-editor"] [role="textbox"]')
    desc=notes.inner_text().strip() if notes.count()==1 else ''
    items=panel.locator('input[data-testid="checklist-item-title-input"]')
    checklist=[items.nth(i).input_value().strip() for i in range(items.count())]
    live={'TaskTitle':actual,'Bucket':row['Bucket'],'Description':desc,'DueDateISO':dateiso(due),
          'Priority':pri,'Checklist (pipe-separated)':checklist}
    return live,panel

def snapshot_excel():
    rows=imp.get_rows(EXCEL)
    out={}
    for row in rows:
        key=str(row.get('TaskKey') or '').strip()
        if not key:raise ValueError('Every Excel task requires a stable TaskKey')
        if key in out:raise ValueError('Duplicate TaskKey '+key)
        out[key]=canonical_row(row)
    return out

def plan_actions(key,current,base,live):
    if base is None:
        return [('CREATE','TaskTitle','New Excel task; no previous baseline')]
    if live is None:
        return [('REVIEW','TaskTitle','Cannot uniquely locate this existing Planner task; do not create a duplicate')]
    changes=[]
    for field in FIELDS:
        oldx=base['excel'].get(field,'' if field!='Checklist (pipe-separated)' else [])
        oldp=base['planner'].get(field,'' if field!='Checklist (pipe-separated)' else [])
        x=current[field];p=live[field]
        xe=equals(field,oldx,x);pe=equals(field,oldp,p)
        if xe and pe:continue
        if xe and not pe:
            changes.append(('PRESERVE',field,'Changed directly in Planner; Excel unchanged'))
        elif not xe and pe:
            if equals(field,x,p):changes.append(('ALREADY',field,'Excel and Planner already match'))
            elif field=='Checklist (pipe-separated)':
                previous=[norm(v) for v in oldx]
                additions=[v for v in x if norm(v) not in previous]
                removals=[v for v in oldx if norm(v) not in [norm(z) for z in x]]
                if removals:changes.append(('REVIEW',field,'Checklist removal/reordering requires manual approval'))
                elif additions:changes.append(('UPDATE',field,'Add new checklist items only'))
                else:changes.append(('REVIEW',field,'Checklist modification requires manual review'))
            else:changes.append(('UPDATE',field,'Excel changed; Planner field unchanged'))
        elif equals(field,x,p):changes.append(('ALREADY',field,'Both sides now match'))
        else:changes.append(('CONFLICT',field,'Both Excel and Planner changed since last sync'))
    return changes or [('UNCHANGED','-','No differences')]

def apply_field(page,panel,field,value,live):
    if field=='Description':
        ed=panel.locator('[data-testid="task-notes"] [data-testid="rich-text-editor"] [role="textbox"][contenteditable="true"]')
        if ed.count()!=1:raise RuntimeError('Notes editor not uniquely found')
        ed.fill(value);ed.press('Tab');page.wait_for_timeout(650)
        return 'Notes entered'
    if field=='DueDateISO':
        picker=panel.locator('[data-testid="task-editor-due-date"] input[data-testid="date-picker"]')
        if picker.count()!=1:raise RuntimeError('Due date input not uniquely found')
        target=datetime.strptime(value,'%Y-%m-%d').strftime('%d/%m/%Y') if value else ''
        picker.fill(target);picker.press('Tab');page.wait_for_timeout(700)
        if dateiso(picker.input_value())!=value:raise RuntimeError('Date not confirmed')
        return 'Due date entered'
    if field=='Priority':
        sel=panel.locator('[data-testid="task-editor-priority"]').get_by_role('combobox',name='Priority')
        if sel.count()!=1:raise RuntimeError('Priority control not unique')
        if norm(sel.inner_text())==norm(value):return 'Priority already matches'
        sel.click()
        choice=page.get_by_role('option',name=value,exact=True)
        matches=[choice.nth(i) for i in range(choice.count()) if choice.nth(i).is_visible()]
        if len(matches)!=1:raise RuntimeError('Priority option not unique')
        matches[0].click();page.wait_for_timeout(600)
        if norm(sel.inner_text())!=norm(value):raise RuntimeError('Priority not confirmed')
        return 'Priority updated'
    if field=='Checklist (pipe-separated)':
        root=panel.locator('[data-testid="checklist-container"]')
        inp=panel.locator('input[data-testid="checklist-new-item-input"]')
        if root.count()!=1 or inp.count()!=1:raise RuntimeError('Checklist editor not unique')
        prior={norm(s) for s in live['Checklist (pipe-separated)']}
        added=[]
        for item in value:
            if norm(item) in prior:continue
            inp.fill(item);inp.press('Enter');page.wait_for_timeout(700)
            now=[norm(root.locator('input[data-testid="checklist-item-title-input"]').nth(i).input_value()) for i in range(root.locator('input[data-testid="checklist-item-title-input"]').count())]
            if norm(item) not in now:raise RuntimeError('Cannot confirm checklist item '+item)
            added.append(item)
        return f'Added {len(added)} checklist items'
    if field=='TaskTitle':
        inp=panel.locator('input[data-testid="task-editor-title-input"]')
        if inp.count()!=1:raise RuntimeError('Title field not unique')
        inp.fill(value);inp.press('Tab');page.wait_for_timeout(700)
        if norm(inp.input_value())!=norm(value):raise RuntimeError('Rename not confirmed')
        return 'Title updated'
    raise RuntimeError('Field needs manual update: '+field)

def report(rows):
    with REPORT.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.writer(f);w.writerow(['TaskKey','Action','Field','Explanation'])
        for row in rows:w.writerow(row)

def main():
    ap=argparse.ArgumentParser(description='Preview-first, conflict-aware Connect to Work Planner synchronization')
    mode=ap.add_mutually_exclusive_group();mode.add_argument('--baseline',action='store_true',help='Capture first comparison baseline, no edits');mode.add_argument('--apply',action='store_true',help='Apply safe changes shown by preview')
    ap.add_argument('--limit',type=int,default=0,help='Maximum task records; 0 means all')
    ap.add_argument('--email',default='showan@saheli.co.uk')
    ap.add_argument('--allow-create',action='store_true',help='Opt in to create new Excel TaskKeys not in the baseline (confirm Planner has no duplicate first)')
    args=ap.parse_args()
    excel=snapshot_excel()
    state=json.loads(STATE.read_text(encoding='utf-8')) if STATE.exists() else {'version':1,'tasks':{}}
    if not args.baseline and not state['tasks']:
        print('NO BASELINE: run --baseline FIRST. No Planner changes attempted.');return
    if args.baseline and state['tasks']:
        print('Baseline already exists; refusing to overwrite it. Keep the file for 3-way comparisons.');return
    print(f'Excel contains {len(excel)} tasks. Mode: '+('BASELINE' if args.baseline else 'APPLY' if args.apply else 'PREVIEW'))
    if args.apply:
        answer=input('This will edit Planner. Type APPLY CONNECT TO WORK to continue: ')
        if answer!='APPLY CONNECT TO WORK':print('Cancelled.');return
    records=[];done=0
    with sync_playwright() as pw:
        context=pw.chromium.launch_persistent_context(str(imp.BROWSER_PROFILE),headless=False,viewport={'width':1450,'height':940})
        try:
            if not context.pages:context.new_page()
            if not any('planner' in p.url.lower() for p in context.pages):context.pages[0].goto('https://planner.cloud.microsoft/',wait_until='domcontentloaded')
            imp.automate_microsoft_signin(context,args.email)
            page=imp.wait_for_planner_board(context);imp.wait_until_board_ready(page)
            if 'hoF-jE4uqkqoh39k06WAH5gAHu5t' not in page.url:raise RuntimeError('Wrong plan URL; stopped')
            for key,current in excel.items():
                if args.limit and done>=args.limit:break
                done+=1
                base=state['tasks'].get(key)
                lookup=(base['planner'] if base else current)
                # Fresh Excel TaskKey has no prior Planner identity. Do not infer a match by title.
                live,panel=(None,None) if base is None else get_live(page,lookup)
                if args.baseline:
                    live,panel=get_live(page,current)
                actions=plan_actions(key,current,base,live)
                if args.baseline:
                    if live is None:
                        records.append((key,'REVIEW','TaskTitle','Not found in Planner; baseline not established'))
                    else:
                        state['tasks'][key]={'excel':current,'planner':live,'last_synced_utc':datetime.utcnow().isoformat()+'Z'}
                        records.append((key,'BASELINED','-','Captured live Planner and Excel, made no changes'))
                else:
                    for action,field,why in actions:
                        status=action;msg=why
                        if action=='UPDATE' and args.apply:
                            try:
                                if field=='Bucket':raise RuntimeError('Bucket move requires review')
                                if field=='DueDateISO' and not current[field]:raise RuntimeError('Clearing due dates requires review')
                                result=apply_field(page,panel,field,current[field],live)
                                status='APPLIED';msg=result
                                # Update baseline for this field only after a successful operation.
                                state['tasks'][key]['excel'][field]=current[field]
                                state['tasks'][key]['planner'][field]=current[field]
                                if field=='TaskTitle':lookup['TaskTitle']=current[field]
                            except Exception as exc:status='REVIEW';msg=str(exc)
                        elif action=='CREATE' and args.apply:
                            if not args.allow_create:
                                status='REVIEW';msg='New TaskKey. Re-run with --allow-create after checking Planner for duplicates.'
                            else:
                                try:
                                    all_titles=page.locator('[data-testid="task-card-title"]')
                                    if any(norm(all_titles.nth(j).inner_text())==norm(current['TaskTitle']) for j in range(all_titles.count())):
                                        raise RuntimeError('Matching task title already found on board; manually reconcile before creation')
                                    row={'TaskKey':key,**current}
                                    result,note=imp.create_task(page,row)
                                    if result!='CREATED':raise RuntimeError(note)
                                    # Enrollment: capture actual state after creation, before changing details.
                                    newlive,newpanel=get_live(page,current)
                                    if newlive is None:raise RuntimeError('Created title, but could not reopen task. Verify Planner before retrying.')
                                    state['tasks'][key]={'excel':current,'planner':newlive,'last_synced_utc':datetime.utcnow().isoformat()+'Z'}
                                    status='CREATED';msg='New task added; baseline captured (run another preview to fill details)'
                                    detail.close_panel(page,newpanel)
                                except Exception as exc:status='REVIEW';msg=f'Creation not safely completed: {exc}. Check Planner for duplicates before retrying.'
                        elif action=='ALREADY' and args.apply:
                            state['tasks'][key]['excel'][field]=current[field]
                            state['tasks'][key]['planner'][field]=current[field]
                        elif action=='PRESERVE' and args.apply:
                            # Keep old Excel baseline, record the new Planner value as baseline
                            # only after operator explicitly syncs Excel; do not alter state here.
                            pass
                        records.append((key,status,field,msg))
                if panel is not None:detail.close_panel(page,panel)
            if args.baseline or args.apply:
                temp=STATE.with_suffix('.json.tmp');temp.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8');temp.replace(STATE)
        except Exception as ex:
            print('STOPPED:',ex,' — retaining all prior work');records.append(('-', 'STOPPED','-',str(ex)))
        finally:
            report(records)
            for key,status,field,msg in records:print(f'{key}: {status} {field} — {msg}')
            print('Report:',REPORT)
            input('Press ENTER to close the automated browser... ')
            context.close()

if __name__=='__main__':main()

import io, json
from datetime import datetime, timezone
from openpyxl import Workbook
from openpyxl.styles import Protection, Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import functions_framework
from flask import Request, make_response

PROTECT_PASSWORD = 'EduTestPro2025'
CORS = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
}

_HDR_FILL  = PatternFill('solid', fgColor='00B894')
_HDR_FONT  = Font(bold=True, color='FFFFFF', name='Arial', size=11)
_BODY      = Font(name='Arial', size=10)
_PASS      = Font(name='Arial', size=10, color='00B894', bold=True)
_FAIL      = Font(name='Arial', size=10, color='E74C3C', bold=True)
_CENTER    = Alignment(horizontal='center', vertical='center')
_LEFT      = Alignment(horizontal='left',   vertical='center')
_THIN      = Side(style='thin', color='D0D0D0')
_BORDER    = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)
_ALT       = PatternFill('solid', fgColor='F8F9FA')
_LOCK      = Protection(locked=True)


@functions_framework.http
def generate_results_xlsx(request: Request):
    if request.method == 'OPTIONS':
        return make_response('', 204, CORS)
    if request.method != 'POST':
        return ('Method not allowed', 405, CORS)

    body          = request.get_json(force=True, silent=True) or {}
    title         = str(body.get('title',          'Results'))[:120]
    school        = str(body.get('school',         ''))[:120]
    downloaded_by = str(body.get('downloaded_by',  ''))[:120]
    rows          = body.get('rows', [])

    data      = _build_xlsx(title, school, downloaded_by, rows)
    safe      = ''.join(c if c.isalnum() or c in ' _-' else '_' for c in title)
    filename  = f"{safe}.xlsx"

    resp = make_response(data)
    resp.headers['Content-Type']        = (
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    for k, v in CORS.items():
        resp.headers[k] = v
    return resp


def _build_xlsx(title, school, by, rows):
    wb = Workbook()
    ws = wb.active
    ws.title       = 'Results'
    ws.freeze_panes = 'A3'

    # Row 1 — title banner
    ws.merge_cells('A1:G1')
    c = ws['A1']
    c.value     = title
    c.font      = Font(bold=True, name='Arial', size=13, color='2D3436')
    c.alignment = _CENTER
    ws.row_dimensions[1].height = 28

    # Row 2 — column headers
    headers = ['#', 'Student Email', 'Score', 'Total', 'Percentage', 'Result', 'Submitted']
    for col, h in enumerate(headers, 1):
        c = ws.cell(row=2, column=col, value=h)
        c.font = _HDR_FONT;  c.fill = _HDR_FILL
        c.alignment = _CENTER;  c.border = _BORDER
    ws.row_dimensions[2].height = 22

    # Data rows
    for i, row in enumerate(rows):
        r    = i + 3
        pct  = int(row.get('percentage', 0))
        rslt = 'PASS' if pct >= 50 else 'FAIL'
        vals = [i+1, row.get('email',''), row.get('score',0),
                row.get('total',0), f"{pct}%", rslt, row.get('submitted','')]
        alns = [_CENTER,_LEFT,_CENTER,_CENTER,_CENTER,_CENTER,_CENTER]
        for col, (val, aln) in enumerate(zip(vals, alns), 1):
            c = ws.cell(row=r, column=col, value=val)
            c.alignment = aln;  c.border = _BORDER
            c.font = (_PASS if rslt=='PASS' else _FAIL) if col==6 else _BODY
            if i % 2 == 0 and col != 6:
                c.fill = _ALT
        ws.row_dimensions[r].height = 18

    # Column widths
    for i, w in enumerate([4,36,8,8,12,10,22], 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # Stats footer
    if rows:
        sr   = len(rows) + 4
        pcts = [int(r.get('percentage',0)) for r in rows]
        avg  = sum(pcts) // len(pcts)
        psd  = sum(1 for p in pcts if p >= 50)
        pr   = round(psd / len(pcts) * 100)
        for lbl, val, col in [('Total',len(rows),2),('Avg',f'{avg}%',4),
                               ('Pass Rate',f'{pr}%',5),('Passed',psd,6)]:
            ws.cell(row=sr,column=col-1,value=lbl).font = Font(bold=True,name='Arial',size=9,color='636e72')
            ws.cell(row=sr,column=col,  value=val ).font = Font(bold=True,name='Arial',size=9,color='2d3436')

    # Lock every cell then protect sheet
    for row_cells in ws.iter_rows():
        for cell in row_cells:
            cell.protection = _LOCK

    ws.protection.set_password(PROTECT_PASSWORD)
    ws.protection.sheet             = True
    ws.protection.insertRows        = True
    ws.protection.insertColumns     = True
    ws.protection.deleteRows        = True
    ws.protection.deleteColumns     = True
    ws.protection.formatCells       = True
    ws.protection.formatRows        = True
    ws.protection.formatColumns     = True
    ws.protection.sort              = True
    ws.protection.autoFilter        = True
    ws.protection.selectLockedCells = False   # allow read/copy
    ws.protection.selectUnlockedCells = False

    # Info sheet
    ws2 = wb.create_sheet('Info')
    for r,(k,v) in enumerate([
        ('EduTest Pro — Results Export',''),('',''),
        ('School', school), ('Report', title), ('Downloaded By', by),
        ('Date', datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')),
        ('Total Records', len(rows)),('',''),
        ('','This file is read-only and protected by EduTest Pro.'),
        ('','To request the unlock password contact your system administrator.'),
    ], 1):
        ws2.cell(row=r,column=1,value=k).font = Font(bold=True,name='Arial',size=10)
        ws2.cell(row=r,column=2,value=v).font = Font(name='Arial',size=10)
    ws2.column_dimensions['A'].width = 22
    ws2.column_dimensions['B'].width = 55
    for row_cells in ws2.iter_rows():
        for cell in row_cells:
            cell.protection = _LOCK
    ws2.protection.set_password(PROTECT_PASSWORD)
    ws2.protection.sheet = True
    ws2.protection.enable()

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
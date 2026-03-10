import io
import os
import json
import sys
import traceback
from datetime import datetime, timezone
from openpyxl import Workbook
from openpyxl.styles import Protection, Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from flask import Flask, request as flask_request, make_response
app = Flask(__name__)

# ── Firebase Admin SDK ────────────────────────────────────────────────────────
import firebase_admin
from firebase_admin import credentials, firestore as admin_firestore, auth as fb_auth

_fdb = None

def _get_db():
    """Initialise Firebase Admin once, return Firestore client."""
    global _fdb
    if _fdb:
        return _fdb
    if not firebase_admin._apps:
        sa_json = os.environ.get('SERVICE_ACCOUNT_JSON', '')
        if sa_json:
            try:
                sa_dict = json.loads(sa_json)
            except json.JSONDecodeError as je:
                sa_json_fixed = sa_json.replace('\r\n', '\n')
                sa_dict = json.loads(sa_json_fixed)
            # Repair private_key if actual newlines were injected
            if 'private_key' in sa_dict:
                pk = sa_dict['private_key']
                if '\\n' in pk:
                    sa_dict['private_key'] = pk.replace('\\n', '\n')
            try:
                cred = credentials.Certificate(sa_dict)
            except Exception as ce:
                traceback.print_exc()
                raise
        else:
            cred = credentials.Certificate('serviceAccountKey.json')
        try:
            firebase_admin.initialize_app(cred)
        except Exception as ie:
            traceback.print_exc()
            raise
    try:
        _fdb = admin_firestore.client()
        return _fdb
    except Exception as fe:
        traceback.print_exc()
        raise
PROTECT_PASSWORD = os.environ.get('XLSX_PASSWORD', 'EduTestPro2025')
CORS = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-EduTest-Key',
}

# ── Style constants ────────────────────────────────────────────────────────────
_GREEN      = '1B6B45'   # dark professional green
_GREEN_LIGHT= '2D9B6A'   # lighter green for sub-headers
_DARK       = '1A1A2E'
_GREY       = '6B7280'
_WHITE      = 'FFFFFF'
_PASS_CLR   = '059669'
_FAIL_CLR   = 'DC2626'
_ALT_BG     = 'F0FDF4'   # very light green tint for alternating rows
_HDR_BG     = 'E8F5E9'   # column header bg

_TITLE_FILL   = PatternFill('solid', fgColor=_GREEN)
_SUB_FILL     = PatternFill('solid', fgColor=_GREEN_LIGHT)
_COL_HDR_FILL = PatternFill('solid', fgColor='166534')
_ALT_FILL     = PatternFill('solid', fgColor=_ALT_BG)
_LOCK         = Protection(locked=True)

def _font(size=10, bold=False, color=_DARK, name='Calibri'):
    return Font(name=name, size=size, bold=bold, color=color)

_CENTER  = Alignment(horizontal='center', vertical='center', wrap_text=False)
_LEFT    = Alignment(horizontal='left',   vertical='center')
_THIN    = Side(style='thin',   color='D1FAE5')
_MED     = Side(style='medium', color='059669')
_BORDER  = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)

NUM_COLS = 9   # #, Name, Email, Class, Arm, Score, %, Result, Date



# =============================================================================
# EXAM ENDPOINTS  (server-side delivery + grading via Firebase Admin SDK)
# =============================================================================

def _verify_token(request):
    """Verify Firebase ID token. Returns (email, uid) or raises."""
    _get_db()  # Ensure Firebase Admin is initialized before using fb_auth
    hdr = request.headers.get('Authorization', '')
    if not hdr.startswith('Bearer '):
        raise PermissionError('Missing or invalid Authorization header')
    decoded = fb_auth.verify_id_token(hdr[7:])
    return decoded['email'].lower(), decoded['uid']


def _secret_ok(request):
    secret = os.environ.get('XLSX_SECRET', '')
    if secret and request.headers.get('X-EduTest-Key', '') != secret:
        raise PermissionError('Unauthorized')


def _json_resp(data, status=200):
    resp = make_response(json.dumps(data), status)
    resp.headers['Content-Type'] = 'application/json'
    for k, v in CORS.items():
        resp.headers[k] = v
    return resp


def _err(msg, status=400):
    return _json_resp({'ok': False, 'error': msg}, status)


@app.route('/get-exam', methods=['POST','OPTIONS'])
def get_exam():
    request = flask_request
    """
    POST /get-exam
    Body: { examId }
    Auth: Bearer <Firebase ID token>
    Returns exam stripped of correctIndex / correctLetter.
    """
    if request.method == 'OPTIONS':
        return make_response('', 204, CORS)
    if request.method != 'POST':
        return _err('Method not allowed', 405)
    try:
        _secret_ok(request)
        email, _ = _verify_token(request)
        body    = request.get_json(force=True, silent=True) or {}
        exam_id = (body.get('examId') or '').strip()
        if not exam_id:
            return _err('examId required')

        db = _get_db()

        # Verify student is active
        u = db.collection('users').document(email).get()
        if not u.exists:
            return _err('Account not found', 403)
        ud = u.to_dict()
        if ud.get('status') != 'active':
            return _err('Account suspended', 403)
        if ud.get('role') != 'student':
            return _err('Only students can take exams', 403)

        # Fetch full exam (Admin SDK bypasses client rules entirely)
        ex = db.collection('exams').document(exam_id).get()
        if not ex.exists:
            return _err('Exam not found', 404)
        ed = ex.to_dict()

        # School isolation check
        if ed.get('schoolId') and ed['schoolId'] != ud.get('schoolId'):
            return _err('Exam not available for your school', 403)

        # Strip correct answers before sending to browser
        safe_qs = [
            {'question': q.get('question', ''), 'options': q.get('options', [])}
            for q in ed.get('questions', [])
        ]

        return _json_resp({'ok': True, 'exam': {
            'id':               exam_id,
            'title':            ed.get('title', ''),
            'description':      ed.get('description', ''),
            'duration_minutes': ed.get('duration_minutes', 60),
            'schoolId':         ed.get('schoolId', ''),
            'questions':        safe_qs,
        }})

    except PermissionError as e:
        return _err(str(e), 403)
    except Exception as e:
        return _err(str(e), 500)


@app.route('/check-submitted', methods=['POST','OPTIONS'])
def check_submitted():
    request = flask_request
    """
    POST /check-submitted
    Body: { examId }
    Returns { ok, submitted }
    """
    if request.method == 'OPTIONS':
        return make_response('', 204, CORS)
    if request.method != 'POST':
        return _err('Method not allowed', 405)
    try:
        _secret_ok(request)
        email, _ = _verify_token(request)
        body    = request.get_json(force=True, silent=True) or {}
        exam_id = (body.get('examId') or '').strip()
        if not exam_id:
            return _err('examId required')
        snap = _get_db().collection('submissions').document(exam_id + '_' + email).get()
        return _json_resp({'ok': True, 'submitted': snap.exists})
    except PermissionError as e:
        return _err(str(e), 403)
    except Exception as e:
        return _err(str(e), 500)


@app.route('/submit-exam', methods=['POST','OPTIONS'])
def submit_exam():
    request = flask_request
    """
    POST /submit-exam
    Body: {
        examId, rawAnswers, questionOrder, optionOrders, timeTaken
    }
    rawAnswers: { "shuffledQIdx": shuffledOptIdx, ... }
    questionOrder: [origIdx, ...]          (from _questionOrder)
    optionOrders:  [[origOptIdx,...], ...] (from _optionOrders)
    timeTaken: seconds elapsed

    Server grades against correctIndex from Firestore — browser never sees it.
    Writes submission to Firestore via Admin SDK (bypasses client rules).
    Returns { ok, correct, wrong, unanswered, total, percentage }
    """
    if request.method == 'OPTIONS':
        return make_response('', 204, CORS)
    if request.method != 'POST':
        return _err('Method not allowed', 405)
    try:
        _secret_ok(request)
        email, _ = _verify_token(request)
        body          = request.get_json(force=True, silent=True) or {}
        exam_id       = (body.get('examId') or '').strip()
        raw_answers   = body.get('rawAnswers', {})
        question_order= body.get('questionOrder', [])
        option_orders = body.get('optionOrders', [])
        time_taken    = int(body.get('timeTaken', 0))

        if not exam_id:
            return _err('examId required')

        db = _get_db()

        # Verify student
        u  = db.collection('users').document(email).get()
        if not u.exists:
            return _err('Account not found', 403)
        ud = u.to_dict()
        if ud.get('status') != 'active' or ud.get('role') != 'student':
            return _err('Not authorised', 403)

        # Prevent double submission
        doc_id = exam_id + '_' + email
        if db.collection('submissions').document(doc_id).get().exists:
            return _err('You have already submitted this exam.', 409)

        # Fetch FULL exam with correct answers
        ex = db.collection('exams').document(exam_id).get()
        if not ex.exists:
            return _err('Exam not found', 404)
        ed = ex.to_dict()

        if ed.get('schoolId') and ed['schoolId'] != ud.get('schoolId'):
            return _err('Exam not available for your school', 403)

        orig_qs = ed.get('questions', [])
        total   = len(orig_qs)

        # Fallback if shuffle orders not sent
        if not question_order:
            question_order = list(range(total))
        if not option_orders:
            option_orders = [list(range(len(q.get('options', [])))) for q in orig_qs]

        # ── Grade server-side ────────────────────────────────────────────────
        correct  = 0
        answered = 0
        audit    = {}

        for si, orig_qi in enumerate(question_order):
            if orig_qi >= len(orig_qs):
                continue
            oq          = orig_qs[orig_qi]
            opt_order   = option_orders[si] if si < len(option_orders) else list(range(len(oq.get('options', []))))
            correct_idx = oq.get('correctIndex', -1)
            options     = oq.get('options', [])

            picked_s = raw_answers.get(str(si))
            if picked_s is not None:
                picked_s    = int(picked_s)
                answered   += 1
                picked_orig = opt_order[picked_s] if picked_s < len(opt_order) else None
            else:
                picked_orig = None

            is_correct = (picked_orig is not None and picked_orig == correct_idx)
            if is_correct:
                correct += 1

            audit[str(orig_qi)] = {
                'questionText': oq.get('question', ''),
                'pickedText':   options[picked_orig] if picked_orig is not None and picked_orig < len(options) else '(not answered)',
                'correctText':  options[correct_idx] if 0 <= correct_idx < len(options) else '',
                'isCorrect':    is_correct,
                'notAnswered':  picked_orig is None,
            }

        wrong      = answered - correct
        unanswered = total - answered
        percentage = round(correct / total * 100) if total else 0

        # ── Write to Firestore via Admin SDK ─────────────────────────────────
        db.collection('submissions').document(doc_id).set({
            'examId':       exam_id,
            'examTitle':    ed.get('title', ''),
            'schoolId':     ed.get('schoolId', ''),
            'studentEmail': email,
            'studentName':  (ud.get('name') or '').strip(),
            'studentClass': (ud.get('classGrade') or '').strip(),
            'answers':      audit,
            'score':        correct,
            'total':        total,
            'wrong':        wrong,
            'unanswered':   unanswered,
            'percentage':   percentage,
            'timeTaken':    time_taken,
            'submittedAt':  admin_firestore.SERVER_TIMESTAMP,
        })

        return _json_resp({
            'ok': True, 'correct': correct, 'wrong': wrong,
            'unanswered': unanswered, 'total': total, 'percentage': percentage,
        })

    except PermissionError as e:
        return _err(str(e), 403)
    except Exception as e:
        import traceback; traceback.print_exc()
        return _err(str(e), 500)


@app.route('/list-exams', methods=['POST','OPTIONS'])
def list_exams():
    request = flask_request
    """
    POST /list-exams
    Body: {} (no body needed — student identity from token)
    Auth: Bearer <Firebase ID token>
    Returns all active exams for the student's school, minus ones
    they have already submitted.  No questions or correct answers included.
    """
    if request.method == 'OPTIONS':
        return make_response('', 204, CORS)
    if request.method != 'POST':
        return _err('Method not allowed', 405)
    try:
        _secret_ok(request)
        email, _ = _verify_token(request)

        db = _get_db()

        # Verify student
        u = db.collection('users').document(email).get()
        if not u.exists:
            return _err('Account not found', 403)
        ud = u.to_dict()
        if ud.get('status') != 'active':
            return _err('Account suspended', 403)
        if ud.get('role') != 'student':
            return _err('Only students can list exams', 403)

        school_id = ud.get('schoolId', '')
        if not school_id:
            return _err('No school assigned to this account', 403)

        # Fetch all exams for this school (Admin SDK — bypasses client rules)
        exams_snap = db.collection('exams').where('schoolId', '==', school_id).get()

        # Fetch this student's submissions to exclude already-done exams
        subs_snap = db.collection('submissions')             .where('studentEmail', '==', email.lower())             .where('schoolId',     '==', school_id)             .get()
        submitted_ids = {s.to_dict().get('examId') for s in subs_snap}

        exams_out = []
        for ex in exams_snap:
            ed = ex.to_dict()
            if ex.id in submitted_ids:
                continue
            exams_out.append({
                'id':               ex.id,
                'title':            ed.get('title', ''),
                'description':      ed.get('description', ''),
                'duration_minutes': ed.get('duration_minutes', 60),
                'targetClass':      ed.get('targetClass', ''),
                'examTerm':         ed.get('examTerm', ''),
                'examType':         ed.get('examType', ''),
                'questionCount':    len(ed.get('questions', [])),
            })

        # Sort by creation time if available (newest first)
        exams_out.sort(key=lambda e: e['title'])

        return _json_resp({'ok': True, 'exams': exams_out})

    except PermissionError as e:
        traceback.print_exc()
        return _err(str(e), 403)
    except Exception as e:
        traceback.print_exc()
        return _err(str(e), 500)


@app.route('/generate_results_xlsx', methods=['POST','OPTIONS'])
def generate_results_xlsx():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, CORS)
    if request.method != 'POST':
        return ('Method not allowed', 405, CORS)

    # ── Auth: secret key + Firebase token + staff role ─────────────────────
    api_secret = os.environ.get('XLSX_SECRET', '')
    if api_secret:
        provided = request.headers.get('X-EduTest-Key', '')
        if provided != api_secret:
            return ('Unauthorized', 401, CORS)
    try:
        caller_email, _ = _verify_token(request)
    except PermissionError as e:
        return _err(str(e), 401)

    # Verify caller is staff (not a student)
    db = _get_db()
    caller_doc = db.collection('users').document(caller_email).get()
    if not caller_doc.exists:
        return _err('Account not found', 403)
    caller_data = caller_doc.to_dict()
    if caller_data.get('role') not in ('super_admin', 'school_admin', 'sub_admin', 'teacher'):
        return _err('Not authorised', 403)

    body             = request.get_json(force=True, silent=True) or {}
    title            = str(body.get('title',            'Exam Results'))[:120]
    school           = str(body.get('school',           ''))[:120]
    academic_session = str(body.get('academic_session', ''))[:80]
    term             = str(body.get('term',             ''))[:60]
    exam_type        = str(body.get('exam_type',        ''))[:60]
    downloaded_by    = str(body.get('downloaded_by',    ''))[:120]
    rows             = body.get('rows', [])

    data     = _build_xlsx(title, school, academic_session, term, exam_type, downloaded_by, rows)
    safe     = ''.join(c if c.isalnum() or c in ' _-' else '_' for c in title)
    filename = f"{safe}.xlsx"

    resp = make_response(data)
    resp.headers['Content-Type']        = (
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    for k, v in CORS.items():
        resp.headers[k] = v
    return resp


def _lock_all(ws):
    for row_cells in ws.iter_rows():
        for cell in row_cells:
            cell.protection = _LOCK
    ws.protection.set_password(PROTECT_PASSWORD)
    ws.protection.sheet               = True
    ws.protection.insertRows          = True
    ws.protection.insertColumns       = True
    ws.protection.deleteRows          = True
    ws.protection.deleteColumns       = True
    ws.protection.formatCells         = True
    ws.protection.formatRows          = True
    ws.protection.formatColumns       = True
    ws.protection.sort                = True
    ws.protection.autoFilter          = True
    ws.protection.selectLockedCells   = False
    ws.protection.selectUnlockedCells = False


def _merged(ws, row, col_start, col_end, value, fill, font, align=None):
    if col_start < col_end:
        ws.merge_cells(
            start_row=row, start_column=col_start,
            end_row=row,   end_column=col_end)
    c = ws.cell(row=row, column=col_start, value=value)
    c.fill      = fill
    c.font      = font
    c.alignment = align or _CENTER
    return c


def _build_xlsx(title, school, academic_session, term, exam_type, downloaded_by, rows):
    wb = Workbook()
    ws = wb.active
    ws.title        = 'Results'
    ws.freeze_panes = 'A6'   # freeze above the column-header row

    # ── Header block ──────────────────────────────────────────────────────────
    # Row 1 — School name (full width, large)
    _merged(ws, 1, 1, NUM_COLS,
            school.upper() if school else 'SCHOOL RESULTS',
            _TITLE_FILL,
            _font(14, True, _WHITE, 'Calibri'))
    ws.row_dimensions[1].height = 32

    # Row 2 — Academic Session  |  Term  |  Exam Type  (split into 3 sections)
    # Section 1: Academic Session (cols 1-3)
    _merged(ws, 2, 1, 3,
            f'Academic Session: {academic_session}' if academic_session else 'Academic Session: —',
            _SUB_FILL, _font(10, True, _WHITE))
    # Section 2: Term (cols 4-6)
    _merged(ws, 2, 4, 6,
            f'Term: {term}' if term else 'Term: —',
            _SUB_FILL, _font(10, True, _WHITE))
    # Section 3: Exam Type (cols 7-9)
    _merged(ws, 2, 7, NUM_COLS,
            f'Type: {exam_type}' if exam_type else 'Type: —',
            _SUB_FILL, _font(10, True, _WHITE))
    ws.row_dimensions[2].height = 22

    # Row 3 — Report / exam title (full width)
    _merged(ws, 3, 1, NUM_COLS,
            title,
            PatternFill('solid', fgColor='F0FDF4'),
            _font(12, True, _GREEN))
    ws.row_dimensions[3].height = 24

    # Row 4 — Stats bar (filled after we know totals)
    ws.row_dimensions[4].height = 18

    # Row 5 — Column headers
    headers = ['#', 'Full Name', 'Email Address', 'Class', 'Arm',
               'Score', 'Percentage', 'Result', 'Date Submitted']
    for col, h in enumerate(headers, 1):
        c = ws.cell(row=5, column=col, value=h)
        c.fill      = _COL_HDR_FILL
        c.font      = _font(10, True, _WHITE)
        c.alignment = _CENTER
        c.border    = Border(
            left=Side(style='medium', color=_WHITE),
            right=Side(style='medium', color=_WHITE),
            bottom=Side(style='medium', color=_WHITE))
    ws.row_dimensions[5].height = 22

    # ── Data rows ─────────────────────────────────────────────────────────────
    pass_count = 0
    pct_total  = 0

    for i, row in enumerate(rows):
        r       = i + 6
        pct     = int(row.get('percentage', 0))
        rslt    = 'PASS' if pct >= 50 else 'FAIL'
        alt     = (i % 2 == 0)
        bg_fill = _ALT_FILL if alt else PatternFill()
        if rslt == 'PASS': pass_count += 1
        pct_total += pct

        values = [
            i + 1,
            row.get('name',  '—'),
            row.get('email', ''),
            row.get('class', '—'),
            row.get('arm',   '—'),
            f"{row.get('score',0)}/{row.get('total',0)}",
            f"{pct}%",
            rslt,
            row.get('submitted', '—'),
        ]
        aligns = [_CENTER, _LEFT, _LEFT, _CENTER, _CENTER,
                  _CENTER, _CENTER, _CENTER, _CENTER]
        colors = [_GREY, _DARK, _GREY, _DARK, _DARK,
                  _DARK, _DARK,
                  _PASS_CLR if rslt == 'PASS' else _FAIL_CLR,
                  _GREY]
        bolds  = [False, True, False, False, False,
                  False, True, True, False]

        for col, (val, aln, clr, bld) in enumerate(zip(values, aligns, colors, bolds), 1):
            c           = ws.cell(row=r, column=col, value=val)
            c.alignment = aln
            c.border    = _BORDER
            c.font      = _font(9 if col in (3,9) else 10, bld, clr)
            if alt:
                c.fill = _ALT_FILL
        ws.row_dimensions[r].height = 18

    # ── Row 4: Stats bar ──────────────────────────────────────────────────────
    total      = len(rows)
    avg        = round(pct_total / total) if total else 0
    fail_count = total - pass_count
    pass_rate  = round(pass_count / total * 100) if total else 0

    stats = [
        ('Total', str(total)),
        ('Passed', str(pass_count)),
        ('Failed', str(fail_count)),
        ('Average', f'{avg}%'),
        ('Pass Rate', f'{pass_rate}%'),
    ]
    stat_fill  = PatternFill('solid', fgColor='DCFCE7')
    stat_cols  = [1, 2, 4, 6, 8]   # label columns
    val_cols   = [2, 3, 5, 7, 9]   # won't be used; labels are merged

    # Write stats as label: value pairs across the row
    for idx, (lbl, val) in enumerate(stats):
        lc = idx * 2 + 1
        if lc + 1 > NUM_COLS:
            break
        l_cell = ws.cell(row=4, column=lc, value=f'{lbl}: {val}')
        l_cell.font      = _font(9, True, _GREEN)
        l_cell.alignment = _CENTER
        l_cell.fill      = stat_fill

    # ── Column widths ─────────────────────────────────────────────────────────
    widths = [5, 28, 32, 10, 6, 10, 13, 10, 22]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # ── Lock sheet ────────────────────────────────────────────────────────────
    _lock_all(ws)

    # ── Info sheet ────────────────────────────────────────────────────────────
    ws2       = wb.create_sheet('Info')
    now_str   = datetime.now(timezone.utc).strftime('%d %b %Y  %H:%M UTC')
    info_rows = [
        ('EduTest Pro — Results Export',    ''),
        ('',                                ''),
        ('School',                          school),
        ('Academic Session',                academic_session or '—'),
        ('Term',                            term             or '—'),
        ('Exam Type',                       exam_type        or '—'),
        ('Report Title',                    title),
        ('Downloaded By',                   downloaded_by),
        ('Download Date',                   now_str),
        ('Total Records',                   total),
        ('',                                ''),
        ('Protection Note',
         'This file is read-only and protected by EduTest Pro.'),
        ('Unlock Password',
         'Contact your system administrator for the password.'),
    ]
    for r, (k, v) in enumerate(info_rows, 1):
        ck       = ws2.cell(row=r, column=1, value=k)
        cv       = ws2.cell(row=r, column=2, value=v)
        ck.font  = _font(10, True,  _GREY)
        cv.font  = _font(10, False, _DARK)
        if r == 1:
            cv.value = ''
            ws2.merge_cells('A1:B1')
            ck.font = _font(13, True, _GREEN)
    ws2.column_dimensions['A'].width = 22
    ws2.column_dimensions['B'].width = 58
    _lock_all(ws2)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()



# =============================================================================
# ANSWER AUDIT ENDPOINT
# =============================================================================
# Per-student layout:
#   Global header (school / session / term / type / title / warning)
#   For each student:
#     ├─ Name, email, class, exam, score banner
#     └─ Table: Q# | Question Text | Option Picked | Correct Answer | Correct?
# =============================================================================

AUDIT_NCOLS = 5

@app.route('/generate_audit_xlsx', methods=['POST','OPTIONS'])
def generate_audit_xlsx():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, CORS)
    if request.method != 'POST':
        return ('Method not allowed', 405, CORS)

    # ── Auth: secret key + Firebase token + staff role ─────────────────────
    api_secret = os.environ.get('XLSX_SECRET', '')
    if api_secret:
        provided = request.headers.get('X-EduTest-Key', '')
        if provided != api_secret:
            return ('Unauthorized', 401, CORS)
    try:
        caller_email, _ = _verify_token(request)
    except PermissionError as e:
        return _err(str(e), 401)

    # Verify caller is staff (not a student)
    db = _get_db()
    caller_doc = db.collection('users').document(caller_email).get()
    if not caller_doc.exists:
        return _err('Account not found', 403)
    caller_data = caller_doc.to_dict()
    if caller_data.get('role') not in ('super_admin', 'school_admin', 'sub_admin', 'teacher'):
        return _err('Not authorised', 403)

    body             = request.get_json(force=True, silent=True) or {}
    title            = str(body.get('title',            'Answer Audit'))[:120]
    school           = str(body.get('school',           ''))[:120]
    academic_session = str(body.get('academic_session', ''))[:80]
    term             = str(body.get('term',             ''))[:60]
    exam_type        = str(body.get('exam_type',        ''))[:60]
    requested_by     = str(body.get('requested_by',     ''))[:120]
    scope            = str(body.get('scope',            ''))[:120]
    rows             = body.get('rows', [])

    data     = _build_audit_xlsx(title, school, academic_session, term,
                                  exam_type, requested_by, scope, rows)
    safe     = ''.join(ch if ch.isalnum() or ch in ' _-' else '_' for ch in title)
    filename = f"{safe}.xlsx"

    resp = make_response(data)
    resp.headers['Content-Type']        = (
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    resp.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    for k, v in CORS.items():
        resp.headers[k] = v
    return resp


def _build_audit_xlsx(title, school, academic_session, term, exam_type,
                       requested_by, scope, rows):
    # Colour scheme — amber to distinguish from normal results
    AMBER        = 'B45309'
    AMBER_LIGHT  = 'D97706'
    AMBER_PALE   = 'FEF3C7'
    AMBER_BG     = 'FFFBEB'
    GREEN_OK     = '059669'
    RED_NO       = 'DC2626'
    GREY_NA      = '9CA3AF'
    STUDENT_BG   = '166534'   # dark green for student banner

    FILL_AMBER       = PatternFill('solid', fgColor=AMBER)
    FILL_AMBER_LIGHT = PatternFill('solid', fgColor=AMBER_LIGHT)
    FILL_AMBER_PALE  = PatternFill('solid', fgColor=AMBER_PALE)
    FILL_AMBER_BG    = PatternFill('solid', fgColor=AMBER_BG)
    FILL_WARN        = PatternFill('solid', fgColor='FEE2E2')
    FILL_STUDENT     = PatternFill('solid', fgColor=STUDENT_BG)
    FILL_META        = PatternFill('solid', fgColor='F0FDF4')
    FILL_COL_HDR     = PatternFill('solid', fgColor='92400E')

    NC = AUDIT_NCOLS

    wb = Workbook()
    ws = wb.active
    ws.title = 'Answer Audit'
    row = 1

    def cell_m(r, c1, c2, val, fill, fnt, aln=None):
        if c1 < c2:
            ws.merge_cells(start_row=r, start_column=c1, end_row=r, end_column=c2)
        cel = ws.cell(row=r, column=c1, value=val)
        cel.fill = fill; cel.font = fnt; cel.alignment = aln or _CENTER
        return cel

    # ── Global header ─────────────────────────────────────────────────────────
    cell_m(row, 1, NC, school.upper() if school else 'ANSWER AUDIT',
           FILL_AMBER, _font(13, True, _WHITE))
    ws.row_dimensions[row].height = 30;  row += 1

    # Session | Term | Type
    cell_m(row, 1, 2,
           f'Session: {academic_session}' if academic_session else 'Session: —',
           FILL_AMBER_LIGHT, _font(9, True, _WHITE))
    ws.cell(row=row, column=3, value=f'Term: {term}' if term else 'Term: —'
            ).fill = FILL_AMBER_LIGHT
    ws.cell(row=row, column=3).font = _font(9, True, _WHITE)
    ws.cell(row=row, column=3).alignment = _CENTER
    cell_m(row, 4, NC, f'Type: {exam_type}' if exam_type else 'Type: —',
           FILL_AMBER_LIGHT, _font(9, True, _WHITE))
    ws.row_dimensions[row].height = 18;  row += 1

    # Report title
    cell_m(row, 1, NC, title, FILL_AMBER_PALE, _font(11, True, AMBER))
    ws.row_dimensions[row].height = 22;  row += 1

    # Confidential warning
    cell_m(row, 1, NC,
           '⚠  CONFIDENTIAL — Authorised Investigation Use Only',
           FILL_WARN, _font(9, True, RED_NO))
    ws.row_dimensions[row].height = 16;  row += 1

    # Scope / requested by
    cell_m(row, 1, 2, f'Scope: {scope}' if scope else '',
           FILL_AMBER_PALE, _font(9, False, AMBER))
    cell_m(row, 3, NC, f'Requested by: {requested_by}',
           FILL_AMBER_PALE, _font(9, False, AMBER))
    ws.row_dimensions[row].height = 14;  row += 1

    row += 1  # spacer

    # ── Per-student sections ──────────────────────────────────────────────────
    for stu in rows:
        q_rows    = stu.get('q_rows', [])
        pct       = int(stu.get('percentage', 0))
        score     = stu.get('score', 0)
        total_qs  = stu.get('total_qs', 0)
        answered  = stu.get('answered', 0)
        unanswered= stu.get('unanswered', 0)

        # Student banner
        cell_m(row, 1, NC,
               f'{stu.get("name","—")}   ·   {stu.get("email","")}',
               FILL_STUDENT, _font(10, True, _WHITE))
        ws.row_dimensions[row].height = 20;  row += 1

        # Student meta
        meta = [
            f'Class: {stu.get("class","—")}',
            f'Exam: {stu.get("exam","—")}',
            f'Score: {score}/{total_qs}  ({pct}%)',
            f'Answered: {answered}  ·  Unanswered: {unanswered}',
            f'Submitted: {stu.get("submitted","—")}',
        ]
        for col, val in enumerate(meta, 1):
            cel = ws.cell(row=row, column=col, value=val)
            cel.fill = FILL_META; cel.font = _font(9, False, _DARK); cel.alignment = _LEFT
        ws.row_dimensions[row].height = 16;  row += 1

        # Column headers
        for col, hdr in enumerate(['Q#', 'Question Text', 'Option Picked', 'Correct Answer', 'Correct?'], 1):
            cel = ws.cell(row=row, column=col, value=hdr)
            cel.fill = FILL_COL_HDR; cel.font = _font(9, True, _WHITE); cel.alignment = _CENTER
        ws.row_dimensions[row].height = 18;  row += 1

        # Question rows
        if not q_rows:
            cell_m(row, 1, NC, '(no answer data recorded)', PatternFill(), _font(9, False, GREY_NA))
            ws.row_dimensions[row].height = 16;  row += 1
        else:
            for qi, qr in enumerate(q_rows):
                status   = qr.get('is_correct', 'NO')
                is_yes   = status == 'YES'
                is_na    = status == 'NOT ANSWERED'
                alt_fill = FILL_AMBER_BG if qi % 2 == 0 else PatternFill()
                picked_color  = GREEN_OK if is_yes else (GREY_NA if is_na else RED_NO)
                status_color  = GREEN_OK if is_yes else (GREY_NA if is_na else RED_NO)

                vals   = [qr.get('q_num', qi+1), qr.get('question','—'),
                          qr.get('picked','(not answered)'), qr.get('correct','—'), status]
                aligns = [_CENTER, _LEFT, _LEFT, _LEFT, _CENTER]
                colors = [_GREY, _DARK, picked_color, GREEN_OK, status_color]
                bolds  = [False,  False, True,         True,     True]

                for col, (v, aln, clr, bld) in enumerate(zip(vals, aligns, colors, bolds), 1):
                    cel = ws.cell(row=row, column=col, value=v)
                    cel.border = _BORDER; cel.font = _font(9, bld, clr); cel.alignment = aln
                    if qi % 2 == 0: cel.fill = alt_fill

                # Wrap text on Question, Picked, Correct columns
                for col in [2, 3, 4]:
                    ws.cell(row=row, column=col).alignment = Alignment(
                        horizontal='left', vertical='top', wrap_text=True)
                ws.row_dimensions[row].height = 32;  row += 1

        # Spacer between students
        ws.row_dimensions[row].height = 6;  row += 1

    # ── Column widths ─────────────────────────────────────────────────────────
    for col, width in enumerate([4, 52, 32, 32, 12], 1):
        ws.column_dimensions[get_column_letter(col)].width = width

    _lock_all(ws)

    # ── Audit Trail sheet ─────────────────────────────────────────────────────
    ws2 = wb.create_sheet('Audit Trail')
    now = datetime.now(timezone.utc).strftime('%d %b %Y  %H:%M UTC')
    trail = [
        ('EduTest Pro — Answer Audit', ''),
        ('', ''),
        ('School',           school),
        ('Academic Session', academic_session or '—'),
        ('Term',             term             or '—'),
        ('Exam Type',        exam_type        or '—'),
        ('Report Title',     title),
        ('Scope',            scope),
        ('Requested By',     requested_by),
        ('Generated On',     now),
        ('Total Students',   len(rows)),
        ('', ''),
        ('Contents',
         'Question text · Option the student selected · Correct answer · Whether correct.'),
        ('Does NOT contain',
         'All answer options are NOT listed — only what was picked and what was correct.'),
        ('Intended use',     'Authorised investigation only. Protected by EduTest Pro.'),
    ]
    for r, (k, v) in enumerate(trail, 1):
        ck = ws2.cell(row=r, column=1, value=k)
        cv = ws2.cell(row=r, column=2, value=v)
        ck.font = _font(10, True, _GREY); cv.font = _font(10, False, _DARK)
        if r == 1:
            ws2.merge_cells('A1:B1'); cv.value = ''
            ck.font = _font(13, True, AMBER)
    ws2.column_dimensions['A'].width = 20
    ws2.column_dimensions['B'].width = 80
    _lock_all(ws2)

    buf = io.BytesIO(); wb.save(buf); return buf.getvalue()


# ── Eager Firebase init at startup ────────────────────────────────────────────
# Initialize Firebase Admin when gunicorn loads the module, not lazily per-request.
# This catches missing SERVICE_ACCOUNT_JSON immediately on startup.
try:
    _get_db()
    print('[STARTUP] Firebase Admin initialized successfully')
except Exception as _e:
    print('[STARTUP] WARNING: Firebase Admin init failed:', _e)


# ── Health check ──────────────────────────────────────────────────────────────
@app.route('/health', methods=['GET'])
def health():
    return make_response('{"ok":true}', 200, {'Content-Type':'application/json'})


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
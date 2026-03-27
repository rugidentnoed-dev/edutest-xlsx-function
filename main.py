import io
import os
import json
import sys
import traceback
import requests
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
# Only accept requests from the Firebase hosting domain.
# OPTIONS preflight still works (browser sends Origin header automatically).
ALLOWED_ORIGINS = {
    'https://edutest-pro-cbt.web.app',
    'https://edutest-pro-cbt.firebaseapp.com',
}

def _cors_headers(req=None):
    origin = (req.headers.get('Origin', '') if req else '') or ''
    allowed = origin if origin in ALLOWED_ORIGINS else 'https://edutest-pro-cbt.web.app'
    return {
        'Access-Control-Allow-Origin':  allowed,
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-EduTest-Key',
        'Vary': 'Origin',
    }

CORS = _cors_headers()

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


def _json_resp(data, status=200, req=None):
    resp = make_response(json.dumps(data), status)
    resp.headers['Content-Type'] = 'application/json'
    for k, v in _cors_headers(req).items():
        resp.headers[k] = v
    return resp


def _rds_json_resp(data, status=200, req=None):
    """Like _json_resp but uses _make_rds_cors to allow RDS frontend origin."""
    resp = make_response(json.dumps(data), status)
    resp.headers['Content-Type'] = 'application/json'
    for k, v in _make_rds_cors(req).items():
        resp.headers[k] = v
    return resp


def _err(msg, status=400, req=None):
    return _json_resp({'ok': False, 'error': msg}, status, req)


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
        return make_response('', 204, _cors_headers(request))
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
        return make_response('', 204, _cors_headers(request))
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
        return make_response('', 204, _cors_headers(request))
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

        # ── S4: Payload size guards ───────────────────────────────────────────
        if not isinstance(raw_answers, dict) or len(raw_answers) > 500:
            return _err('Invalid or oversized answer payload', 400)
        if not isinstance(question_order, list) or len(question_order) > 500:
            return _err('Invalid questionOrder', 400)
        if not isinstance(option_orders, list) or len(option_orders) > 500:
            return _err('Invalid optionOrders', 400)

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

        # Block expired exams
        close_date_str = ed.get('closeDate')
        if close_date_str:
            try:
                close_dt = datetime.fromisoformat(close_date_str.replace('Z', '+00:00'))
                if datetime.now(timezone.utc) > close_dt:
                    return _err('This exam has closed and is no longer available.', 403)
            except Exception:
                pass

        orig_qs = ed.get('questions', [])
        total   = len(orig_qs)

        # ── S3: Server-side timer validation ─────────────────────────────────
        # Allow a 60-second grace period for network latency on top of duration.
        # time_taken is in seconds; duration_minutes is in minutes.
        duration_minutes = ed.get('duration_minutes', 0)
        if duration_minutes and time_taken > 0:
            max_allowed_secs = duration_minutes * 60 + 60  # +60s grace
            if time_taken > max_allowed_secs:
                # Cap it — don't reject, just record the real maximum.
                # Rejecting could punish students with slow connections.
                time_taken = max_allowed_secs

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
        import traceback
        traceback.print_exc()
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
        return make_response('', 204, _cors_headers(request))
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

        now       = datetime.now(timezone.utc)
        exams_out = []
        for ex in exams_snap:
            ed = ex.to_dict()
            if ex.id in submitted_ids:
                continue
            # Skip expired exams — closeDate is an ISO string stored by the client
            close_date_str = ed.get('closeDate')
            if close_date_str:
                try:
                    close_dt = datetime.fromisoformat(close_date_str.replace('Z', '+00:00'))
                    if now > close_dt:
                        continue  # exam has expired — hide from student
                except Exception:
                    pass  # malformed date — show the exam anyway
            exams_out.append({
                'id':               ex.id,
                'title':            ed.get('title', ''),
                'description':      ed.get('description', ''),
                'duration_minutes': ed.get('duration_minutes', 60),
                'targetClass':      ed.get('targetClass', ''),
                'examTerm':         ed.get('examTerm', ''),
                'examType':         ed.get('examType', ''),
                'questionCount':    len(ed.get('questions', [])),
                'closeDate':        close_date_str or None,
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
        return make_response('', 204, _cors_headers(request))
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
        return make_response('', 204, _cors_headers(request))
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


# ── Paystack payment verification ────────────────────────────────────────────

@app.route('/init-payment', methods=['POST','OPTIONS'])
def init_payment():
    """
    Initialize a Paystack transaction and return the authorization_url.
    Called by the client (school_admin) to start a redirect payment flow.
    The secret key stays on the server — never exposed to the browser.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _cors_headers(request))
    if request.method != 'POST':
        return _err('Method not allowed', 405, request)
    try:
        _secret_ok(request)
        caller_email, _ = _verify_token(request)

        # Verify caller is school_admin
        db = _get_db()
        caller_doc = db.collection('users').document(caller_email).get()
        if not caller_doc.exists:
            return _err('Account not found', 403, request)
        caller_data = caller_doc.to_dict()
        if caller_data.get('role') not in ('school_admin', 'super_admin'):
            return _err('Not authorised', 403, request)

        body      = request.get_json(force=True, silent=True) or {}
        school_id = (body.get('schoolId') or '').strip()
        amount    = int(body.get('amount', 0))        # in NGN (not kobo)
        email     = (body.get('email')    or '').strip()
        reference = (body.get('reference') or '').strip()
        callback  = (body.get('callbackUrl') or 'https://edutest-pro-cbt.web.app/app.html').strip()

        if not school_id or not amount or not email:
            return _err('schoolId, amount, and email are required', 400, request)

        # Verify caller belongs to this school (unless super_admin)
        if caller_data.get('role') == 'school_admin':
            if caller_data.get('schoolId') != school_id:
                return _err('Not authorised for this school', 403, request)

        paystack_secret = os.environ.get('PAYSTACK_SECRET_KEY', '')
        if not paystack_secret:
            return _err('Payment gateway not configured on server', 500, request)

        # Call Paystack Initialize API with the secret key (server-side only)
        resp = requests.post(
            'https://api.paystack.co/transaction/initialize',
            headers={
                'Authorization': f'Bearer {paystack_secret}',
                'Content-Type':  'application/json',
            },
            json={
                'email':        email,
                'amount':       amount * 100,   # convert NGN to kobo
                'currency':     'NGN',
                'reference':    reference,
                'callback_url': callback,
                'metadata': {
                    'custom_fields': [
                        {'display_name': 'School ID',  'variable_name': 'school_id',  'value': school_id},
                        {'display_name': 'Renewed By', 'variable_name': 'renewed_by', 'value': email},
                    ],
                },
            },
            timeout=15,
        )
        data = resp.json()

        if not resp.ok or not data.get('status') or not data.get('data', {}).get('authorization_url'):
            msg = data.get('message', 'Paystack initialization failed')
            return _err(msg, 400, request)

        return _json_resp({
            'ok':              True,
            'authorization_url': data['data']['authorization_url'],
            'reference':         data['data']['reference'],
            'access_code':       data['data'].get('access_code', ''),
        }, req=request)

    except PermissionError as e:
        return _err(str(e), 403, request)
    except Exception as e:
        traceback.print_exc()
        return _err(str(e), 500, request)


@app.route('/verify-payment', methods=['POST','OPTIONS'])
def verify_payment():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _cors_headers(request))
    if request.method != 'POST':
        return _err('Method not allowed', 405, request)
    try:
        _secret_ok(request)
        caller_email, _ = _verify_token(request)

        # Verify caller is school_admin
        db = _get_db()
        caller_doc = db.collection('users').document(caller_email).get()
        if not caller_doc.exists:
            return _err('Account not found', 403, request)
        caller_data = caller_doc.to_dict()
        if caller_data.get('role') not in ('school_admin', 'super_admin'):
            return _err('Not authorised', 403, request)

        body      = request.get_json(force=True, silent=True) or {}
        reference = (body.get('reference') or '').strip()
        school_id = (body.get('schoolId')   or '').strip()
        amount    = int(body.get('amount', 0))

        if not reference or not school_id:
            return _err('reference and schoolId required', 400, request)

        # Verify with Paystack API
        paystack_secret = os.environ.get('PAYSTACK_SECRET_KEY', '')
        if not paystack_secret:
            return _err('Payment gateway not configured', 500, request)

        resp = requests.get(
            f'https://api.paystack.co/transaction/verify/{reference}',
            headers={'Authorization': f'Bearer {paystack_secret}'},
            timeout=10,
        )
        data = resp.json()

        if not data.get('status') or data.get('data', {}).get('status') != 'success':
            return _err('Payment not successful', 402, request)

        paid_amount_kobo = data['data'].get('amount', 0)
        if paid_amount_kobo < amount * 100:
            return _err('Payment amount insufficient', 402, request)

        # Activate subscription — set start to today, duration from school doc
        school_snap = db.collection('schools').document(school_id).get()
        if not school_snap.exists:
            return _err('School not found', 404, request)
        school_data = school_snap.to_dict()

        # Verify caller belongs to this school (unless super_admin)
        if caller_data.get('role') == 'school_admin':
            if caller_data.get('schoolId') != school_id:
                return _err('Not authorised for this school', 403, request)

        sub_days = school_data.get('subscriptionDays', 270)
        db.collection('schools').document(school_id).update({
            'subscriptionStart': datetime.now(timezone.utc).isoformat(),
            'subscriptionDays':  sub_days,
            'lastPaymentRef':    reference,
            'lastPaymentAmount': paid_amount_kobo // 100,
            'lastPaymentDate':   datetime.now(timezone.utc).isoformat(),
        })

        return _json_resp({'ok': True, 'message': 'Subscription activated'}, req=request)

    except PermissionError as e:
        return _err(str(e), 403, request)
    except Exception as e:
        traceback.print_exc()
        return _err(str(e), 500, request)


# ── Eager Firebase init at startup ────────────────────────────────────────────
# Initialize Firebase Admin when gunicorn loads the module, not lazily per-request.
# This catches missing SERVICE_ACCOUNT_JSON immediately on startup.
try:
    _get_db()
    print('[STARTUP] Firebase Admin initialized successfully')
except Exception as _e:
    print('[STARTUP] WARNING: Firebase Admin init failed:', _e)




# ── Eager Firebase init at startup ────────────────────────────────────────────
# Initialize Firebase Admin when gunicorn loads the module, not lazily per-request.
# This catches missing SERVICE_ACCOUNT_JSON immediately on startup.
try:
    _get_db()
    print('[STARTUP] Firebase Admin initialized successfully')
except Exception as _e:
    print('[STARTUP] WARNING: Firebase Admin init failed:', _e)



# =============================================================================
# RDS — Result Distribution System Integration
# Token bridge: CBT generates signed HMAC-SHA256 token → RDS validates it
# =============================================================================

import hmac
import hashlib
import base64
import uuid

RDS_URL = os.environ.get('RDS_URL', 'https://edutest-rds.web.app')


def _rds_secret():
    secret = os.environ.get('RDS_BRIDGE_SECRET', '')
    if not secret:
        raise PermissionError('RDS_BRIDGE_SECRET not configured on server')
    return secret


def _make_rds_cors(req=None):
    """CORS headers that allow both CBT and RDS frontend origins."""
    origin = (req.headers.get('Origin', '') if req else '') or ''

    # Build set of allowed origins — includes all known Firebase Hosting domains
    rds_base = RDS_URL.rstrip('/')
    allowed_origins = {
        # CBT origins
        'https://edutest-pro-cbt.web.app',
        'https://edutest-pro-cbt.firebaseapp.com',
        # RDS origins (web.app and firebaseapp.com variants)
        rds_base,
        rds_base.replace('.web.app', '.firebaseapp.com'),
        rds_base + ':443',
    }

    # Allow any *.web.app or *.firebaseapp.com origin (all Firebase Hosting)
    is_firebase = (
        origin.endswith('.web.app') or
        origin.endswith('.firebaseapp.com')
    )

    if origin in allowed_origins or is_firebase:
        allowed = origin
    else:
        allowed = rds_base

    return {
        'Access-Control-Allow-Origin':  allowed,
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-EduTest-Key',
        'Access-Control-Allow-Credentials': 'false',
        'Vary': 'Origin',
    }


@app.route('/launch-rds', methods=['POST', 'OPTIONS'])
def launch_rds():
    """
    Called by CBT app.js when a user clicks "Result Distribution".
    Generates a signed one-time token and returns the RDS redirect URL.
    Only school_admin, sub_admin, teacher, and super_admin can access RDS.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))

    try:
        _secret_ok(request)
        caller_email, _ = _verify_token(request)

        db = _get_db()
        caller_doc = db.collection('users').document(caller_email).get()
        if not caller_doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Account not found'}, 403, request)

        caller = caller_doc.to_dict()
        allowed_roles = ('super_admin', 'school_admin', 'sub_admin', 'teacher')
        if caller.get('role') not in allowed_roles:
            return _rds_json_resp({'ok': False, 'error': 'Your role does not have access to Result Distribution System.'}, 403, request)

        # Build token payload
        nonce = uuid.uuid4().hex
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        payload = {
            'uid':      caller_email,
            'role':     caller.get('role'),
            'schoolId': caller.get('schoolId', ''),
            'email':    caller_email,
            'name':     caller.get('name', caller_email),
            'iat':      now_ms,
            'exp':      now_ms + 60000,   # 60-second window
            'nonce':    nonce,
        }

        # Encode and sign
        encoded = base64.urlsafe_b64encode(
            json.dumps(payload, separators=(',', ':')).encode()
        ).rstrip(b'=').decode()

        sig = hmac.new(
            _rds_secret().encode(),
            encoded.encode(),
            hashlib.sha256
        ).hexdigest()

        token = f'{encoded}.{sig}'

        # Store nonce in Firestore (one-time use enforcement)
        db.collection('rds_nonces').document(nonce).set({
            'uid':      caller_email,
            'schoolId': caller.get('schoolId', ''),
            'usedAt':   None,
            'expiresAt': datetime.fromtimestamp((now_ms + 60000) / 1000, tz=timezone.utc),
            'createdAt': datetime.now(timezone.utc),
        })

        redirect_url = f'{RDS_URL}/access?token={requests.utils.quote(token)}'
        return _rds_json_resp({'ok': True, 'redirectUrl': redirect_url}, req=request)

    except PermissionError as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 403, request)
    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-verify', methods=['POST', 'OPTIONS'])
def rds_verify():
    """
    Called by rds.html AccessGate on first load.
    Verifies the token, consumes the nonce, creates an 8-hour session.
    No auth header needed here — the token IS the credential.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))

    try:
        body = request.get_json(force=True, silent=True) or {}
        token = (body.get('token') or '').strip()

        if not token or '.' not in token:
            return _rds_json_resp({'ok': False, 'errorCode': 'INVALID_TOKEN_FORMAT',
                'message': 'Malformed access token. Please try again from CBT.'}, 400, request)

        # Split token
        parts = token.rsplit('.', 1)
        if len(parts) != 2:
            return _rds_json_resp({'ok': False, 'errorCode': 'INVALID_TOKEN_FORMAT',
                'message': 'Malformed access token.'}, 400, request)

        encoded, received_sig = parts

        # Verify signature
        expected_sig = hmac.new(
            _rds_secret().encode(),
            encoded.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected_sig, received_sig):
            return _rds_json_resp({'ok': False, 'errorCode': 'INVALID_SIGNATURE',
                'message': 'Access token signature is invalid. Please try again from CBT.'}, 401, request)

        # Decode payload
        try:
            padding = 4 - len(encoded) % 4
            padded = encoded + '=' * (padding % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        except Exception:
            return _rds_json_resp({'ok': False, 'errorCode': 'INVALID_PAYLOAD',
                'message': 'Access token data is corrupted. Please try again from CBT.'}, 400, request)

        # Check required fields
        required = ('uid', 'role', 'email', 'name', 'iat', 'exp', 'nonce')
        if not all(k in payload for k in required):
            return _rds_json_resp({'ok': False, 'errorCode': 'INCOMPLETE_PAYLOAD',
                'message': 'Access token is missing required data. Please try again from CBT.'}, 400, request)

        # Check expiry
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if now_ms > payload['exp']:
            return _rds_json_resp({'ok': False, 'errorCode': 'TOKEN_EXPIRED',
                'message': 'Access token has expired. Please click Result Distribution again in CBT.'}, 401, request)

        # Check nonce (one-time use)
        db = _get_db()
        nonce = payload['nonce']
        nonce_doc = db.collection('rds_nonces').document(nonce).get()

        if not nonce_doc.exists:
            return _rds_json_resp({'ok': False, 'errorCode': 'INVALID_NONCE',
                'message': 'Invalid access token. Please try again from CBT.'}, 401, request)

        nonce_data = nonce_doc.to_dict()
        if nonce_data.get('usedAt') is not None:
            return _rds_json_resp({'ok': False, 'errorCode': 'TOKEN_ALREADY_USED',
                'message': 'This access token has already been used. Please click Result Distribution again in CBT.'}, 401, request)

        # Mark nonce as used
        db.collection('rds_nonces').document(nonce).update({
            'usedAt': datetime.now(timezone.utc)
        })

        # Check school subscription (skip for super_admin)
        role = payload['role']
        school_id = payload.get('schoolId', '')
        if role != 'super_admin' and school_id:
            school_snap = db.collection('schools').document(school_id).get()
            if school_snap.exists:
                school_data = school_snap.to_dict()
                # Use the same calcSubStatus logic as the frontend
                sub_start = school_data.get('subscriptionStart')
                sub_days  = school_data.get('subscriptionDays', 0)
                grace     = school_data.get('gracePeriodDays', 7)
                paused    = school_data.get('subscriptionPaused', False)

                if paused:
                    return _rds_json_resp({'ok': False, 'errorCode': 'SUBSCRIPTION_PAUSED',
                        'message': 'Result Distribution is not available — subscription is paused. Contact support.'}, 403, request)

                if sub_start and sub_days:
                    from datetime import timedelta
                    start_dt = datetime.fromisoformat(sub_start.replace('Z', '+00:00')) if isinstance(sub_start, str) else sub_start
                    expiry   = start_dt + timedelta(days=sub_days)
                    grace_end = expiry + timedelta(days=grace)
                    now_dt    = datetime.now(timezone.utc)
                    if now_dt > grace_end:
                        return _rds_json_resp({'ok': False, 'errorCode': 'SUBSCRIPTION_EXPIRED',
                            'message': 'RDS subscription has expired. Please renew in EduTest Pro CBT.'}, 403, request)

        # Create 8-hour session
        session_id = str(uuid.uuid4())
        expires_at = datetime.now(timezone.utc).replace(
            second=0, microsecond=0
        )
        from datetime import timedelta
        expires_at = datetime.now(timezone.utc) + timedelta(hours=8)

        session_data = {
            'sessionId': session_id,
            'uid':       payload['uid'],
            'role':      role,
            'schoolId':  school_id,
            'email':     payload['email'],
            'name':      payload['name'],
            'createdAt': datetime.now(timezone.utc),
            'expiresAt': expires_at,
            'lastActive': datetime.now(timezone.utc),
        }
        db.collection('rds_sessions').document(session_id).set(session_data)

        return _rds_json_resp({
            'ok':        True,
            'sessionId': session_id,
            'user': {
                'uid':      payload['uid'],
                'role':     role,
                'schoolId': school_id,
                'email':    payload['email'],
                'name':     payload['name'],
            },
            'expiresAt': expires_at.isoformat(),
        }, req=request)

    except PermissionError as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 403, request)
    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-validate-session', methods=['POST', 'OPTIONS'])
def rds_validate_session():
    """Validate an existing RDS session. Called on every RDS page load."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))

    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = (body.get('sessionId') or '').strip()

        if not session_id:
            return _rds_json_resp({'valid': False, 'reason': 'NO_SESSION'}, 401, request)

        db = _get_db()
        snap = db.collection('rds_sessions').document(session_id).get()

        if not snap.exists:
            return _rds_json_resp({'valid': False, 'reason': 'SESSION_NOT_FOUND'}, 401, request)

        sess = snap.to_dict()
        expires_at = sess.get('expiresAt')

        # Handle both datetime objects and strings
        if isinstance(expires_at, str):
            from datetime import timedelta
            expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))

        if expires_at and datetime.now(timezone.utc) > expires_at:
            db.collection('rds_sessions').document(session_id).delete()
            return _rds_json_resp({'valid': False, 'reason': 'SESSION_EXPIRED'}, 401, request)

        # Update lastActive
        db.collection('rds_sessions').document(session_id).update({
            'lastActive': datetime.now(timezone.utc)
        })

        return _rds_json_resp({
            'valid': True,
            'user': {
                'uid':      sess.get('uid'),
                'role':     sess.get('role'),
                'schoolId': sess.get('schoolId'),
                'email':    sess.get('email'),
                'name':     sess.get('name'),
            },
        }, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'valid': False, 'reason': str(e)}, 500, request)


@app.route('/rds-logout', methods=['POST', 'OPTIONS'])
def rds_logout():
    """Destroy an RDS session."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))

    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = (body.get('sessionId') or '').strip()

        if session_id:
            db = _get_db()
            db.collection('rds_sessions').document(session_id).delete()

        return _rds_json_resp({'ok': True}, req=request)

    except Exception as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)



# =============================================================================
# RDS — Phase 1: Student Registration
# Collection: rds_students (in CBT Firestore, edutest-pro-6dd48)
# 
# Document schema:
#   id            : auto-generated
#   schoolId      : from session
#   teacherId     : uid of teacher who added/manages this student
#   name          : student full name
#   email         : student Gmail (nullable for non-CBT students)
#   classGrade    : e.g. "SS2A", "JSS1B"
#   admissionNumber: school admission number (optional)
#   parentEmail   : parent/guardian email for result delivery
#   parentWhatsapp: parent WhatsApp number (with country code)
#   source        : "cbt" | "manual"
#   cbtSynced     : true if pulled from CBT users collection
#   createdAt     : timestamp
#   updatedAt     : timestamp
#   createdBy     : teacher uid
# =============================================================================


@app.route('/rds-students/list', methods=['POST', 'OPTIONS'])
def rds_students_list():
    """
    List all RDS students for a school.
    Teacher sees own students; school_admin and sub_admin see all.
    Accepts optional filters: classGrade, source.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        body      = request.get_json(force=True, silent=True) or {}
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()

        db = _get_db()
        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s = sess.to_dict()
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        q = db.collection('rds_students').where('schoolId', '==', school_id)

        # Teachers only see students they registered
        if role == 'teacher':
            q = q.where('teacherId', '==', teacher_id)

        # Optional filters
        if body.get('classGrade'):
            q = q.where('classGrade', '==', body['classGrade'])
        if body.get('source'):
            q = q.where('source', '==', body['source'])

        docs = q.order_by('name').get()
        students = []
        for d in docs:
            row = d.to_dict()
            row['id'] = d.id
            # Convert timestamps to ISO strings
            for f in ('createdAt', 'updatedAt'):
                if row.get(f) and hasattr(row[f], 'isoformat'):
                    row[f] = row[f].isoformat()
            students.append(row)

        return _rds_json_resp({'ok': True, 'students': students, 'count': len(students)}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-students/sync-cbt', methods=['POST', 'OPTIONS'])
def rds_students_sync_cbt():
    """
    Pull CBT students for this school into rds_students.
    Only creates records that do not already exist (matched by email).
    Returns counts: { created, skipped, total }.
    Called by teacher — imports their school's CBT students into RDS.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s = sess.to_dict()
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        if role not in ('teacher', 'school_admin', 'sub_admin', 'super_admin'):
            return _rds_json_resp({'ok': False, 'error': 'Not authorised'}, 403, request)

        # Fetch all CBT students for this school
        cbt_snap = db.collection('users')             .where('schoolId', '==', school_id)             .where('role', '==', 'student')             .get()

        created = 0
        skipped = 0

        for doc in cbt_snap:
            cbt = doc.to_dict()
            email = doc.id  # CBT uses email as doc ID

            # Check if already in rds_students
            existing = db.collection('rds_students')                 .where('schoolId', '==', school_id)                 .where('email', '==', email)                 .limit(1).get()

            if existing:
                skipped += 1
                continue

            # Create rds_students record
            db.collection('rds_students').add({
                'schoolId':        school_id,
                'teacherId':       teacher_id,
                'name':            cbt.get('name', ''),
                'email':           email,
                'classGrade':      cbt.get('classGrade', ''),
                'admissionNumber': '',
                'parentEmail':     '',
                'parentWhatsapp':  '',
                'source':          'cbt',
                'cbtSynced':       True,
                'createdAt':       datetime.now(timezone.utc),
                'updatedAt':       datetime.now(timezone.utc),
                'createdBy':       teacher_id,
            })
            created += 1

        return _rds_json_resp({
            'ok':      True,
            'created': created,
            'skipped': skipped,
            'total':   created + skipped,
            'message': f'{created} student(s) imported, {skipped} already existed.',
        }, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-students/add', methods=['POST', 'OPTIONS'])
def rds_students_add():
    """
    Manually add a single student (non-CBT or additional details).
    Required: name, classGrade
    Optional: email, admissionNumber, parentEmail, parentWhatsapp
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s = sess.to_dict()
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body = request.get_json(force=True, silent=True) or {}
        name       = (body.get('name') or '').strip()
        class_grade = (body.get('classGrade') or '').strip()

        if not name:
            return _rds_json_resp({'ok': False, 'error': 'Student name is required.'}, 400, request)
        if not class_grade:
            return _rds_json_resp({'ok': False, 'error': 'Class/Grade is required.'}, 400, request)

        email = (body.get('email') or '').strip().lower()

        # Check for duplicate email within school
        if email:
            dup = db.collection('rds_students')                 .where('schoolId', '==', school_id)                 .where('email', '==', email)                 .limit(1).get()
            if dup:
                return _rds_json_resp({'ok': False, 'error': f'A student with email {email} already exists in this school.'}, 409, request)

        ref, _ = db.collection('rds_students').add({
            'schoolId':        school_id,
            'teacherId':       teacher_id,
            'name':            name,
            'email':           email,
            'classGrade':      class_grade,
            'admissionNumber': (body.get('admissionNumber') or '').strip(),
            'parentEmail':     (body.get('parentEmail') or '').strip().lower(),
            'parentWhatsapp':  (body.get('parentWhatsapp') or '').strip(),
            'source':          'manual',
            'cbtSynced':       False,
            'createdAt':       datetime.now(timezone.utc),
            'updatedAt':       datetime.now(timezone.utc),
            'createdBy':       teacher_id,
        })

        return _rds_json_resp({'ok': True, 'id': ref.id, 'message': f'Student "{name}" added successfully.'}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-students/update/<student_id>', methods=['POST', 'OPTIONS'])
def rds_students_update(student_id):
    """
    Update parent contact details and other fields on an rds_student.
    Teacher can update their own students.
    school_admin / sub_admin can update any student in their school.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s = sess.to_dict()
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        doc_ref = db.collection('rds_students').document(student_id)
        doc     = doc_ref.get()
        if not doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Student not found.'}, 404, request)

        existing = doc.to_dict()
        if existing.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)

        # Teachers can only update their own students
        if role == 'teacher' and existing.get('teacherId') != teacher_id:
            return _rds_json_resp({'ok': False, 'error': 'You can only update students you registered.'}, 403, request)

        body = request.get_json(force=True, silent=True) or {}

        # Only allow updating safe fields
        allowed = ('parentEmail', 'parentWhatsapp', 'admissionNumber', 'classGrade', 'name')
        updates = {k: (body[k] or '').strip() for k in allowed if k in body}
        updates['updatedAt'] = datetime.now(timezone.utc)

        doc_ref.update(updates)
        return _rds_json_resp({'ok': True, 'message': 'Student updated successfully.'}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-students/delete/<student_id>', methods=['POST', 'OPTIONS'])
def rds_students_delete(student_id):
    """Delete a manually-added student (cannot delete CBT-synced students)."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s = sess.to_dict()
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        doc_ref = db.collection('rds_students').document(student_id)
        doc     = doc_ref.get()
        if not doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Student not found.'}, 404, request)

        existing = doc.to_dict()
        if existing.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)

        if role == 'teacher' and existing.get('teacherId') != teacher_id:
            return _rds_json_resp({'ok': False, 'error': 'You can only delete students you registered.'}, 403, request)

        doc_ref.delete()
        return _rds_json_resp({'ok': True, 'message': 'Student deleted.'}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-students/bulk-add', methods=['POST', 'OPTIONS'])
def rds_students_bulk_add():
    """
    Bulk add non-CBT students from a JSON array.
    Each item: { name, classGrade, admissionNumber?, parentEmail?, parentWhatsapp?, email? }
    Returns { created, failed[] }
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s = sess.to_dict()
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body = request.get_json(force=True, silent=True) or {}
        rows = body.get('students', [])

        if not rows or not isinstance(rows, list):
            return _rds_json_resp({'ok': False, 'error': 'No student data provided.'}, 400, request)

        created = 0
        failed  = []
        batch   = db.batch()
        count   = 0

        for row in rows:
            name        = (row.get('name') or '').strip()
            class_grade = (row.get('classGrade') or '').strip()

            if not name or not class_grade:
                failed.append(f'Row skipped — name and classGrade are required: {row}')
                continue

            email = (row.get('email') or '').strip().lower()

            # Duplicate check for email
            if email:
                dup = db.collection('rds_students')                     .where('schoolId', '==', school_id)                     .where('email', '==', email)                     .limit(1).get()
                if dup:
                    failed.append(f'{name} ({email}) — already exists')
                    continue

            ref = db.collection('rds_students').document()
            batch.set(ref, {
                'schoolId':        school_id,
                'teacherId':       teacher_id,
                'name':            name,
                'email':           email,
                'classGrade':      class_grade,
                'admissionNumber': (row.get('admissionNumber') or '').strip(),
                'parentEmail':     (row.get('parentEmail') or '').strip().lower(),
                'parentWhatsapp':  (row.get('parentWhatsapp') or '').strip(),
                'source':          'manual',
                'cbtSynced':       False,
                'createdAt':       datetime.now(timezone.utc),
                'updatedAt':       datetime.now(timezone.utc),
                'createdBy':       teacher_id,
            })
            created += 1
            count   += 1

            # Firestore batch limit is 500
            if count == 499:
                batch.commit()
                batch  = db.batch()
                count  = 0

        if count > 0:
            batch.commit()

        return _rds_json_resp({
            'ok':      True,
            'created': created,
            'failed':  failed,
            'message': f'{created} student(s) added successfully.',
        }, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-students/classes', methods=['POST', 'OPTIONS'])
def rds_students_classes():
    """Return distinct classGrade values for this school's rds_students."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        school_id = sess.to_dict().get('schoolId')

        docs   = db.collection('rds_students').where('schoolId', '==', school_id).get()
        grades = sorted(set(d.to_dict().get('classGrade', '') for d in docs if d.to_dict().get('classGrade')))

        return _rds_json_resp({'ok': True, 'classes': grades}, req=request)

    except Exception as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)



# =============================================================================
# RDS — Phase 2: Result File Creation & Management
# Collections: rds_result_files, rds_result_entries
# =============================================================================

# ── Grading helpers ────────────────────────────────────────────────────────────

def _compute_grade(total, max_score=100):
    """Nigerian WAEC/NECO grading system."""
    if max_score <= 0: return 'F9'
    pct = (total / max_score) * 100
    if pct >= 75: return 'A1'
    if pct >= 70: return 'B2'
    if pct >= 65: return 'B3'
    if pct >= 60: return 'C4'
    if pct >= 55: return 'C5'
    if pct >= 50: return 'C6'
    if pct >= 45: return 'D7'
    if pct >= 40: return 'E8'
    return 'F9'

def _compute_remark(grade):
    remarks = {
        'A1': 'Excellent', 'B2': 'Very Good', 'B3': 'Good',
        'C4': 'Credit', 'C5': 'Credit', 'C6': 'Credit',
        'D7': 'Pass', 'E8': 'Pass', 'F9': 'Fail'
    }
    return remarks.get(grade, '—')

def _compute_positions(entries):
    """
    Assign class positions based on totalScore descending.
    Handles ties — students with equal totalScore share a position.
    Returns dict: {entryId: position_string}
    """
    sorted_entries = sorted(entries, key=lambda e: e.get('totalScore', 0), reverse=True)
    positions = {}
    pos = 1
    for i, entry in enumerate(sorted_entries):
        if i > 0 and entry.get('totalScore', 0) < sorted_entries[i-1].get('totalScore', 0):
            pos = i + 1
        positions[entry['id']] = pos
    return positions

def _ordinal(n):
    if 11 <= n % 100 <= 13:
        return f'{n}th'
    return f'{n}{["th","st","nd","rd","th","th","th","th","th","th"][n % 10]}'


# ── Result File CRUD ───────────────────────────────────────────────────────────

@app.route('/rds-result-files/create', methods=['POST', 'OPTIONS'])
def rds_result_files_create():
    """
    Create a new result file (draft).
    Required: session, term, classGrade, subject
    Optional: cbtExamId (link to CBT exam for auto-pulling scores)
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s          = sess.to_dict()
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')
        teacher_name = s.get('name', '')

        body       = request.get_json(force=True, silent=True) or {}
        session_yr = (body.get('session') or '').strip()
        term       = (body.get('term') or '').strip()
        class_grade= (body.get('classGrade') or '').strip()
        subject    = (body.get('subject') or '').strip()
        cbt_exam_id= (body.get('cbtExamId') or '').strip()

        if not session_yr: return _rds_json_resp({'ok': False, 'error': 'Academic session is required.'}, 400, request)
        if not term:       return _rds_json_resp({'ok': False, 'error': 'Term is required.'}, 400, request)
        if not class_grade:return _rds_json_resp({'ok': False, 'error': 'Class/Grade is required.'}, 400, request)
        if not subject:    return _rds_json_resp({'ok': False, 'error': 'Subject is required.'}, 400, request)

        # Prevent duplicate — same teacher, school, session, term, class, subject
        dup = db.collection('rds_result_files')             .where('schoolId',   '==', school_id)             .where('teacherId',  '==', teacher_id)             .where('session',    '==', session_yr)             .where('term',       '==', term)             .where('classGrade', '==', class_grade)             .where('subject',    '==', subject)             .limit(1).get()
        if dup:
            return _rds_json_resp({'ok': False, 'error': f'A result file for {subject} — {class_grade} — {term} {session_yr} already exists.'}, 409, request)

        ref, _ = db.collection('rds_result_files').add({
            'schoolId':        school_id,
            'teacherId':       teacher_id,
            'teacherName':     teacher_name,
            'session':         session_yr,
            'term':            term,
            'classGrade':      class_grade,
            'subject':         subject,
            'cbtExamId':       cbt_exam_id,
            'status':          'draft',
            'totalStudents':   0,
            'createdAt':       datetime.now(timezone.utc),
            'updatedAt':       datetime.now(timezone.utc),
            'submittedAt':     None,
            'approvedAt':      None,
            'approvedBy':      None,
            'rejectedAt':      None,
            'rejectedBy':      None,
            'rejectionComment':None,
            'distributedAt':   None,
        })

        return _rds_json_resp({'ok': True, 'fileId': ref.id,
            'message': f'Result file created for {subject} — {class_grade}.'}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-result-files/list', methods=['POST', 'OPTIONS'])
def rds_result_files_list():
    """List result files — teachers see own, admins/coordinators see all school files."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s          = sess.to_dict()
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        q = db.collection('rds_result_files').where('schoolId', '==', school_id)
        if role == 'teacher':
            q = q.where('teacherId', '==', teacher_id)

        body = request.get_json(force=True, silent=True) or {}
        if body.get('status'):
            q = q.where('status', '==', body['status'])

        docs  = q.order_by('createdAt', direction='DESCENDING').get()
        files = []
        for d in docs:
            row = d.to_dict()
            row['id'] = d.id
            for f in ('createdAt', 'updatedAt', 'submittedAt', 'approvedAt',
                      'rejectedAt', 'distributedAt'):
                if row.get(f) and hasattr(row[f], 'isoformat'):
                    row[f] = row[f].isoformat()
            files.append(row)

        return _rds_json_resp({'ok': True, 'files': files, 'count': len(files)}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-result-files/get/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_get(file_id):
    """Get a single result file with all its entries."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s         = sess.to_dict()
        school_id = s.get('schoolId')

        doc = db.collection('rds_result_files').document(file_id).get()
        if not doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)

        file_data = doc.to_dict()
        if file_data.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)

        file_data['id'] = doc.id
        for f in ('createdAt', 'updatedAt', 'submittedAt', 'approvedAt',
                  'rejectedAt', 'distributedAt'):
            if file_data.get(f) and hasattr(file_data[f], 'isoformat'):
                file_data[f] = file_data[f].isoformat()

        # Fetch entries
        entries_snap = db.collection('rds_result_entries')             .where('resultFileId', '==', file_id).get()
        entries = []
        for e in entries_snap:
            row = e.to_dict()
            row['id'] = e.id
            for f in ('createdAt', 'updatedAt'):
                if row.get(f) and hasattr(row[f], 'isoformat'):
                    row[f] = row[f].isoformat()
            entries.append(row)

        # Sort by student name
        entries.sort(key=lambda e: e.get('studentName', ''))

        # Compute positions
        positions = _compute_positions(entries)
        for entry in entries:
            entry['position'] = positions.get(entry['id'], '—')
            entry['positionStr'] = _ordinal(entry['position']) if isinstance(entry['position'], int) else '—'

        return _rds_json_resp({'ok': True, 'file': file_data, 'entries': entries}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-result-files/populate', methods=['POST', 'OPTIONS'])
def rds_result_files_populate():
    """
    Populate a result file with students from rds_students (same classGrade).
    For CBT students: auto-pull exam score from submissions if cbtExamId is set.
    For manual students: create blank entries for teacher to fill.
    Skips students already in this file.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s          = sess.to_dict()
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body    = request.get_json(force=True, silent=True) or {}
        file_id = (body.get('fileId') or '').strip()
        if not file_id:
            return _rds_json_resp({'ok': False, 'error': 'fileId required.'}, 400, request)

        # Get result file
        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') != 'draft':
            return _rds_json_resp({'ok': False, 'error': 'Only draft files can be populated.'}, 400, request)

        class_grade  = fd.get('classGrade', '')
        cbt_exam_id  = fd.get('cbtExamId', '')

        # Get students for this class
        students_snap = db.collection('rds_students')             .where('schoolId',   '==', school_id)             .where('classGrade', '==', class_grade).get()

        # Get existing entries to skip duplicates
        existing_snap = db.collection('rds_result_entries')             .where('resultFileId', '==', file_id).get()
        existing_student_ids = {e.to_dict().get('studentId') for e in existing_snap}

        # Load CBT submissions for this exam if linked
        cbt_scores = {}
        if cbt_exam_id:
            subs_snap = db.collection('submissions')                 .where('examId', '==', cbt_exam_id)                 .where('schoolId', '==', school_id).get()
            for sub in subs_snap:
                sd = sub.to_dict()
                email = sd.get('studentEmail', '').lower()
                if email:
                    cbt_scores[email] = {
                        'score':      sd.get('score', 0),
                        'total':      sd.get('total', 0),
                        'percentage': sd.get('percentage', 0),
                    }

        created = 0
        skipped = 0
        batch   = db.batch()
        count   = 0

        for student_doc in students_snap:
            student_id = student_doc.id
            sd         = student_doc.to_dict()

            if student_id in existing_student_ids:
                skipped += 1
                continue

            email  = (sd.get('email') or '').lower()
            source = sd.get('source', 'manual')

            # Pull CBT exam score if available
            cbt_data    = cbt_scores.get(email, {}) if (source == 'cbt' and email) else {}
            cbt_score   = cbt_data.get('score', None)
            cbt_total   = cbt_data.get('total', None)
            cbt_pct     = cbt_data.get('percentage', None)

            # For CBT students: map CBT percentage to exam score out of 60
            # e.g. 80% CBT score → 48/60 exam score
            exam_score_from_cbt = None
            if cbt_pct is not None:
                exam_score_from_cbt = round((cbt_pct / 100) * 60, 1)

            entry_ref = db.collection('rds_result_entries').document()
            batch.set(entry_ref, {
                'resultFileId':      file_id,
                'schoolId':          school_id,
                'studentId':         student_id,
                'studentName':       sd.get('name', ''),
                'classGrade':        class_grade,
                'source':            source,
                # CBT data (populated for CBT students)
                'cbtScore':          cbt_score,
                'cbtTotal':          cbt_total,
                'cbtPercentage':     cbt_pct,
                # Score fields (teacher fills in / pre-filled from CBT)
                'testScore':         None,
                'examScore':         exam_score_from_cbt,  # pre-fill from CBT or None
                'caScore':           None,
                # Computed (filled when teacher saves scores)
                'totalScore':        None,
                'grade':             None,
                'remark':            None,
                'position':          None,
                'createdAt':         datetime.now(timezone.utc),
                'updatedAt':         datetime.now(timezone.utc),
            })
            created += 1
            count   += 1
            if count == 499:
                batch.commit()
                batch  = db.batch()
                count  = 0

        if count > 0:
            batch.commit()

        # Update totalStudents on result file
        total_now = len(existing_student_ids) + created
        db.collection('rds_result_files').document(file_id).update({
            'totalStudents': total_now,
            'updatedAt':     datetime.now(timezone.utc),
        })

        return _rds_json_resp({
            'ok':      True,
            'created': created,
            'skipped': skipped,
            'total':   total_now,
            'message': f'{created} student(s) added to result file.',
        }, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-result-entries/save', methods=['POST', 'OPTIONS'])
def rds_result_entries_save():
    """
    Save scores for one or more entries.
    Accepts: { fileId, entries: [{id, testScore, examScore, caScore}, ...] }
    Auto-computes totalScore, grade, remark.
    Positions are recomputed across ALL entries for the file.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s         = sess.to_dict()
        school_id = s.get('schoolId')

        body    = request.get_json(force=True, silent=True) or {}
        file_id = (body.get('fileId') or '').strip()
        entries = body.get('entries', [])

        if not file_id:
            return _rds_json_resp({'ok': False, 'error': 'fileId required.'}, 400, request)

        # Verify file belongs to school and is draft
        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') not in ('draft',):
            return _rds_json_resp({'ok': False, 'error': 'Only draft files can be edited.'}, 400, request)

        # Save each entry
        batch = db.batch()
        count = 0

        for row in entries:
            entry_id  = (row.get('id') or '').strip()
            if not entry_id: continue

            test_score = row.get('testScore')
            exam_score = row.get('examScore')
            ca_score   = row.get('caScore')

            # Convert to float, treat empty/None as None
            def to_num(v):
                try: return float(v) if v is not None and str(v).strip() != '' else None
                except: return None

            test  = to_num(test_score)
            exam  = to_num(exam_score)
            ca    = to_num(ca_score)

            # Compute total — only if all three are present
            total = None
            grade = None
            remark = None
            if test is not None and exam is not None and ca is not None:
                total  = round(test + exam + ca, 1)
                max_sc = 130  # 40 + 60 + 30
                grade  = _compute_grade(total, max_sc)
                remark = _compute_remark(grade)

            update = {
                'testScore': test,
                'examScore': exam,
                'caScore':   ca,
                'totalScore':total,
                'grade':     grade,
                'remark':    remark,
                'updatedAt': datetime.now(timezone.utc),
            }
            ref = db.collection('rds_result_entries').document(entry_id)
            batch.update(ref, update)
            count += 1

            if count == 499:
                batch.commit()
                batch = db.batch()
                count = 0

        if count > 0:
            batch.commit()

        # Recompute positions across ALL entries in this file
        all_entries_snap = db.collection('rds_result_entries')             .where('resultFileId', '==', file_id).get()
        all_entries = []
        for e in all_entries_snap:
            row = e.to_dict()
            row['id'] = e.id
            all_entries.append(row)

        positions = _compute_positions([e for e in all_entries if e.get('totalScore') is not None])
        pos_batch = db.batch()
        pos_count = 0
        for entry in all_entries:
            pos = positions.get(entry['id'])
            if pos is not None:
                ref = db.collection('rds_result_entries').document(entry['id'])
                pos_batch.update(ref, {'position': pos})
                pos_count += 1
                if pos_count == 499:
                    pos_batch.commit()
                    pos_batch = db.batch()
                    pos_count = 0
        if pos_count > 0:
            pos_batch.commit()

        # Update file updatedAt
        db.collection('rds_result_files').document(file_id).update({
            'updatedAt': datetime.now(timezone.utc)
        })

        return _rds_json_resp({'ok': True, 'saved': len(entries),
            'message': 'Scores saved successfully.'}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-result-files/submit/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_submit(file_id):
    """
    Teacher submits a result file for coordinator/admin review.
    Validates all entries have complete scores before allowing submission.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s         = sess.to_dict()
        school_id = s.get('schoolId')
        role      = s.get('role')

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') != 'draft':
            return _rds_json_resp({'ok': False, 'error': 'Only draft files can be submitted.'}, 400, request)

        # Check all entries are complete
        entries_snap = db.collection('rds_result_entries')             .where('resultFileId', '==', file_id).get()
        entries = [e.to_dict() for e in entries_snap]

        if not entries:
            return _rds_json_resp({'ok': False,
                'error': 'No students in this result file. Please populate it first.'}, 400, request)

        incomplete = [e.get('studentName', '?') for e in entries
                      if e.get('totalScore') is None]
        if incomplete:
            return _rds_json_resp({'ok': False,
                'error': f'{len(incomplete)} student(s) have incomplete scores: {", ".join(incomplete[:5])}{"..." if len(incomplete) > 5 else ""}.'}, 400, request)

        db.collection('rds_result_files').document(file_id).update({
            'status':      'submitted',
            'submittedAt': datetime.now(timezone.utc),
            'updatedAt':   datetime.now(timezone.utc),
        })

        return _rds_json_resp({'ok': True,
            'message': 'Result file submitted for review.'}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-result-files/delete/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_delete(file_id):
    """Delete a draft result file and all its entries."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        s         = sess.to_dict()
        school_id = s.get('schoolId')
        role      = s.get('role')
        uid       = s.get('uid')

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') != 'draft':
            return _rds_json_resp({'ok': False, 'error': 'Only draft files can be deleted.'}, 400, request)
        if role == 'teacher' and fd.get('teacherId') != uid:
            return _rds_json_resp({'ok': False, 'error': 'You can only delete your own result files.'}, 403, request)

        # Delete all entries first
        entries_snap = db.collection('rds_result_entries')             .where('resultFileId', '==', file_id).get()
        batch = db.batch()
        count = 0
        for e in entries_snap:
            batch.delete(e.reference)
            count += 1
            if count == 499:
                batch.commit()
                batch = db.batch()
                count = 0
        if count > 0:
            batch.commit()

        # Delete the file
        db.collection('rds_result_files').document(file_id).delete()

        return _rds_json_resp({'ok': True, 'message': 'Result file deleted.'}, req=request)

    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-result-files/cbt-exams', methods=['POST', 'OPTIONS'])
def rds_cbt_exams_for_class():
    """
    Return list of CBT exams for a given classGrade in this school.
    Used in result file creation — teacher picks which CBT exam to link.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        session_id = (request.headers.get('Authorization') or '').replace('Session ', '').strip()
        db = _get_db()

        sess = db.collection('rds_sessions').document(session_id).get()
        if not sess.exists:
            return _rds_json_resp({'ok': False, 'error': 'Invalid session'}, 401, request)
        school_id = sess.to_dict().get('schoolId')

        body        = request.get_json(force=True, silent=True) or {}
        class_grade = (body.get('classGrade') or '').strip()

        q = db.collection('exams').where('schoolId', '==', school_id)
        if class_grade:
            q = q.where('targetClass', '==', class_grade)

        exams_snap = q.get()
        exams = []
        for e in exams_snap:
            ed = e.to_dict()
            exams.append({
                'id':          e.id,
                'title':       ed.get('title', ''),
                'targetClass': ed.get('targetClass', ''),
                'examTerm':    ed.get('examTerm', ''),
                'examType':    ed.get('examType', ''),
                'subject':     ed.get('subject', ed.get('title', '')),
            })
        exams.sort(key=lambda e: e['title'])

        return _rds_json_resp({'ok': True, 'exams': exams}, req=request)

    except Exception as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# ── Health check ──────────────────────────────────────────────────────────────
@app.route('/health', methods=['GET'])
def health():
    return make_response('{"ok":true}', 200, {'Content-Type':'application/json'})


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
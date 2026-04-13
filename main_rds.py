"""
main_rds.py — EduTest Pro RDS Blueprint
Covers: RDS auth (verify/validate/logout), Students, Result Files,
        Phase 3 Approval, Assignments, Subjects, Grade Settings,
        Broadsheet, and Search.
All routes are registered on the Flask Blueprint `rds_bp`.
"""

import os
import json
import traceback
import uuid
import hmac
import hashlib
import base64
import requests
from datetime import datetime, timedelta, timezone

from flask import Blueprint, request as flask_request, make_response
from firebase_admin import firestore as admin_firestore

from shared import (
    get_db, make_rds_cors, rds_json_resp, rds_session, ts,
    _font, _CENTER, _LEFT, _BORDER,
    _GREEN, _DARK, _GREY, _WHITE,
    RDS_URL,
)

rds_bp = Blueprint('rds', __name__)

# Role sets used across multiple endpoints
COORDINATOR_ROLES = {'sub_admin', 'school_admin', 'super_admin'}
ADMIN_ROLES       = {'school_admin', 'super_admin'}


def _rds_secret():
    secret = os.environ.get('RDS_BRIDGE_SECRET', '')
    if not secret:
        raise PermissionError('RDS_BRIDGE_SECRET not configured on server')
    return secret


@rds_bp.route('/rds-verify', methods=['POST', 'OPTIONS'])
def rds_verify():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        body  = request.get_json(force=True, silent=True) or {}
        token = (body.get('token') or '').strip()

        if not token or '.' not in token:
            return rds_json_resp({'ok': False, 'errorCode': 'INVALID_TOKEN_FORMAT',
                'message': 'Malformed access token. Please try again from CBT.'}, 400, request)

        parts = token.rsplit('.', 1)
        if len(parts) != 2:
            return rds_json_resp({'ok': False, 'errorCode': 'INVALID_TOKEN_FORMAT',
                'message': 'Malformed access token.'}, 400, request)

        encoded, received_sig = parts

        expected_sig = hmac.new(
            _rds_secret().encode(),
            encoded.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected_sig, received_sig):
            return rds_json_resp({'ok': False, 'errorCode': 'INVALID_SIGNATURE',
                'message': 'Access token signature is invalid. Please try again from CBT.'}, 401, request)

        try:
            padding = 4 - len(encoded) % 4
            padded  = encoded + '=' * (padding % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        except Exception:
            return rds_json_resp({'ok': False, 'errorCode': 'INVALID_PAYLOAD',
                'message': 'Access token data is corrupted. Please try again from CBT.'}, 400, request)

        required = ('uid', 'role', 'email', 'name', 'iat', 'exp', 'nonce')
        if not all(k in payload for k in required):
            return rds_json_resp({'ok': False, 'errorCode': 'INCOMPLETE_PAYLOAD',
                'message': 'Access token is missing required data. Please try again from CBT.'}, 400, request)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if now_ms > payload['exp']:
            return rds_json_resp({'ok': False, 'errorCode': 'TOKEN_EXPIRED',
                'message': 'Access token has expired. Please click Result Distribution again in CBT.'}, 401, request)

        db    = get_db()
        nonce = payload['nonce']
        nonce_doc = db.collection('rds_nonces').document(nonce).get()

        if not nonce_doc.exists:
            return rds_json_resp({'ok': False, 'errorCode': 'INVALID_NONCE',
                'message': 'Invalid access token. Please try again from CBT.'}, 401, request)

        nonce_data = nonce_doc.to_dict()
        if nonce_data.get('usedAt') is not None:
            return rds_json_resp({'ok': False, 'errorCode': 'TOKEN_ALREADY_USED',
                'message': 'This access token has already been used. Please click Result Distribution again in CBT.'}, 401, request)

        db.collection('rds_nonces').document(nonce).update({
            'usedAt': datetime.now(timezone.utc)
        })

        role      = payload['role']
        school_id = payload.get('schoolId', '')
        if role != 'super_admin' and school_id:
            school_snap = db.collection('schools').document(school_id).get()
            if school_snap.exists:
                school_data = school_snap.to_dict()
                sub_start   = school_data.get('subscriptionStart')
                sub_days    = school_data.get('subscriptionDays', 0)
                grace       = school_data.get('gracePeriodDays', 7)
                paused      = school_data.get('subscriptionPaused', False)

                if paused:
                    return rds_json_resp({'ok': False, 'errorCode': 'SUBSCRIPTION_PAUSED',
                        'message': 'Result Distribution is not available — subscription is paused. Contact support.'}, 403, request)

                if sub_start and sub_days:
                    start_dt  = datetime.fromisoformat(sub_start.replace('Z', '+00:00')) if isinstance(sub_start, str) else sub_start
                    expiry    = start_dt + timedelta(days=sub_days)
                    grace_end = expiry + timedelta(days=grace)
                    if datetime.now(timezone.utc) > grace_end:
                        return rds_json_resp({'ok': False, 'errorCode': 'SUBSCRIPTION_EXPIRED',
                            'message': 'RDS subscription has expired. Please renew in EduTest Pro CBT.'}, 403, request)

        session_id   = str(uuid.uuid4())
        expires_at   = datetime.now(timezone.utc) + timedelta(hours=8)
        session_data = {
            'sessionId':  session_id,
            'uid':        payload['uid'],
            'role':       role,
            'schoolId':   school_id,
            'email':      payload['email'],
            'name':       payload['name'],
            'createdAt':  datetime.now(timezone.utc),
            'expiresAt':  expires_at,
            'lastActive': datetime.now(timezone.utc),
        }
        db.collection('rds_sessions').document(session_id).set(session_data)

        return rds_json_resp({
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
        return rds_json_resp({'ok': False, 'error': str(e)}, 403, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-validate-session', methods=['POST', 'OPTIONS'])
def rds_validate_session():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        body       = request.get_json(force=True, silent=True) or {}
        session_id = (body.get('sessionId') or '').strip()

        if not session_id:
            return rds_json_resp({'valid': False, 'reason': 'NO_SESSION'}, 401, request)

        db   = get_db()
        snap = db.collection('rds_sessions').document(session_id).get()

        if not snap.exists:
            return rds_json_resp({'valid': False, 'reason': 'SESSION_NOT_FOUND'}, 401, request)

        sess       = snap.to_dict()
        expires_at = sess.get('expiresAt')
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))

        if expires_at and datetime.now(timezone.utc) > expires_at:
            db.collection('rds_sessions').document(session_id).delete()
            return rds_json_resp({'valid': False, 'reason': 'SESSION_EXPIRED'}, 401, request)

        db.collection('rds_sessions').document(session_id).update({
            'lastActive': datetime.now(timezone.utc)
        })

        return rds_json_resp({
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
        return rds_json_resp({'valid': False, 'reason': str(e)}, 500, request)


@rds_bp.route('/rds-logout', methods=['POST', 'OPTIONS'])
def rds_logout():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        body       = request.get_json(force=True, silent=True) or {}
        session_id = (body.get('sessionId') or '').strip()
        if session_id:
            get_db().collection('rds_sessions').document(session_id).delete()
        return rds_json_resp({'ok': True}, req=request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Phase 1: Student Registration
# =============================================================================

@rds_bp.route('/rds-students/list', methods=['POST', 'OPTIONS'])
def rds_students_list():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body = request.get_json(force=True, silent=True) or {}
        q    = db.collection('rds_students').where('schoolId', '==', school_id)
        if role == 'teacher':
            q = q.where('teacherId', '==', teacher_id)
        if body.get('classGrade'):
            q = q.where('classGrade', '==', body['classGrade'])
        if body.get('source'):
            q = q.where('source', '==', body['source'])

        docs     = q.order_by('name').get()
        students = []
        for d in docs:
            row = d.to_dict(); row['id'] = d.id
            for f in ('createdAt', 'updatedAt'):
                row[f] = ts(row.get(f))
            students.append(row)

        return rds_json_resp({'ok': True, 'students': students, 'count': len(students)}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-students/sync-cbt', methods=['POST', 'OPTIONS'])
def rds_students_sync_cbt():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        if role not in ('teacher', 'school_admin', 'sub_admin', 'super_admin'):
            return rds_json_resp({'ok': False, 'error': 'Not authorised'}, 403, request)

        cbt_snap = (db.collection('users')
                      .where('schoolId', '==', school_id)
                      .where('role', '==', 'student')
                      .get())

        created = 0
        skipped = 0

        for doc in cbt_snap:
            cbt   = doc.to_dict()
            email = doc.id

            existing = (db.collection('rds_students')
                          .where('schoolId', '==', school_id)
                          .where('email', '==', email)
                          .limit(1).get())
            if existing:
                skipped += 1
                continue

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

        return rds_json_resp({
            'ok':      True,
            'created': created,
            'skipped': skipped,
            'total':   created + skipped,
            'message': f'{created} student(s) imported, {skipped} already existed.',
        }, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-students/add', methods=['POST', 'OPTIONS'])
def rds_students_add():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body        = request.get_json(force=True, silent=True) or {}
        name        = (body.get('name') or '').strip()
        class_grade = (body.get('classGrade') or '').strip()

        if not name:        return rds_json_resp({'ok': False, 'error': 'Student name is required.'}, 400, request)
        if not class_grade: return rds_json_resp({'ok': False, 'error': 'Class/Grade is required.'}, 400, request)

        email = (body.get('email') or '').strip().lower()
        if email:
            dup = (db.collection('rds_students')
                     .where('schoolId', '==', school_id)
                     .where('email', '==', email)
                     .limit(1).get())
            if dup:
                return rds_json_resp({'ok': False,
                    'error': f'A student with email {email} already exists in this school.'}, 409, request)

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

        return rds_json_resp({'ok': True, 'id': ref.id,
            'message': f'Student "{name}" added successfully.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-students/update/<student_id>', methods=['POST', 'OPTIONS'])
def rds_students_update(student_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        doc_ref  = db.collection('rds_students').document(student_id)
        doc      = doc_ref.get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Student not found.'}, 404, request)

        existing = doc.to_dict()
        if existing.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if role == 'teacher' and existing.get('teacherId') != teacher_id:
            return rds_json_resp({'ok': False, 'error': 'You can only update students you registered.'}, 403, request)

        body    = request.get_json(force=True, silent=True) or {}
        allowed = ('parentEmail', 'parentWhatsapp', 'admissionNumber', 'classGrade', 'name')
        updates = {k: (body[k] or '').strip() for k in allowed if k in body}
        updates['updatedAt'] = datetime.now(timezone.utc)
        doc_ref.update(updates)

        return rds_json_resp({'ok': True, 'message': 'Student updated successfully.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-students/delete/<student_id>', methods=['POST', 'OPTIONS'])
def rds_students_delete(student_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        doc_ref  = db.collection('rds_students').document(student_id)
        doc      = doc_ref.get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Student not found.'}, 404, request)

        existing = doc.to_dict()
        if existing.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if role == 'teacher' and existing.get('teacherId') != teacher_id:
            return rds_json_resp({'ok': False, 'error': 'You can only delete students you registered.'}, 403, request)

        doc_ref.delete()
        return rds_json_resp({'ok': True, 'message': 'Student deleted.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-students/bulk-add', methods=['POST', 'OPTIONS'])
def rds_students_bulk_add():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body = request.get_json(force=True, silent=True) or {}
        rows = body.get('students', [])
        if not rows or not isinstance(rows, list):
            return rds_json_resp({'ok': False, 'error': 'No student data provided.'}, 400, request)

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
            if email:
                dup = (db.collection('rds_students')
                         .where('schoolId', '==', school_id)
                         .where('email', '==', email)
                         .limit(1).get())
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
            if count == 499:
                batch.commit()
                batch = db.batch()
                count = 0

        if count > 0:
            batch.commit()

        return rds_json_resp({
            'ok':      True,
            'created': created,
            'failed':  failed,
            'message': f'{created} student(s) added successfully.',
        }, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-students/classes', methods=['POST', 'OPTIONS'])
def rds_students_classes():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db        = get_db()
        s         = rds_session(request, db)
        school_id = s.get('schoolId')

        docs   = db.collection('rds_students').where('schoolId', '==', school_id).get()
        grades = sorted(set(
            d.to_dict().get('classGrade', '')
            for d in docs if d.to_dict().get('classGrade')
        ))
        return rds_json_resp({'ok': True, 'classes': grades}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Phase 2: Result File Creation & Management
# =============================================================================

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
    return {
        'A1': 'Excellent', 'B2': 'Very Good', 'B3': 'Good',
        'C4': 'Credit',    'C5': 'Credit',    'C6': 'Credit',
        'D7': 'Pass',      'E8': 'Pass',      'F9': 'Fail',
    }.get(grade, '—')


def _compute_positions(entries):
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


def _serialize_file(doc):
    row = doc.to_dict()
    row['id'] = doc.id
    for f in ('createdAt', 'updatedAt', 'submittedAt', 'approvedAt',
              'rejectedAt', 'distributedAt'):
        row[f] = ts(row.get(f))
    return row


@rds_bp.route('/rds-result-files/create', methods=['POST', 'OPTIONS'])
def rds_result_files_create():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id    = s.get('schoolId')
        teacher_id   = s.get('uid')
        teacher_name = s.get('name', '')

        body        = request.get_json(force=True, silent=True) or {}
        session_yr  = (body.get('session')    or '').strip()
        term        = (body.get('term')        or '').strip()
        class_grade = (body.get('classGrade') or '').strip()
        subject     = (body.get('subject')    or '').strip()
        cbt_exam_id = (body.get('cbtExamId')  or '').strip()

        if not session_yr:  return rds_json_resp({'ok': False, 'error': 'Academic session is required.'}, 400, request)
        if not term:        return rds_json_resp({'ok': False, 'error': 'Term is required.'}, 400, request)
        if not class_grade: return rds_json_resp({'ok': False, 'error': 'Class/Grade is required.'}, 400, request)
        if not subject:     return rds_json_resp({'ok': False, 'error': 'Subject is required.'}, 400, request)

        dup = (db.collection('rds_result_files')
                 .where('schoolId',   '==', school_id)
                 .where('teacherId',  '==', teacher_id)
                 .where('session',    '==', session_yr)
                 .where('term',       '==', term)
                 .where('classGrade', '==', class_grade)
                 .where('subject',    '==', subject)
                 .limit(1).get())
        if dup:
            return rds_json_resp({'ok': False,
                'error': f'A result file for {subject} — {class_grade} — {term} {session_yr} already exists.'}, 409, request)

        ref, _ = db.collection('rds_result_files').add({
            'schoolId':         school_id,
            'teacherId':        teacher_id,
            'teacherName':      teacher_name,
            'session':          session_yr,
            'term':             term,
            'classGrade':       class_grade,
            'subject':          subject,
            'cbtExamId':        cbt_exam_id,
            # Phase 2 status flow: draft → submitted → approved | rejected → distributed
            'status':           'draft',
            'totalStudents':    0,
            'createdAt':        datetime.now(timezone.utc),
            'updatedAt':        datetime.now(timezone.utc),
            'submittedAt':      None,
            'approvedAt':       None,
            'approvedBy':       None,
            'approvedByName':   None,
            'rejectedAt':       None,
            'rejectedBy':       None,
            'rejectedByName':   None,
            'rejectionComment': None,
            'distributedAt':    None,
        })

        return rds_json_resp({'ok': True, 'fileId': ref.id,
            'message': f'Result file created for {subject} — {class_grade}.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/list', methods=['POST', 'OPTIONS'])
def rds_result_files_list():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        q    = db.collection('rds_result_files').where('schoolId', '==', school_id)
        body = request.get_json(force=True, silent=True) or {}

        if role == 'teacher':
            q = q.where('teacherId', '==', teacher_id)
        if body.get('status'):
            q = q.where('status', '==', body['status'])

        docs  = q.order_by('createdAt', direction='DESCENDING').get()
        files = [_serialize_file(d) for d in docs]

        return rds_json_resp({'ok': True, 'files': files, 'count': len(files)}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/get/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_get(file_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')

        doc = db.collection('rds_result_files').document(file_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)

        file_data = _serialize_file(doc)
        if file_data.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)

        entries_snap = (db.collection('rds_result_entries')
                          .where('resultFileId', '==', file_id).get())
        entries = []
        for e in entries_snap:
            row = e.to_dict(); row['id'] = e.id
            for f in ('createdAt', 'updatedAt'):
                row[f] = ts(row.get(f))
            entries.append(row)

        entries.sort(key=lambda e: e.get('studentName', ''))
        positions = _compute_positions(entries)
        for entry in entries:
            entry['position']    = positions.get(entry['id'], '—')
            entry['positionStr'] = _ordinal(entry['position']) if isinstance(entry['position'], int) else '—'

        return rds_json_resp({'ok': True, 'file': file_data, 'entries': entries}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/populate', methods=['POST', 'OPTIONS'])
def rds_result_files_populate():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body    = request.get_json(force=True, silent=True) or {}
        file_id = (body.get('fileId') or '').strip()
        if not file_id:
            return rds_json_resp({'ok': False, 'error': 'fileId required.'}, 400, request)

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft files can be populated.'}, 400, request)

        class_grade = fd.get('classGrade', '')
        cbt_exam_id = fd.get('cbtExamId', '')

        students_snap = (db.collection('rds_students')
                           .where('schoolId',   '==', school_id)
                           .where('classGrade', '==', class_grade).get())

        existing_snap = (db.collection('rds_result_entries')
                           .where('resultFileId', '==', file_id).get())
        existing_student_ids = {e.to_dict().get('studentId') for e in existing_snap}

        cbt_scores = {}
        if cbt_exam_id:
            subs_snap = (db.collection('submissions')
                           .where('examId',   '==', cbt_exam_id)
                           .where('schoolId', '==', school_id).get())
            for sub in subs_snap:
                sd    = sub.to_dict()
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

            cbt_data            = cbt_scores.get(email, {}) if (source == 'cbt' and email) else {}
            cbt_score           = cbt_data.get('score', None)
            cbt_total           = cbt_data.get('total', None)
            cbt_pct             = cbt_data.get('percentage', None)
            exam_score_from_cbt = round((cbt_pct / 100) * 60, 1) if cbt_pct is not None else None

            entry_ref = db.collection('rds_result_entries').document()
            batch.set(entry_ref, {
                'resultFileId':  file_id,
                'schoolId':      school_id,
                'studentId':     student_id,
                'studentName':   sd.get('name', ''),
                'classGrade':    class_grade,
                'source':        source,
                'cbtScore':      cbt_score,
                'cbtTotal':      cbt_total,
                'cbtPercentage': cbt_pct,
                'testScore':     None,
                'examScore':     exam_score_from_cbt,
                'caScore':       None,
                'totalScore':    None,
                'grade':         None,
                'remark':        None,
                'position':      None,
                'createdAt':     datetime.now(timezone.utc),
                'updatedAt':     datetime.now(timezone.utc),
            })
            created += 1
            count   += 1
            if count == 499:
                batch.commit()
                batch = db.batch()
                count = 0

        if count > 0:
            batch.commit()

        total_now = len(existing_student_ids) + created
        db.collection('rds_result_files').document(file_id).update({
            'totalStudents': total_now,
            'updatedAt':     datetime.now(timezone.utc),
        })

        return rds_json_resp({
            'ok':      True,
            'created': created,
            'skipped': skipped,
            'total':   total_now,
            'message': f'{created} student(s) added to result file.',
        }, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-entries/save', methods=['POST', 'OPTIONS'])
def rds_result_entries_save():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')

        body    = request.get_json(force=True, silent=True) or {}
        file_id = (body.get('fileId') or '').strip()
        entries = body.get('entries', [])

        if not file_id:
            return rds_json_resp({'ok': False, 'error': 'fileId required.'}, 400, request)

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') not in ('draft',):
            return rds_json_resp({'ok': False, 'error': 'Only draft files can be edited.'}, 400, request)

        def to_num(v):
            try:   return float(v) if v is not None and str(v).strip() != '' else None
            except: return None

        batch = db.batch()
        count = 0

        for row in entries:
            entry_id = (row.get('id') or '').strip()
            if not entry_id: continue

            test  = to_num(row.get('testScore'))
            exam  = to_num(row.get('examScore'))
            ca    = to_num(row.get('caScore'))

            total  = None
            grade  = None
            remark = None
            if test is not None and exam is not None and ca is not None:
                total  = round(test + exam + ca, 1)
                grade  = _compute_grade(total, 130)
                remark = _compute_remark(grade)

            ref = db.collection('rds_result_entries').document(entry_id)
            batch.update(ref, {
                'testScore':  test,
                'examScore':  exam,
                'caScore':    ca,
                'totalScore': total,
                'grade':      grade,
                'remark':     remark,
                'updatedAt':  datetime.now(timezone.utc),
            })
            count += 1
            if count == 499:
                batch.commit()
                batch = db.batch()
                count = 0

        if count > 0:
            batch.commit()

        # Recompute positions for all entries in this file
        all_entries_snap = (db.collection('rds_result_entries')
                              .where('resultFileId', '==', file_id).get())
        all_entries = []
        for e in all_entries_snap:
            row = e.to_dict(); row['id'] = e.id
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

        db.collection('rds_result_files').document(file_id).update({
            'updatedAt': datetime.now(timezone.utc)
        })

        return rds_json_resp({'ok': True, 'saved': len(entries),
            'message': 'Scores saved successfully.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/submit/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_submit(file_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft files can be submitted.'}, 400, request)

        entries_snap = (db.collection('rds_result_entries')
                          .where('resultFileId', '==', file_id).get())
        entries = [e.to_dict() for e in entries_snap]

        if not entries:
            return rds_json_resp({'ok': False,
                'error': 'No students in this result file. Please populate it first.'}, 400, request)

        incomplete = [e.get('studentName', '?') for e in entries if e.get('totalScore') is None]
        if incomplete:
            names = ', '.join(incomplete[:5]) + ('...' if len(incomplete) > 5 else '')
            return rds_json_resp({'ok': False,
                'error': f'{len(incomplete)} student(s) have incomplete scores: {names}.'}, 400, request)

        db.collection('rds_result_files').document(file_id).update({
            'status':      'submitted',
            'submittedAt': datetime.now(timezone.utc),
            'updatedAt':   datetime.now(timezone.utc),
        })

        return rds_json_resp({'ok': True, 'message': 'Result file submitted for review.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/delete/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_delete(file_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()
        if fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if fd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft files can be deleted.'}, 400, request)
        if role == 'teacher' and fd.get('teacherId') != uid:
            return rds_json_resp({'ok': False, 'error': 'You can only delete your own result files.'}, 403, request)

        entries_snap = (db.collection('rds_result_entries')
                          .where('resultFileId', '==', file_id).get())
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

        db.collection('rds_result_files').document(file_id).delete()
        return rds_json_resp({'ok': True, 'message': 'Result file deleted.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/cbt-exams', methods=['POST', 'OPTIONS'])
def rds_cbt_exams_for_class():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db        = get_db()
        s         = rds_session(request, db)
        school_id = s.get('schoolId')

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
        return rds_json_resp({'ok': True, 'exams': exams}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Phase 3: Coordinator Approval Workflow
#
# Status flow:  draft → submitted → approved | rejected → distributed
#
# Actors:
#   teacher       → creates draft, populates, saves scores, submits, recalls
#   sub_admin     → same as teacher for their own files
#   school_admin  → coordinator: approves / rejects / requests correction
#   super_admin   → can do everything
#
# Endpoints:
#   POST /rds-result-files/approve/<file_id>   — coordinator approves
#   POST /rds-result-files/reject/<file_id>    — coordinator rejects with comment
#   POST /rds-result-files/recall/<file_id>    — teacher recalls submitted file back to draft
#   POST /rds-result-files/pending             — coordinator lists all submitted files
#   POST /rds-result-files/approval-history    — full audit log per file
# =============================================================================

COORDINATOR_ROLES = ('school_admin', 'super_admin')
TEACHER_ROLES     = ('teacher', 'sub_admin', 'school_admin', 'super_admin')


@rds_bp.route('/rds-result-files/approve/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_approve(file_id):
    """
    Coordinator approves a submitted result file.
    Status: submitted → approved
    Only school_admin and super_admin can approve.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')
        name      = s.get('name', uid)

        if role not in COORDINATOR_ROLES:
            return rds_json_resp({'ok': False,
                'error': 'Only coordinators (school_admin / super_admin) can approve result files.'}, 403, request)

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()

        # super_admin can approve any school; school_admin only their own
        if role == 'school_admin' and fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised for this school.'}, 403, request)

        if fd.get('status') != 'submitted':
            return rds_json_resp({'ok': False,
                'error': f'Only submitted files can be approved. Current status: {fd.get("status")}.'}, 400, request)

        body    = request.get_json(force=True, silent=True) or {}
        comment = (body.get('comment') or '').strip()

        db.collection('rds_result_files').document(file_id).update({
            'status':          'approved',
            'approvedAt':      datetime.now(timezone.utc),
            'approvedBy':      uid,
            'approvedByName':  name,
            'approvalComment': comment,
            'updatedAt':       datetime.now(timezone.utc),
            # Clear any previous rejection data
            'rejectedAt':      None,
            'rejectedBy':      None,
            'rejectedByName':  None,
            'rejectionComment':None,
        })

        # Log approval event
        db.collection('rds_approval_log').add({
            'fileId':    file_id,
            'schoolId':  fd.get('schoolId'),
            'action':    'approved',
            'actorUid':  uid,
            'actorName': name,
            'actorRole': role,
            'comment':   comment,
            'timestamp': datetime.now(timezone.utc),
            'fileMeta': {
                'subject':    fd.get('subject'),
                'classGrade': fd.get('classGrade'),
                'session':    fd.get('session'),
                'term':       fd.get('term'),
                'teacherName':fd.get('teacherName'),
            },
        })

        return rds_json_resp({'ok': True,
            'message': f'Result file approved by {name}.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/reject/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_reject(file_id):
    """
    Coordinator rejects a submitted result file with a required comment.
    Status: submitted → rejected
    Teacher can then recall → fix → resubmit.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')
        name      = s.get('name', uid)

        if role not in COORDINATOR_ROLES:
            return rds_json_resp({'ok': False,
                'error': 'Only coordinators (school_admin / super_admin) can reject result files.'}, 403, request)

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()

        if role == 'school_admin' and fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised for this school.'}, 403, request)

        if fd.get('status') != 'submitted':
            return rds_json_resp({'ok': False,
                'error': f'Only submitted files can be rejected. Current status: {fd.get("status")}.'}, 400, request)

        body    = request.get_json(force=True, silent=True) or {}
        comment = (body.get('comment') or '').strip()

        if not comment:
            return rds_json_resp({'ok': False,
                'error': 'A rejection comment is required so the teacher knows what to fix.'}, 400, request)

        db.collection('rds_result_files').document(file_id).update({
            'status':          'rejected',
            'rejectedAt':      datetime.now(timezone.utc),
            'rejectedBy':      uid,
            'rejectedByName':  name,
            'rejectionComment':comment,
            'updatedAt':       datetime.now(timezone.utc),
        })

        # Log rejection event
        db.collection('rds_approval_log').add({
            'fileId':    file_id,
            'schoolId':  fd.get('schoolId'),
            'action':    'rejected',
            'actorUid':  uid,
            'actorName': name,
            'actorRole': role,
            'comment':   comment,
            'timestamp': datetime.now(timezone.utc),
            'fileMeta': {
                'subject':    fd.get('subject'),
                'classGrade': fd.get('classGrade'),
                'session':    fd.get('session'),
                'term':       fd.get('term'),
                'teacherName':fd.get('teacherName'),
            },
        })

        return rds_json_resp({'ok': True,
            'message': f'Result file rejected. Comment sent to teacher.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/recall/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_recall(file_id):
    """
    Teacher recalls a submitted or rejected file back to draft for editing.
    Cannot recall an approved file.
    Status: submitted | rejected → draft
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')
        name      = s.get('name', uid)

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()

        if fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)

        # Teachers can only recall their own files; admins can recall any
        if role == 'teacher' and fd.get('teacherId') != uid:
            return rds_json_resp({'ok': False,
                'error': 'You can only recall your own result files.'}, 403, request)

        current_status = fd.get('status')
        if current_status == 'approved':
            return rds_json_resp({'ok': False,
                'error': 'Approved files cannot be recalled. Contact a coordinator.'}, 400, request)
        if current_status == 'draft':
            return rds_json_resp({'ok': False,
                'error': 'This file is already in draft.'}, 400, request)
        if current_status == 'distributed':
            return rds_json_resp({'ok': False,
                'error': 'Distributed files cannot be recalled.'}, 400, request)

        db.collection('rds_result_files').document(file_id).update({
            'status':      'draft',
            'submittedAt': None,
            'updatedAt':   datetime.now(timezone.utc),
        })

        # Log recall event
        db.collection('rds_approval_log').add({
            'fileId':    file_id,
            'schoolId':  fd.get('schoolId'),
            'action':    'recalled',
            'actorUid':  uid,
            'actorName': name,
            'actorRole': role,
            'comment':   f'Recalled from {current_status} back to draft.',
            'timestamp': datetime.now(timezone.utc),
            'fileMeta': {
                'subject':    fd.get('subject'),
                'classGrade': fd.get('classGrade'),
                'session':    fd.get('session'),
                'term':       fd.get('term'),
                'teacherName':fd.get('teacherName'),
            },
        })

        return rds_json_resp({'ok': True,
            'message': 'Result file recalled to draft. You can now edit and resubmit.'}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/pending', methods=['POST', 'OPTIONS'])
def rds_result_files_pending():
    """
    Coordinator view: list all submitted files awaiting approval.
    Can also filter by status (submitted | approved | rejected | distributed).
    Only school_admin and super_admin can call this.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')

        if role not in COORDINATOR_ROLES:
            return rds_json_resp({'ok': False,
                'error': 'Only coordinators can view the approval queue.'}, 403, request)

        body   = request.get_json(force=True, silent=True) or {}
        status = (body.get('status') or 'submitted').strip()

        q = db.collection('rds_result_files').where('schoolId', '==', school_id)
        if status != 'all':
            q = q.where('status', '==', status)

        docs  = q.order_by('submittedAt', direction='DESCENDING').get()
        files = [_serialize_file(d) for d in docs]

        # Enrich with entry count
        for f in files:
            count_snap = (db.collection('rds_result_entries')
                            .where('resultFileId', '==', f['id']).get())
            f['entryCount'] = len(count_snap)

        return rds_json_resp({
            'ok':    True,
            'files': files,
            'count': len(files),
            'status': status,
        }, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-result-files/approval-history/<file_id>', methods=['POST', 'OPTIONS'])
def rds_result_files_approval_history(file_id):
    """
    Return full approval audit log for a result file.
    Available to all RDS roles — teachers can see the history of their own files.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        role      = s.get('role')
        uid       = s.get('uid')

        file_doc = db.collection('rds_result_files').document(file_id).get()
        if not file_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Result file not found.'}, 404, request)
        fd = file_doc.to_dict()

        if fd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)

        # Teachers can only see history of their own files
        if role == 'teacher' and fd.get('teacherId') != uid:
            return rds_json_resp({'ok': False,
                'error': 'You can only view the history of your own result files.'}, 403, request)

        log_snap = (db.collection('rds_approval_log')
                      .where('fileId', '==', file_id)
                      .order_by('timestamp', direction='DESCENDING')
                      .get())

        log = []
        for doc in log_snap:
            row = doc.to_dict()
            row['id'] = doc.id
            row['timestamp'] = ts(row.get('timestamp'))
            log.append(row)

        return rds_json_resp({
            'ok':     True,
            'fileId': file_id,
            'file':   _serialize_file(file_doc),
            'log':    log,
        }, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Assignments: Teacher ↔ Coordinator mapping
# =============================================================================

@rds_bp.route('/rds-assignments/assign', methods=['POST', 'OPTIONS'])
def rds_assignments_assign():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        if role not in ('school_admin', 'super_admin'):
            return rds_json_resp({'ok': False, 'error': 'Only school admins can manage assignments.'}, 403, request)
        body            = request.get_json(force=True, silent=True) or {}
        teacher_uid     = (body.get('teacherUid')     or '').strip()
        coordinator_uid = (body.get('coordinatorUid') or '').strip()
        if not teacher_uid or not coordinator_uid:
            return rds_json_resp({'ok': False, 'error': 'teacherUid and coordinatorUid are required.'}, 400, request)
        # Remove any existing assignment for this teacher
        existing_snap = (db.collection('rds_assignments')
                           .where('schoolId',   '==', school_id)
                           .where('teacherUid', '==', teacher_uid).get())
        batch = db.batch()
        for doc in existing_snap:
            batch.delete(doc.reference)
        batch.commit()
        # Get names
        teacher_doc  = db.collection('users').document(teacher_uid).get()
        coord_doc    = db.collection('users').document(coordinator_uid).get()
        teacher_name = teacher_doc.to_dict().get('name', teacher_uid) if teacher_doc.exists else teacher_uid
        coord_name   = coord_doc.to_dict().get('name', coordinator_uid) if coord_doc.exists else coordinator_uid
        ref, _ = db.collection('rds_assignments').add({
            'schoolId':        school_id,
            'teacherUid':      teacher_uid,
            'teacherName':     teacher_name,
            'coordinatorUid':  coordinator_uid,
            'coordinatorName': coord_name,
            'assignedBy':      s.get('uid'),
            'assignedByName':  s.get('name', ''),
            'assignedAt':      datetime.now(timezone.utc),
            'updatedAt':       datetime.now(timezone.utc),
        })
        return rds_json_resp({'ok': True, 'assignmentId': ref.id,
            'message': f'{teacher_name} assigned to {coord_name}.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-assignments/unassign', methods=['POST', 'OPTIONS'])
def rds_assignments_unassign():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        if role not in ('school_admin', 'super_admin'):
            return rds_json_resp({'ok': False, 'error': 'Only school admins can manage assignments.'}, 403, request)
        body        = request.get_json(force=True, silent=True) or {}
        teacher_uid = (body.get('teacherUid') or '').strip()
        if not teacher_uid:
            return rds_json_resp({'ok': False, 'error': 'teacherUid is required.'}, 400, request)
        snap = (db.collection('rds_assignments')
                  .where('schoolId',   '==', school_id)
                  .where('teacherUid', '==', teacher_uid).get())
        if not snap:
            return rds_json_resp({'ok': False, 'error': 'No assignment found for this teacher.'}, 404, request)
        batch = db.batch()
        for doc in snap:
            batch.delete(doc.reference)
        batch.commit()
        return rds_json_resp({'ok': True, 'message': 'Teacher unassigned successfully.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-assignments/list', methods=['POST', 'OPTIONS'])
def rds_assignments_list():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        if role not in ('school_admin', 'super_admin'):
            return rds_json_resp({'ok': False, 'error': 'Only school admins can view assignments.'}, 403, request)
        body         = request.get_json(force=True, silent=True) or {}
        filter_coord = (body.get('coordinatorUid') or '').strip()
        q = db.collection('rds_assignments').where('schoolId', '==', school_id)
        if filter_coord:
            q = q.where('coordinatorUid', '==', filter_coord)
        assign_snap = q.order_by('assignedAt', direction='DESCENDING').get()
        assignments = []
        assigned_teacher_uids = set()
        for doc in assign_snap:
            row = doc.to_dict(); row['id'] = doc.id
            row['assignedAt'] = ts(row.get('assignedAt'))
            row['updatedAt']  = ts(row.get('updatedAt'))
            assignments.append(row)
            assigned_teacher_uids.add(row.get('teacherUid', ''))
        # All teachers in school
        teachers_snap = (db.collection('users')
                           .where('schoolId', '==', school_id)
                           .where('role', '==', 'teacher').get())
        all_teachers = []
        for doc in teachers_snap:
            td = doc.to_dict()
            all_teachers.append({
                'uid':        doc.id,
                'name':       td.get('name', doc.id),
                'email':      doc.id,
                'isAssigned': doc.id in assigned_teacher_uids,
            })
        all_teachers.sort(key=lambda t: t['name'])
        # All coordinators (sub_admins) in school
        coords_snap = (db.collection('users')
                         .where('schoolId', '==', school_id)
                         .where('role', '==', 'sub_admin').get())
        coordinators = []
        for doc in coords_snap:
            cd = doc.to_dict()
            coordinators.append({'uid': doc.id, 'name': cd.get('name', doc.id), 'email': doc.id})
        coordinators.sort(key=lambda c: c['name'])
        return rds_json_resp({
            'ok': True, 'assignments': assignments,
            'teachers': all_teachers, 'coordinators': coordinators,
            'count': len(assignments),
        }, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-assignments/my-coordinator', methods=['POST', 'OPTIONS'])
def rds_assignments_my_coordinator():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id   = s.get('schoolId')
        teacher_uid = s.get('uid')
        snap = (db.collection('rds_assignments')
                  .where('schoolId',   '==', school_id)
                  .where('teacherUid', '==', teacher_uid)
                  .limit(1).get())
        if not snap:
            return rds_json_resp({'ok': True, 'coordinator': None,
                'message': 'You have not been assigned to a coordinator yet.'}, req=request)
        row = snap[0].to_dict()
        return rds_json_resp({'ok': True, 'coordinator': {
            'uid': row.get('coordinatorUid'), 'name': row.get('coordinatorName'),
        }}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Subjects Management
# =============================================================================

@rds_bp.route('/rds-subjects/upload', methods=['POST', 'OPTIONS'])
def rds_subjects_upload():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        if s.get('role') != 'super_admin':
            return rds_json_resp({'ok': False, 'error': 'Only super admins can upload the global subjects list.'}, 403, request)
        body     = request.get_json(force=True, silent=True) or {}
        subjects = body.get('subjects', [])
        if not subjects or not isinstance(subjects, list):
            return rds_json_resp({'ok': False, 'error': 'subjects array is required.'}, 400, request)
        # Delete existing global subjects
        existing  = db.collection('rds_global_subjects').get()
        del_batch = db.batch(); del_count = 0
        for doc in existing:
            del_batch.delete(doc.reference); del_count += 1
            if del_count == 499:
                del_batch.commit(); del_batch = db.batch(); del_count = 0
        if del_count > 0:
            del_batch.commit()
        # Insert new subjects
        batch = db.batch(); count = 0; created = 0
        for subj in subjects:
            name = (subj.get('name') or '').strip()
            if not name: continue
            ref = db.collection('rds_global_subjects').document()
            batch.set(ref, {
                'name':      name,
                'code':      (subj.get('code') or '').strip().upper(),
                'category':  (subj.get('category') or 'General').strip(),
                'createdAt': datetime.now(timezone.utc),
                'createdBy': s.get('uid'),
            })
            created += 1; count += 1
            if count == 499:
                batch.commit(); batch = db.batch(); count = 0
        if count > 0:
            batch.commit()
        return rds_json_resp({'ok': True, 'created': created,
            'message': f'{created} subjects uploaded to global list.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-subjects/list-global', methods=['POST', 'OPTIONS'])
def rds_subjects_list_global():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        rds_session(request, db)
        snap = db.collection('rds_global_subjects').order_by('name').get()
        subjects = []
        for doc in snap:
            row = doc.to_dict(); row['id'] = doc.id; subjects.append(row)
        return rds_json_resp({'ok': True, 'subjects': subjects, 'count': len(subjects)}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-subjects/school-add', methods=['POST', 'OPTIONS'])
def rds_subjects_school_add():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        if role not in ('school_admin', 'super_admin'):
            return rds_json_resp({'ok': False, 'error': 'Only school admins can add subjects.'}, 403, request)
        body = request.get_json(force=True, silent=True) or {}
        name = (body.get('name') or '').strip()
        if not name:
            return rds_json_resp({'ok': False, 'error': 'Subject name is required.'}, 400, request)
        dup = (db.collection('rds_school_subjects')
                 .where('schoolId', '==', school_id)
                 .where('name', '==', name).limit(1).get())
        if dup:
            return rds_json_resp({'ok': False, 'error': f'Subject "{name}" already exists for this school.'}, 409, request)
        ref, _ = db.collection('rds_school_subjects').add({
            'schoolId':  school_id,
            'name':      name,
            'code':      (body.get('code') or '').strip().upper(),
            'category':  (body.get('category') or 'General').strip(),
            'source':    'manual',
            'createdAt': datetime.now(timezone.utc),
            'createdBy': s.get('uid'),
        })
        return rds_json_resp({'ok': True, 'id': ref.id,
            'message': f'Subject "{name}" added to school list.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-subjects/school-list', methods=['POST', 'OPTIONS'])
def rds_subjects_school_list():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        snap = (db.collection('rds_school_subjects')
                  .where('schoolId', '==', school_id).order_by('name').get())
        subjects = []
        for doc in snap:
            row = doc.to_dict(); row['id'] = doc.id; subjects.append(row)
        return rds_json_resp({'ok': True, 'subjects': subjects, 'count': len(subjects)}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-subjects/school-delete/<subject_id>', methods=['POST', 'OPTIONS'])
def rds_subjects_school_delete(subject_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        if role not in ('school_admin', 'super_admin'):
            return rds_json_resp({'ok': False, 'error': 'Only school admins can delete subjects.'}, 403, request)
        doc_ref = db.collection('rds_school_subjects').document(subject_id)
        doc     = doc_ref.get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Subject not found.'}, 404, request)
        if doc.to_dict().get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        doc_ref.delete()
        return rds_json_resp({'ok': True, 'message': 'Subject deleted.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-subjects/all', methods=['POST', 'OPTIONS'])
def rds_subjects_all():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        global_snap = db.collection('rds_global_subjects').order_by('name').get()
        school_snap = (db.collection('rds_school_subjects')
                         .where('schoolId', '==', school_id).order_by('name').get())
        seen = set(); merged = []
        for doc in global_snap:
            row = doc.to_dict(); row['id'] = doc.id; row['source'] = 'global'
            key = row.get('name', '').lower()
            if key not in seen:
                seen.add(key); merged.append(row)
        for doc in school_snap:
            row = doc.to_dict(); row['id'] = doc.id; row['source'] = 'school'
            key = row.get('name', '').lower()
            if key not in seen:
                seen.add(key); merged.append(row)
        merged.sort(key=lambda x: x.get('name', '').lower())
        return rds_json_resp({'ok': True, 'subjects': merged, 'count': len(merged)}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Grade Settings
# =============================================================================

@rds_bp.route('/rds-grade-settings/save', methods=['POST', 'OPTIONS'])
def rds_grade_settings_save():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        if role not in ('school_admin', 'super_admin'):
            return rds_json_resp({'ok': False, 'error': 'Only school admins can configure grade settings.'}, 403, request)
        body   = request.get_json(force=True, silent=True) or {}
        grades = body.get('grades', [])
        if not grades or not isinstance(grades, list):
            return rds_json_resp({'ok': False, 'error': 'grades array is required.'}, 400, request)
        db.collection('rds_grade_settings').document(school_id).set({
            'schoolId':  school_id,
            'grades':    grades,
            'updatedAt': datetime.now(timezone.utc),
            'updatedBy': s.get('uid'),
        })
        return rds_json_resp({'ok': True, 'message': 'Grade settings saved.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-grade-settings/get', methods=['POST', 'OPTIONS'])
def rds_grade_settings_get():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        doc = db.collection('rds_grade_settings').document(school_id).get()
        if not doc.exists:
            default_grades = [
                {'label': 'A1', 'minScore': 75, 'maxScore': 100, 'remark': 'Excellent'},
                {'label': 'B2', 'minScore': 70, 'maxScore': 74,  'remark': 'Very Good'},
                {'label': 'B3', 'minScore': 65, 'maxScore': 69,  'remark': 'Good'},
                {'label': 'C4', 'minScore': 60, 'maxScore': 64,  'remark': 'Credit'},
                {'label': 'C5', 'minScore': 55, 'maxScore': 59,  'remark': 'Credit'},
                {'label': 'C6', 'minScore': 50, 'maxScore': 54,  'remark': 'Credit'},
                {'label': 'D7', 'minScore': 45, 'maxScore': 49,  'remark': 'Pass'},
                {'label': 'E8', 'minScore': 40, 'maxScore': 44,  'remark': 'Pass'},
                {'label': 'F9', 'minScore': 0,  'maxScore': 39,  'remark': 'Fail'},
            ]
            return rds_json_resp({'ok': True, 'grades': default_grades, 'isDefault': True}, req=request)
        data = doc.to_dict()
        return rds_json_resp({'ok': True, 'grades': data.get('grades', []), 'isDefault': False}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Broadsheet System
# =============================================================================

def _grade_from_settings(score, grade_settings):
    if grade_settings:
        for g in sorted(grade_settings, key=lambda x: x.get('minScore', 0), reverse=True):
            if score >= g.get('minScore', 0):
                return g.get('label', 'F9'), g.get('remark', '—')
    return _compute_grade(score, 100), _compute_remark(_compute_grade(score, 100))


def _serialize_broadsheet(doc):
    row = doc.to_dict(); row['id'] = doc.id
    for f in ('createdAt', 'updatedAt', 'submittedAt', 'approvedAt', 'rejectedAt', 'distributedAt'):
        row[f] = ts(row.get(f))
    return row


@rds_bp.route('/rds-broadsheet/create', methods=['POST', 'OPTIONS'])
def rds_broadsheet_create():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id    = s.get('schoolId')
        teacher_id   = s.get('uid')
        teacher_name = s.get('name', '')
        body        = request.get_json(force=True, silent=True) or {}
        session_yr  = (body.get('session')    or '').strip()
        term        = (body.get('term')        or '').strip()
        class_grade = (body.get('classGrade') or '').strip()
        subjects    = body.get('subjects', [])
        if not session_yr:  return rds_json_resp({'ok': False, 'error': 'Academic session is required.'}, 400, request)
        if not term:        return rds_json_resp({'ok': False, 'error': 'Term is required.'}, 400, request)
        if not class_grade: return rds_json_resp({'ok': False, 'error': 'Class/Grade is required.'}, 400, request)
        if not subjects:    return rds_json_resp({'ok': False, 'error': 'At least one subject is required.'}, 400, request)
        dup = (db.collection('rds_broadsheets')
                 .where('schoolId',   '==', school_id)
                 .where('teacherId',  '==', teacher_id)
                 .where('session',    '==', session_yr)
                 .where('term',       '==', term)
                 .where('classGrade', '==', class_grade)
                 .limit(1).get())
        if dup:
            return rds_json_resp({'ok': False,
                'error': f'A broadsheet for {class_grade} — {term} {session_yr} already exists.'}, 409, request)
        ref, _ = db.collection('rds_broadsheets').add({
            'schoolId':        school_id,
            'teacherId':       teacher_id,
            'teacherName':     teacher_name,
            'session':         session_yr,
            'term':            term,
            'classGrade':      class_grade,
            'subjects':        [sub.strip() for sub in subjects if sub.strip()],
            'status':          'draft',
            'totalStudents':   0,
            'createdAt':       datetime.now(timezone.utc),
            'updatedAt':       datetime.now(timezone.utc),
            'submittedAt':     None,
            'approvedAt':      None,
            'approvedBy':      None,
            'approvedByName':  None,
            'rejectedAt':      None,
            'rejectedBy':      None,
            'rejectedByName':  None,
            'rejectionComment':None,
        })
        return rds_json_resp({'ok': True, 'broadsheetId': ref.id,
            'message': f'Broadsheet created for {class_grade} — {term} {session_yr}.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/list', methods=['POST', 'OPTIONS'])
def rds_broadsheet_list():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')
        q    = db.collection('rds_broadsheets').where('schoolId', '==', school_id)
        body = request.get_json(force=True, silent=True) or {}
        if role == 'teacher':
            q = q.where('teacherId', '==', teacher_id)
        if body.get('status'):
            q = q.where('status', '==', body['status'])
        if body.get('classGrade'):
            q = q.where('classGrade', '==', body['classGrade'])
        docs   = q.order_by('createdAt', direction='DESCENDING').get()
        sheets = [_serialize_broadsheet(d) for d in docs]
        return rds_json_resp({'ok': True, 'broadsheets': sheets, 'count': len(sheets)}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/get/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_get(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        sheet_data = _serialize_broadsheet(doc)
        if sheet_data.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        rows_snap = (db.collection('rds_broadsheet_rows')
                       .where('broadsheetId', '==', sheet_id).get())
        rows = []
        for r in rows_snap:
            row = r.to_dict(); row['id'] = r.id
            for f in ('createdAt', 'updatedAt'):
                row[f] = ts(row.get(f))
            rows.append(row)
        rows.sort(key=lambda r: r.get('studentName', ''))
        for row in rows:
            scores = row.get('subjectScores', {})
            totals = [v.get('total') for v in scores.values() if v.get('total') is not None]
            row['overallTotal'] = round(sum(totals), 1) if totals else None
            row['subjectCount'] = len(totals)
        scored_rows = [r for r in rows if r.get('overallTotal') is not None]
        scored_rows.sort(key=lambda r: r['overallTotal'], reverse=True)
        pos = 1
        for i, r in enumerate(scored_rows):
            if i > 0 and r['overallTotal'] < scored_rows[i-1]['overallTotal']:
                pos = i + 1
            r['overallPosition']    = pos
            r['overallPositionStr'] = _ordinal(pos)
        return rds_json_resp({'ok': True, 'broadsheet': sheet_data, 'rows': rows}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/populate/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_populate(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if bd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft broadsheets can be populated.'}, 400, request)
        class_grade = bd.get('classGrade', '')
        subjects    = bd.get('subjects', [])
        students_snap = (db.collection('rds_students')
                           .where('schoolId',   '==', school_id)
                           .where('classGrade', '==', class_grade).get())
        existing_snap = (db.collection('rds_broadsheet_rows')
                           .where('broadsheetId', '==', sheet_id).get())
        existing_ids  = {r.to_dict().get('studentId') for r in existing_snap}
        empty_scores  = {subj: {'ca': None, 'exam': None, 'total': None, 'grade': None, 'remark': None}
                         for subj in subjects}
        batch = db.batch(); count = 0; created = 0; skipped = 0
        for student_doc in students_snap:
            student_id = student_doc.id
            sd         = student_doc.to_dict()
            if student_id in existing_ids:
                skipped += 1; continue
            ref = db.collection('rds_broadsheet_rows').document()
            batch.set(ref, {
                'broadsheetId':    sheet_id,
                'schoolId':        school_id,
                'studentId':       student_id,
                'studentName':     sd.get('name', ''),
                'classGrade':      class_grade,
                'admissionNumber': sd.get('admissionNumber', ''),
                'subjectScores':   dict(empty_scores),
                'createdAt':       datetime.now(timezone.utc),
                'updatedAt':       datetime.now(timezone.utc),
                'createdBy':       teacher_id,
            })
            created += 1; count += 1
            if count == 499:
                batch.commit(); batch = db.batch(); count = 0
        if count > 0:
            batch.commit()
        total_now = len(existing_ids) + created
        db.collection('rds_broadsheets').document(sheet_id).update({
            'totalStudents': total_now, 'updatedAt': datetime.now(timezone.utc),
        })
        return rds_json_resp({'ok': True, 'created': created, 'skipped': skipped,
            'total': total_now, 'message': f'{created} student(s) added to broadsheet.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/sync-cbt/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_sync_cbt(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        body        = request.get_json(force=True, silent=True) or {}
        subject     = (body.get('subject')    or '').strip()
        cbt_exam_id = (body.get('cbtExamId') or '').strip()
        exam_weight = float(body.get('examWeight', 60))
        if not subject or not cbt_exam_id:
            return rds_json_resp({'ok': False, 'error': 'subject and cbtExamId are required.'}, 400, request)
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if bd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft broadsheets can be synced.'}, 400, request)
        if subject not in bd.get('subjects', []):
            return rds_json_resp({'ok': False, 'error': f'Subject "{subject}" is not in this broadsheet.'}, 400, request)
        subs_snap = (db.collection('submissions')
                       .where('examId',   '==', cbt_exam_id)
                       .where('schoolId', '==', school_id).get())
        cbt_by_email = {}
        for sub in subs_snap:
            sd    = sub.to_dict()
            email = sd.get('studentEmail', '').lower()
            if email:
                pct = sd.get('percentage', 0)
                cbt_by_email[email] = round((pct / 100) * exam_weight, 1)
        rows_snap = (db.collection('rds_broadsheet_rows')
                       .where('broadsheetId', '==', sheet_id).get())
        student_ids = [r.to_dict().get('studentId') for r in rows_snap]
        student_emails = {}
        for sid in student_ids:
            if sid:
                sdoc = db.collection('rds_students').document(sid).get()
                if sdoc.exists:
                    student_emails[sid] = sdoc.to_dict().get('email', '').lower()
        batch = db.batch(); count = 0; synced = 0; no_match = 0
        for row_doc in rows_snap:
            row_data   = row_doc.to_dict()
            student_id = row_data.get('studentId', '')
            email      = student_emails.get(student_id, '')
            exam_score = cbt_by_email.get(email)
            if exam_score is None:
                no_match += 1; continue
            scores      = row_data.get('subjectScores', {})
            subj_scores = scores.get(subject, {})
            subj_scores['exam']       = exam_score
            subj_scores['cbtSynced']  = True
            subj_scores['cbtExamId']  = cbt_exam_id
            ca = subj_scores.get('ca')
            if ca is not None:
                total = round(ca + exam_score, 1)
                pct   = (total / (40 + exam_weight)) * 100 if (40 + exam_weight) > 0 else 0
                grade, remark = _grade_from_settings(pct, None)
                subj_scores['total']  = total
                subj_scores['grade']  = grade
                subj_scores['remark'] = remark
            scores[subject] = subj_scores
            batch.update(row_doc.reference, {'subjectScores': scores, 'updatedAt': datetime.now(timezone.utc)})
            synced += 1; count += 1
            if count == 499:
                batch.commit(); batch = db.batch(); count = 0
        if count > 0:
            batch.commit()
        return rds_json_resp({'ok': True, 'synced': synced, 'noMatch': no_match,
            'message': f'{synced} student(s) synced from CBT for {subject}.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/save-scores/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_save_scores(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        body  = request.get_json(force=True, silent=True) or {}
        rows  = body.get('rows', [])
        force = bool(body.get('force', False))
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if bd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft broadsheets can be edited.'}, 400, request)
        gs_doc = db.collection('rds_grade_settings').document(school_id).get()
        grade_settings = gs_doc.to_dict().get('grades', []) if gs_doc.exists else []
        def to_num(v):
            try:   return float(v) if v is not None and str(v).strip() != '' else None
            except: return None
        batch = db.batch(); count = 0; saved = 0
        for row in rows:
            row_id  = (row.get('rowId') or '').strip()
            subject = (row.get('subject') or '').strip()
            if not row_id or not subject: continue
            row_ref = db.collection('rds_broadsheet_rows').document(row_id)
            row_doc = row_ref.get()
            if not row_doc.exists: continue
            row_data    = row_doc.to_dict()
            scores      = row_data.get('subjectScores', {})
            subj_scores = scores.get(subject, {})
            ca   = to_num(row.get('ca'))
            exam = to_num(row.get('exam'))
            if ca is not None:
                subj_scores['ca'] = ca
            if exam is not None and (not subj_scores.get('cbtSynced') or force):
                subj_scores['exam'] = exam
            ca_val   = subj_scores.get('ca')
            exam_val = subj_scores.get('exam')
            if ca_val is not None and exam_val is not None:
                total     = round(ca_val + exam_val, 1)
                max_score = 40 + float(body.get('examWeight', 60))
                pct       = (total / max_score) * 100 if max_score > 0 else 0
                grade, remark = _grade_from_settings(pct, grade_settings)
                subj_scores['total']  = total
                subj_scores['grade']  = grade
                subj_scores['remark'] = remark
            else:
                subj_scores['total']  = None
                subj_scores['grade']  = None
                subj_scores['remark'] = None
            scores[subject] = subj_scores
            batch.update(row_ref, {'subjectScores': scores, 'updatedAt': datetime.now(timezone.utc)})
            saved += 1; count += 1
            if count == 499:
                batch.commit(); batch = db.batch(); count = 0
        if count > 0:
            batch.commit()
        db.collection('rds_broadsheets').document(sheet_id).update({'updatedAt': datetime.now(timezone.utc)})
        return rds_json_resp({'ok': True, 'saved': saved, 'message': f'{saved} row(s) updated.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/submit/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_submit(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if bd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft broadsheets can be submitted.'}, 400, request)
        rows_snap = (db.collection('rds_broadsheet_rows')
                       .where('broadsheetId', '==', sheet_id).get())
        if not rows_snap:
            return rds_json_resp({'ok': False,
                'error': 'No students in this broadsheet. Please populate it first.'}, 400, request)
        db.collection('rds_broadsheets').document(sheet_id).update({
            'status': 'submitted', 'submittedAt': datetime.now(timezone.utc),
            'updatedAt': datetime.now(timezone.utc),
        })
        return rds_json_resp({'ok': True, 'message': 'Broadsheet submitted for review.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/approve/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_approve(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')
        name      = s.get('name', uid)
        if role not in COORDINATOR_ROLES:
            return rds_json_resp({'ok': False, 'error': 'Only coordinators can approve broadsheets.'}, 403, request)
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if role == 'school_admin' and bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised for this school.'}, 403, request)
        if bd.get('status') != 'submitted':
            return rds_json_resp({'ok': False,
                'error': f'Only submitted broadsheets can be approved. Current: {bd.get("status")}.'}, 400, request)
        body    = request.get_json(force=True, silent=True) or {}
        comment = (body.get('comment') or '').strip()
        db.collection('rds_broadsheets').document(sheet_id).update({
            'status': 'approved', 'approvedAt': datetime.now(timezone.utc),
            'approvedBy': uid, 'approvedByName': name, 'approvalComment': comment,
            'updatedAt': datetime.now(timezone.utc),
            'rejectedAt': None, 'rejectedBy': None, 'rejectedByName': None, 'rejectionComment': None,
        })
        db.collection('rds_broadsheet_log').add({
            'sheetId': sheet_id, 'schoolId': bd.get('schoolId'), 'action': 'approved',
            'actorUid': uid, 'actorName': name, 'actorRole': role,
            'comment': comment, 'timestamp': datetime.now(timezone.utc),
        })
        return rds_json_resp({'ok': True, 'message': f'Broadsheet approved by {name}.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/reject/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_reject(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')
        name      = s.get('name', uid)
        if role not in COORDINATOR_ROLES:
            return rds_json_resp({'ok': False, 'error': 'Only coordinators can reject broadsheets.'}, 403, request)
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if role == 'school_admin' and bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised for this school.'}, 403, request)
        if bd.get('status') != 'submitted':
            return rds_json_resp({'ok': False,
                'error': f'Only submitted broadsheets can be rejected. Current: {bd.get("status")}.'}, 400, request)
        body    = request.get_json(force=True, silent=True) or {}
        comment = (body.get('comment') or '').strip()
        if not comment:
            return rds_json_resp({'ok': False,
                'error': 'A rejection comment is required so the teacher knows what to fix.'}, 400, request)
        db.collection('rds_broadsheets').document(sheet_id).update({
            'status': 'rejected', 'rejectedAt': datetime.now(timezone.utc),
            'rejectedBy': uid, 'rejectedByName': name, 'rejectionComment': comment,
            'updatedAt': datetime.now(timezone.utc),
        })
        db.collection('rds_broadsheet_log').add({
            'sheetId': sheet_id, 'schoolId': bd.get('schoolId'), 'action': 'rejected',
            'actorUid': uid, 'actorName': name, 'actorRole': role,
            'comment': comment, 'timestamp': datetime.now(timezone.utc),
        })
        return rds_json_resp({'ok': True, 'message': 'Broadsheet rejected. Comment sent to teacher.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/recall/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_recall(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')
        name      = s.get('name', uid)
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if role == 'teacher' and bd.get('teacherId') != uid:
            return rds_json_resp({'ok': False, 'error': 'You can only recall your own broadsheets.'}, 403, request)
        current_status = bd.get('status')
        if current_status == 'approved':
            return rds_json_resp({'ok': False, 'error': 'Approved broadsheets cannot be recalled. Contact a coordinator.'}, 400, request)
        if current_status == 'draft':
            return rds_json_resp({'ok': False, 'error': 'This broadsheet is already in draft.'}, 400, request)
        if current_status == 'distributed':
            return rds_json_resp({'ok': False, 'error': 'Distributed broadsheets cannot be recalled.'}, 400, request)
        db.collection('rds_broadsheets').document(sheet_id).update({
            'status': 'draft', 'submittedAt': None, 'updatedAt': datetime.now(timezone.utc),
        })
        db.collection('rds_broadsheet_log').add({
            'sheetId': sheet_id, 'schoolId': bd.get('schoolId'), 'action': 'recalled',
            'actorUid': uid, 'actorName': name, 'actorRole': role,
            'comment': f'Recalled from {current_status} back to draft.',
            'timestamp': datetime.now(timezone.utc),
        })
        return rds_json_resp({'ok': True,
            'message': 'Broadsheet recalled to draft. You can now edit and resubmit.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/delete/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_delete(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        uid       = s.get('uid')
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if bd.get('status') != 'draft':
            return rds_json_resp({'ok': False, 'error': 'Only draft broadsheets can be deleted.'}, 400, request)
        if role == 'teacher' and bd.get('teacherId') != uid:
            return rds_json_resp({'ok': False, 'error': 'You can only delete your own broadsheets.'}, 403, request)
        rows_snap = (db.collection('rds_broadsheet_rows')
                       .where('broadsheetId', '==', sheet_id).get())
        batch = db.batch(); count = 0
        for r in rows_snap:
            batch.delete(r.reference); count += 1
            if count == 499:
                batch.commit(); batch = db.batch(); count = 0
        if count > 0:
            batch.commit()
        db.collection('rds_broadsheets').document(sheet_id).delete()
        return rds_json_resp({'ok': True, 'message': 'Broadsheet deleted.'}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/pending', methods=['POST', 'OPTIONS'])
def rds_broadsheet_pending():
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        if role not in COORDINATOR_ROLES:
            return rds_json_resp({'ok': False, 'error': 'Only coordinators can view the approval queue.'}, 403, request)
        body   = request.get_json(force=True, silent=True) or {}
        status = (body.get('status') or 'submitted').strip()
        q = db.collection('rds_broadsheets').where('schoolId', '==', school_id)
        if status != 'all':
            q = q.where('status', '==', status)
        docs   = q.order_by('submittedAt', direction='DESCENDING').get()
        sheets = [_serialize_broadsheet(d) for d in docs]
        return rds_json_resp({'ok': True, 'broadsheets': sheets, 'count': len(sheets), 'status': status}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-broadsheet/history/<sheet_id>', methods=['POST', 'OPTIONS'])
def rds_broadsheet_history(sheet_id):
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id = s.get('schoolId')
        role      = s.get('role')
        uid       = s.get('uid')
        doc = db.collection('rds_broadsheets').document(sheet_id).get()
        if not doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Broadsheet not found.'}, 404, request)
        bd = doc.to_dict()
        if bd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)
        if role == 'teacher' and bd.get('teacherId') != uid:
            return rds_json_resp({'ok': False, 'error': 'You can only view history of your own broadsheets.'}, 403, request)
        log_snap = (db.collection('rds_broadsheet_log')
                      .where('sheetId', '==', sheet_id)
                      .order_by('timestamp', direction='DESCENDING').get())
        log = []
        for ldoc in log_snap:
            row = ldoc.to_dict(); row['id'] = ldoc.id
            row['timestamp'] = ts(row.get('timestamp'))
            log.append(row)
        return rds_json_resp({'ok': True, 'sheetId': sheet_id,
            'broadsheet': _serialize_broadsheet(doc), 'log': log}, req=request)
    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# RDS — Search Results (cross-student, cross-subject lookup)
# =============================================================================

@rds_bp.route('/rds-search/results', methods=['POST', 'OPTIONS'])
def rds_search_results():
    """
    Search result entries across result files and broadsheets.
    Supports filtering by: studentName, classGrade, session, term, subject, status.
    Returns a unified list of result records.
    Body: {
        query: str (optional — partial name match),
        classGrade: str,
        session: str,
        term: str,
        subject: str,
        status: str,
        source: "result_files" | "broadsheets" | "all"  (default: "all")
    }
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        role       = s.get('role')
        school_id  = s.get('schoolId')
        teacher_id = s.get('uid')

        body        = request.get_json(force=True, silent=True) or {}
        query       = (body.get('query')      or '').strip().lower()
        class_grade = (body.get('classGrade') or '').strip()
        session_yr  = (body.get('session')    or '').strip()
        term        = (body.get('term')        or '').strip()
        subject     = (body.get('subject')    or '').strip()
        status      = (body.get('status')     or '').strip()
        source      = (body.get('source')     or 'all').strip()

        results = []

        # ── Search result files ──────────────────────────────────────────────
        if source in ('result_files', 'all'):
            q = db.collection('rds_result_files').where('schoolId', '==', school_id)
            if role == 'teacher':
                q = q.where('teacherId', '==', teacher_id)
            if class_grade:
                q = q.where('classGrade', '==', class_grade)
            if session_yr:
                q = q.where('session', '==', session_yr)
            if term:
                q = q.where('term', '==', term)
            if subject:
                q = q.where('subject', '==', subject)
            if status:
                q = q.where('status', '==', status)

            files_snap = q.get()
            for file_doc in files_snap:
                fd      = file_doc.to_dict()
                file_id = file_doc.id

                entries_snap = (db.collection('rds_result_entries')
                                  .where('resultFileId', '==', file_id).get())
                for e in entries_snap:
                    ed = e.to_dict()
                    student_name = ed.get('studentName', '')
                    if query and query not in student_name.lower():
                        continue
                    results.append({
                        'source':       'result_file',
                        'fileId':       file_id,
                        'entryId':      e.id,
                        'studentName':  student_name,
                        'classGrade':   ed.get('classGrade', fd.get('classGrade', '')),
                        'subject':      fd.get('subject', ''),
                        'session':      fd.get('session', ''),
                        'term':         fd.get('term', ''),
                        'teacherName':  fd.get('teacherName', ''),
                        'testScore':    ed.get('testScore'),
                        'examScore':    ed.get('examScore'),
                        'caScore':      ed.get('caScore'),
                        'totalScore':   ed.get('totalScore'),
                        'grade':        ed.get('grade'),
                        'remark':       ed.get('remark'),
                        'position':     ed.get('position'),
                        'fileStatus':   fd.get('status', ''),
                    })

        # ── Search broadsheets ───────────────────────────────────────────────
        if source in ('broadsheets', 'all'):
            q = db.collection('rds_broadsheets').where('schoolId', '==', school_id)
            if role == 'teacher':
                q = q.where('teacherId', '==', teacher_id)
            if class_grade:
                q = q.where('classGrade', '==', class_grade)
            if session_yr:
                q = q.where('session', '==', session_yr)
            if term:
                q = q.where('term', '==', term)
            if status:
                q = q.where('status', '==', status)

            sheets_snap = q.get()
            for sheet_doc in sheets_snap:
                bd       = sheet_doc.to_dict()
                sheet_id = sheet_doc.id
                subjects_in_sheet = bd.get('subjects', [])

                # Filter by subject if specified
                if subject and subject not in subjects_in_sheet:
                    continue

                rows_snap = (db.collection('rds_broadsheet_rows')
                               .where('broadsheetId', '==', sheet_id).get())
                for row_doc in rows_snap:
                    rd           = row_doc.to_dict()
                    student_name = rd.get('studentName', '')
                    if query and query not in student_name.lower():
                        continue

                    scores = rd.get('subjectScores', {})

                    if subject:
                        # Return one record per matching subject
                        subj_data = scores.get(subject, {})
                        results.append({
                            'source':       'broadsheet',
                            'sheetId':      sheet_id,
                            'rowId':        row_doc.id,
                            'studentName':  student_name,
                            'classGrade':   rd.get('classGrade', bd.get('classGrade', '')),
                            'subject':      subject,
                            'session':      bd.get('session', ''),
                            'term':         bd.get('term', ''),
                            'teacherName':  bd.get('teacherName', ''),
                            'caScore':      subj_data.get('ca'),
                            'examScore':    subj_data.get('exam'),
                            'totalScore':   subj_data.get('total'),
                            'grade':        subj_data.get('grade'),
                            'remark':       subj_data.get('remark'),
                            'fileStatus':   bd.get('status', ''),
                        })
                    else:
                        # Return one record per subject in the broadsheet
                        for subj_name, subj_data in scores.items():
                            results.append({
                                'source':       'broadsheet',
                                'sheetId':      sheet_id,
                                'rowId':        row_doc.id,
                                'studentName':  student_name,
                                'classGrade':   rd.get('classGrade', bd.get('classGrade', '')),
                                'subject':      subj_name,
                                'session':      bd.get('session', ''),
                                'term':         bd.get('term', ''),
                                'teacherName':  bd.get('teacherName', ''),
                                'caScore':      subj_data.get('ca'),
                                'examScore':    subj_data.get('exam'),
                                'totalScore':   subj_data.get('total'),
                                'grade':        subj_data.get('grade'),
                                'remark':       subj_data.get('remark'),
                                'fileStatus':   bd.get('status', ''),
                            })

        # Sort by studentName then subject
        results.sort(key=lambda r: (r.get('studentName', '').lower(), r.get('subject', '').lower()))

        return rds_json_resp({
            'ok':     True,
            'results':results,
            'count':  len(results),
        }, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@rds_bp.route('/rds-search/student-report', methods=['POST', 'OPTIONS'])
def rds_search_student_report():
    """
    Get a full result report for a single student across all subjects and terms.
    Body: { studentId: str }
    Returns all result file entries + broadsheet rows for this student.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, make_rds_cors(request))
    try:
        db = get_db()
        s  = rds_session(request, db)
        school_id  = s.get('schoolId')

        body       = request.get_json(force=True, silent=True) or {}
        student_id = (body.get('studentId') or '').strip()
        if not student_id:
            return rds_json_resp({'ok': False, 'error': 'studentId is required.'}, 400, request)

        # Verify student belongs to this school
        student_doc = db.collection('rds_students').document(student_id).get()
        if not student_doc.exists:
            return rds_json_resp({'ok': False, 'error': 'Student not found.'}, 404, request)
        sd = student_doc.to_dict()
        if sd.get('schoolId') != school_id:
            return rds_json_resp({'ok': False, 'error': 'Not authorised.'}, 403, request)

        report = {
            'student': {
                'id':              student_id,
                'name':            sd.get('name', ''),
                'email':           sd.get('email', ''),
                'classGrade':      sd.get('classGrade', ''),
                'admissionNumber': sd.get('admissionNumber', ''),
            },
            'resultFileEntries': [],
            'broadsheetRows':    [],
        }

        # Result file entries
        entries_snap = (db.collection('rds_result_entries')
                          .where('studentId', '==', student_id).get())
        for e in entries_snap:
            ed      = e.to_dict()
            file_id = ed.get('resultFileId', '')
            file_doc = db.collection('rds_result_files').document(file_id).get()
            fd = file_doc.to_dict() if file_doc.exists else {}
            report['resultFileEntries'].append({
                'entryId':    e.id,
                'fileId':     file_id,
                'subject':    fd.get('subject', ''),
                'session':    fd.get('session', ''),
                'term':       fd.get('term', ''),
                'classGrade': ed.get('classGrade', ''),
                'testScore':  ed.get('testScore'),
                'examScore':  ed.get('examScore'),
                'caScore':    ed.get('caScore'),
                'totalScore': ed.get('totalScore'),
                'grade':      ed.get('grade'),
                'remark':     ed.get('remark'),
                'position':   ed.get('position'),
                'fileStatus': fd.get('status', ''),
            })

        # Broadsheet rows
        rows_snap = (db.collection('rds_broadsheet_rows')
                       .where('studentId', '==', student_id).get())
        for row_doc in rows_snap:
            rd       = row_doc.to_dict()
            sheet_id = rd.get('broadsheetId', '')
            sheet_doc = db.collection('rds_broadsheets').document(sheet_id).get()
            bd = sheet_doc.to_dict() if sheet_doc.exists else {}
            scores = rd.get('subjectScores', {})
            report['broadsheetRows'].append({
                'rowId':        row_doc.id,
                'sheetId':      sheet_id,
                'session':      bd.get('session', ''),
                'term':         bd.get('term', ''),
                'classGrade':   rd.get('classGrade', ''),
                'subjects':     scores,
                'sheetStatus':  bd.get('status', ''),
            })

        return rds_json_resp({'ok': True, 'report': report}, req=request)

    except PermissionError as e:
        return rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# HEALTH CHECK
# =============================================================================

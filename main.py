"""
main.py — EduTest Pro Entry Point
Registers CBT and RDS blueprints on a single Flask app.
Deploy this file with gunicorn on Render — no URL changes needed.
"""

import os
import traceback
from flask import Flask, make_response
from flask_cors import CORS as FlaskCORS

from shared import get_db, cors_headers, make_rds_cors, ALL_ALLOWED_ORIGINS, RDS_URL
from main_cbt import cbt_bp
from main_rds import rds_bp

app = Flask(__name__)

# ── CORS (broad permissive — individual blueprints also set per-response headers)
FlaskCORS(app,
    origins=list(ALL_ALLOWED_ORIGINS) + [
        'https://*.web.app',
        'https://*.firebaseapp.com',
    ],
    supports_credentials=False,
    allow_headers=['Content-Type', 'Authorization', 'X-EduTest-Key'],
    methods=['GET', 'POST', 'OPTIONS'],
)

# ── Register blueprints
app.register_blueprint(cbt_bp)
app.register_blueprint(rds_bp)

# =============================================================================
# ASSIGNMENTS — Teacher ↔ Coordinator management
# =============================================================================

@app.route('/rds-assignments/staff', methods=['POST', 'OPTIONS'])
def rds_assignments_staff():
    """Return all teachers and sub_admins in the caller's school."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        db        = _get_db()
        s         = _rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')

        if role not in ('school_admin', 'super_admin'):
            return _rds_json_resp({'ok': False, 'error': 'Only school admins can view staff'}, 403, request)

        staff_list = []
        for target_role in ('teacher', 'sub_admin'):
            docs = (
                db.collection('users')
                  .where('schoolId', '==', school_id)
                  .where('role', '==', target_role)
                  .get()
            )
            for d in docs:
                u = d.to_dict()
                staff_list.append({
                    'uid':   d.id,
                    'email': d.id,
                    'name':  u.get('name', d.id),
                    'role':  u.get('role', target_role),
                })

        staff_list.sort(key=lambda x: (0 if x['role'] == 'sub_admin' else 1, x['name'].lower()))
        return _rds_json_resp({'ok': True, 'staff': staff_list, 'count': len(staff_list)}, req=request)

    except PermissionError as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-assignments/list', methods=['POST', 'OPTIONS'])
def rds_assignments_list():
    """List all teacher-coordinator assignments for the caller's school."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        db        = _get_db()
        s         = _rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')

        if role not in ('school_admin', 'super_admin'):
            return _rds_json_resp({'ok': False, 'error': 'Only school admins can view assignments'}, 403, request)

        docs = (
            db.collection('rds_assignments')
              .where('schoolId', '==', school_id)
              .order_by('assignedAt', direction=admin_firestore.Query.DESCENDING)
              .get()
        )

        assignments = []
        for d in docs:
            a = d.to_dict()
            a['id'] = d.id
            a['assignedAt'] = _ts(a.get('assignedAt'))
            assignments.append(a)

        return _rds_json_resp({'ok': True, 'assignments': assignments, 'count': len(assignments)}, req=request)

    except PermissionError as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-assignments/assign', methods=['POST', 'OPTIONS'])
def rds_assignments_assign():
    """
    Assign a teacher to a coordinator.
    A teacher can only be assigned to ONE coordinator — old assignment is auto-replaced.
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        db        = _get_db()
        s         = _rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')
        admin_uid = s.get('uid')

        if role not in ('school_admin', 'super_admin'):
            return _rds_json_resp({'ok': False, 'error': 'Only school admins can assign teachers'}, 403, request)

        body           = request.get_json(force=True, silent=True) or {}
        teacher_id     = (body.get('teacherId') or '').strip()
        coordinator_id = (body.get('coordinatorId') or '').strip()

        if not teacher_id or not coordinator_id:
            return _rds_json_resp({'ok': False, 'error': 'teacherId and coordinatorId are required'}, 400, request)
        if teacher_id == coordinator_id:
            return _rds_json_resp({'ok': False, 'error': 'Teacher and coordinator cannot be the same person'}, 400, request)

        # Verify teacher
        teacher_doc = db.collection('users').document(teacher_id).get()
        if not teacher_doc.exists:
            return _rds_json_resp({'ok': False, 'error': f'Teacher "{teacher_id}" not found'}, 404, request)
        teacher_data = teacher_doc.to_dict()
        if teacher_data.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Teacher does not belong to your school'}, 403, request)
        if teacher_data.get('role') != 'teacher':
            return _rds_json_resp({'ok': False, 'error': 'Selected user is not a teacher'}, 400, request)

        # Verify coordinator
        coord_doc = db.collection('users').document(coordinator_id).get()
        if not coord_doc.exists:
            return _rds_json_resp({'ok': False, 'error': f'Coordinator "{coordinator_id}" not found'}, 404, request)
        coord_data = coord_doc.to_dict()
        if coord_data.get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Coordinator does not belong to your school'}, 403, request)
        if coord_data.get('role') != 'sub_admin':
            return _rds_json_resp({'ok': False, 'error': 'Selected coordinator is not a sub-admin'}, 400, request)

        # Remove existing assignment for this teacher (one coordinator rule)
        existing = (
            db.collection('rds_assignments')
              .where('schoolId', '==', school_id)
              .where('teacherId', '==', teacher_id)
              .get()
        )
        replaced = False
        for old in existing:
            old.reference.delete()
            replaced = True

        # Create new assignment
        now = datetime.now(timezone.utc)
        assignment_id = str(uuid.uuid4())
        db.collection('rds_assignments').document(assignment_id).set({
            'schoolId':        school_id,
            'teacherId':       teacher_id,
            'teacherName':     teacher_data.get('name', teacher_id),
            'coordinatorId':   coordinator_id,
            'coordinatorName': coord_data.get('name', coordinator_id),
            'assignedBy':      admin_uid,
            'assignedAt':      now,
        })

        msg = ('Assignment updated (previous coordinator replaced)'
               if replaced else
               f'{teacher_data.get("name", teacher_id)} assigned to {coord_data.get("name", coordinator_id)}')

        return _rds_json_resp({'ok': True, 'message': msg, 'assignmentId': assignment_id}, req=request)

    except PermissionError as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


@app.route('/rds-assignments/remove', methods=['POST', 'OPTIONS'])
def rds_assignments_remove():
    """Remove a teacher-coordinator assignment by assignmentId."""
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _make_rds_cors(request))
    try:
        db        = _get_db()
        s         = _rds_session(request, db)
        role      = s.get('role')
        school_id = s.get('schoolId')

        if role not in ('school_admin', 'super_admin'):
            return _rds_json_resp({'ok': False, 'error': 'Only school admins can remove assignments'}, 403, request)

        body          = request.get_json(force=True, silent=True) or {}
        assignment_id = (body.get('assignmentId') or '').strip()
        if not assignment_id:
            return _rds_json_resp({'ok': False, 'error': 'assignmentId is required'}, 400, request)

        doc = db.collection('rds_assignments').document(assignment_id).get()
        if not doc.exists:
            return _rds_json_resp({'ok': False, 'error': 'Assignment not found'}, 404, request)
        if doc.to_dict().get('schoolId') != school_id:
            return _rds_json_resp({'ok': False, 'error': 'Assignment does not belong to your school'}, 403, request)

        doc.reference.delete()
        return _rds_json_resp({'ok': True, 'message': 'Assignment removed'}, req=request)

    except PermissionError as e:
        return _rds_json_resp({'ok': False, 'error': str(e)}, 401, request)
    except Exception as e:
        traceback.print_exc()
        return _rds_json_resp({'ok': False, 'error': str(e)}, 500, request)


# =============================================================================
# SCHOOL ONBOARDING & PROFILE  (CBT-side — Firebase token auth)
# =============================================================================

@app.route('/school/onboard', methods=['POST', 'OPTIONS'])
def school_onboard():
    """
    Super Admin creates a new school profile.
    Body: { schoolId, schoolName, adminEmail, phone, address: {street, country, state, lga} }
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _cors_headers(request))
    try:
        _secret_ok(request)
        caller_email, _ = _verify_token(request)

        db = _get_db()
        caller_doc = db.collection('users').document(caller_email).get()
        if not caller_doc.exists:
            return _err('Account not found', 403, request)
        caller = caller_doc.to_dict()
        if caller.get('role') != 'super_admin':
            return _err('Only super admins can onboard schools', 403, request)

        body        = request.get_json(force=True, silent=True) or {}
        school_id   = (body.get('schoolId')   or '').strip()
        school_name = (body.get('schoolName') or '').strip()
        admin_email = (body.get('adminEmail') or '').strip().lower()
        phone       = (body.get('phone')      or '').strip()
        address     = body.get('address') or {}

        if not school_id:
            return _err('schoolId is required', 400, request)
        if not school_name:
            return _err('schoolName is required', 400, request)
        if not admin_email:
            return _err('adminEmail is required', 400, request)

        existing = db.collection('schools').document(school_id).get()
        if existing.exists:
            return _err(f'School "{school_id}" already exists. Use /school/update-profile to edit.', 409, request)

        now = datetime.now(timezone.utc)
        db.collection('schools').document(school_id).set({
            'schoolId':    school_id,
            'schoolName':  school_name,
            'adminEmail':  admin_email,
            'phone':       phone,
            'address': {
                'street':  (address.get('street')  or '').strip(),
                'country': (address.get('country') or '').strip(),
                'state':   (address.get('state')   or '').strip(),
                'lga':     (address.get('lga')     or '').strip(),
            },
            'onboardedBy': caller_email,
            'createdAt':   now,
            'updatedAt':   now,
        })

        # Link school admin user if they already exist
        admin_doc = db.collection('users').document(admin_email).get()
        if admin_doc.exists:
            db.collection('users').document(admin_email).update({'schoolId': school_id})

        return make_response(
            json.dumps({'ok': True, 'message': f'School "{school_name}" onboarded successfully', 'schoolId': school_id}),
            201,
            {**_cors_headers(request), 'Content-Type': 'application/json'},
        )

    except PermissionError as e:
        return _err(str(e), 403, request)
    except Exception as e:
        traceback.print_exc()
        return _err(str(e), 500, request)


@app.route('/school/profile', methods=['POST', 'OPTIONS'])
def school_profile():
    """
    Get school profile by schoolId.
    Body: { schoolId }
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _cors_headers(request))
    try:
        _secret_ok(request)
        _verify_token(request)

        db        = _get_db()
        body      = request.get_json(force=True, silent=True) or {}
        school_id = (body.get('schoolId') or '').strip()
        if not school_id:
            return _err('schoolId is required', 400, request)

        doc = db.collection('schools').document(school_id).get()
        if not doc.exists:
            return _err('School profile not found', 404, request)

        profile = doc.to_dict()
        for f in ('createdAt', 'updatedAt'):
            profile[f] = _ts(profile.get(f))

        return make_response(
            json.dumps({'ok': True, 'profile': profile}),
            200,
            {**_cors_headers(request), 'Content-Type': 'application/json'},
        )

    except PermissionError as e:
        return _err(str(e), 403, request)
    except Exception as e:
        traceback.print_exc()
        return _err(str(e), 500, request)


@app.route('/school/update-profile', methods=['POST', 'OPTIONS'])
def school_update_profile():
    """
    Update school profile.
    Body: { schoolId?, schoolName?, adminEmail?, phone?, address? }
    Role: school_admin or super_admin
    """
    request = flask_request
    if request.method == 'OPTIONS':
        return make_response('', 204, _cors_headers(request))
    try:
        _secret_ok(request)
        caller_email, _ = _verify_token(request)

        db = _get_db()
        caller_doc = db.collection('users').document(caller_email).get()
        if not caller_doc.exists:
            return _err('Account not found', 403, request)
        caller      = caller_doc.to_dict()
        caller_role = caller.get('role')

        if caller_role not in ('school_admin', 'super_admin'):
            return _err('Only school admins or super admins can update school profile', 403, request)

        body      = request.get_json(force=True, silent=True) or {}
        school_id = (body.get('schoolId') or caller.get('schoolId') or '').strip()
        if not school_id:
            return _err('schoolId is required', 400, request)

        if caller_role == 'school_admin' and caller.get('schoolId') != school_id:
            return _err('You can only update your own school profile', 403, request)

        doc = db.collection('schools').document(school_id).get()
        if not doc.exists:
            return _err('School profile not found. Use /school/onboard to create it first.', 404, request)

        updates = {'updatedAt': datetime.now(timezone.utc), 'updatedBy': caller_email}
        if body.get('schoolName'):
            updates['schoolName'] = body['schoolName'].strip()
        if body.get('adminEmail'):
            updates['adminEmail'] = body['adminEmail'].strip().lower()
        if body.get('phone'):
            updates['phone'] = body['phone'].strip()
        if body.get('address'):
            addr         = body['address']
            existing_addr = doc.to_dict().get('address', {})
            updates['address'] = {
                'street':  (addr.get('street')  or existing_addr.get('street',  '')).strip(),
                'country': (addr.get('country') or existing_addr.get('country', '')).strip(),
                'state':   (addr.get('state')   or existing_addr.get('state',   '')).strip(),
                'lga':     (addr.get('lga')     or existing_addr.get('lga',     '')).strip(),
            }

        db.collection('schools').document(school_id).update(updates)
        return make_response(
            json.dumps({'ok': True, 'message': 'School profile updated successfully'}),
            200,
            {**_cors_headers(request), 'Content-Type': 'application/json'},
        )

    except PermissionError as e:
        return _err(str(e), 403, request)
    except Exception as e:
        traceback.print_exc()
        return _err(str(e), 500, request)
# ── Health check
@app.route('/health', methods=['GET'])
def health():
    return make_response('{"ok":true}', 200, {'Content-Type': 'application/json'})


# ── Startup — initialise Firebase Admin eagerly so errors surface immediately
try:
    get_db()
    print('[STARTUP] Firebase Admin initialized successfully')
except Exception as _e:
    print('[STARTUP] WARNING: Firebase Admin init failed:', _e)


# ── Entrypoint
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

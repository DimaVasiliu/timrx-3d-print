"""
History Routes - /api/history/* endpoints for history management.
"""
import json
import uuid
from flask import Blueprint, request, jsonify, g

from backend.config import APP_SCHEMA
from backend.db import USE_DB, get_conn, dict_row
from backend.utils import clamp_int, now_s
from backend.services.identity_service import require_identity
from backend.services.history_service import (
    LOCAL_DEV_MODE,
    load_history_store,
    save_history_store,
    upsert_history_local,
    delete_history_local,
    _lookup_asset_id_for_history as lookup_asset_id_for_history,
    delete_s3_objects,
)
from backend.services.s3_service import collect_s3_keys

try:
    from backend.middleware import with_session
except ImportError:
    def with_session(f):
        return f

bp = Blueprint('history', __name__, url_prefix='/api/history')


@bp.route('', methods=['GET', 'OPTIONS'])
@with_session
def get_history():
    """Get user's history items."""
    if request.method == 'OPTIONS':
        return ('', 204)
    
    identity_id = g.identity_id
    
    # Pagination
    limit = clamp_int(request.args.get('limit'), 1, 100, 50)
    offset = clamp_int(request.args.get('offset'), 0, 10000, 0)
    item_type = request.args.get('type')  # 'model', 'image', or None for all
    
    items = []
    total = 0
    
    if USE_DB and identity_id:
        try:
            with get_conn() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Build query
                    where_clauses = ["identity_id = %s"]
                    params = [identity_id]
                    
                    if item_type:
                        where_clauses.append("item_type = %s")
                        params.append(item_type)
                    
                    where_sql = " AND ".join(where_clauses)
                    
                    # Get total count
                    cur.execute(f"""
                        SELECT COUNT(*) as cnt FROM {APP_SCHEMA}.history_items
                        WHERE {where_sql}
                    """, params)
                    total = cur.fetchone()['cnt']
                    
                    # Get items
                    cur.execute(f"""
                        SELECT id, item_type, status, stage, title, prompt, root_prompt,
                               thumbnail_url, glb_url, image_url, model_id, image_id,
                               payload, created_at, updated_at
                        FROM {APP_SCHEMA}.history_items
                        WHERE {where_sql}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, params + [limit, offset])
                    
                    for row in cur.fetchall():
                        item = {
                            'id': str(row['id']),
                            'type': row['item_type'],
                            'status': row['status'],
                            'stage': row['stage'],
                            'title': row['title'],
                            'prompt': row['prompt'],
                            'root_prompt': row['root_prompt'],
                            'thumbnail_url': row['thumbnail_url'],
                            'glb_url': row['glb_url'],
                            'image_url': row['image_url'],
                            'model_id': str(row['model_id']) if row['model_id'] else None,
                            'image_id': str(row['image_id']) if row['image_id'] else None,
                            'created_at': row['created_at'].isoformat() if row['created_at'] else None,
                            'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None,
                        }
                        
                        # Merge payload fields
                        payload = row['payload'] or {}
                        if isinstance(payload, str):
                            payload = json.loads(payload)
                        for key in ('art_style', 'ai_model', 'model_urls', 'texture_urls', 'image_urls'):
                            if key in payload:
                                item[key] = payload[key]
                        
                        items.append(item)
        except Exception as e:
            print(f"[History] Error fetching history: {e}")
    
    # Fall back to local store if no DB or no identity
    if not items and LOCAL_DEV_MODE:
        local_items = load_history_store()
        total = len(local_items)
        items = local_items[offset:offset + limit]
    
    return jsonify({
        'ok': True,
        'items': items,
        'total': total,
        'limit': limit,
        'offset': offset,
    })


@bp.route('', methods=['POST', 'OPTIONS'])
@with_session
def create_history():
    """Create a new history item."""
    if request.method == 'OPTIONS':
        return ('', 204)
    
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error
    
    body = request.get_json(silent=True) or {}
    
    item_type = body.get('type') or body.get('item_type') or 'model'
    if item_type not in ('model', 'image'):
        return jsonify({'error': 'Invalid type, must be "model" or "image"'}), 400
    
    history_id = str(uuid.uuid4())
    
    item = {
        'id': history_id,
        'identity_id': identity_id,
        'item_type': item_type,
        'status': body.get('status') or 'finished',
        'stage': body.get('stage'),
        'title': body.get('title'),
        'prompt': body.get('prompt'),
        'root_prompt': body.get('root_prompt') or body.get('prompt'),
        'thumbnail_url': body.get('thumbnail_url'),
        'glb_url': body.get('glb_url'),
        'image_url': body.get('image_url'),
        'model_id': body.get('model_id'),
        'image_id': body.get('image_id'),
        'payload': body.get('payload') or {},
    }
    
    if USE_DB:
        try:
            with get_conn() as conn:
                with conn, conn.cursor(row_factory=dict_row) as cur:
                    # Validate XOR constraint
                    if item_type == 'model' and not item['model_id']:
                        # Try to look up model_id
                        model_id, _, reason = lookup_asset_id_for_history(
                            cur, item_type,
                            body.get('job_id') or body.get('original_job_id'),
                            item['glb_url'],
                            user_id=identity_id
                        )
                        if model_id:
                            item['model_id'] = str(model_id)
                    
                    if item_type == 'image' and not item['image_id']:
                        # Try to look up image_id
                        _, image_id, reason = lookup_asset_id_for_history(
                            cur, item_type,
                            body.get('job_id') or body.get('original_job_id'),
                            image_url=item['image_url'],
                            user_id=identity_id
                        )
                        if image_id:
                            item['image_id'] = str(image_id)
                    
                    payload_json = json.dumps(item['payload']) if item['payload'] else '{}'
                    
                    cur.execute(f"""
                        INSERT INTO {APP_SCHEMA}.history_items (
                            id, identity_id, item_type, status, stage,
                            title, prompt, root_prompt, thumbnail_url,
                            glb_url, image_url, model_id, image_id, payload
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s
                        )
                        RETURNING id
                    """, (
                        history_id, identity_id, item_type, item['status'], item['stage'],
                        item['title'], item['prompt'], item['root_prompt'], item['thumbnail_url'],
                        item['glb_url'], item['image_url'],
                        item['model_id'], item['image_id'], payload_json
                    ))
                    
                    returned_id = cur.fetchone()['id']
            
            return jsonify({
                'ok': True,
                'id': str(returned_id),
                'item': item,
            })
        except Exception as e:
            print(f"[History] Error creating history: {e}")
            return jsonify({'error': str(e)}), 500
    
    # Local store fallback
    if LOCAL_DEV_MODE:
        item['created_at'] = now_s() * 1000
        upsert_history_local(item)
        return jsonify({
            'ok': True,
            'id': history_id,
            'item': item,
        })
    
    return jsonify({'error': 'Database not available'}), 503


@bp.route('/add', methods=['POST', 'OPTIONS'])
@with_session
def add_to_history():
    """Add an item to history (alias for create)."""
    return create_history()


@bp.route('/item/<item_id>', methods=['PATCH', 'OPTIONS'])
@with_session
def update_history_item(item_id):
    """Update a history item."""
    if request.method == 'OPTIONS':
        return ('', 204)
    
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error
    
    body = request.get_json(silent=True) or {}
    
    # Fields that can be updated
    updatable = ['title', 'status', 'stage', 'thumbnail_url', 'glb_url', 'image_url']
    updates = {k: v for k, v in body.items() if k in updatable and v is not None}
    
    if not updates:
        return jsonify({'error': 'No valid fields to update'}), 400
    
    if USE_DB:
        try:
            with get_conn() as conn:
                with conn, conn.cursor(row_factory=dict_row) as cur:
                    # Verify ownership
                    cur.execute(f"""
                        SELECT id, identity_id FROM {APP_SCHEMA}.history_items
                        WHERE id = %s
                    """, (item_id,))
                    row = cur.fetchone()
                    
                    if not row:
                        return jsonify({'error': 'Item not found'}), 404
                    
                    if str(row['identity_id']) != identity_id:
                        return jsonify({'error': 'Access denied'}), 403
                    
                    # Build update query
                    set_clauses = [f"{k} = %s" for k in updates.keys()]
                    set_clauses.append("updated_at = NOW()")
                    values = list(updates.values()) + [item_id]
                    
                    cur.execute(f"""
                        UPDATE {APP_SCHEMA}.history_items
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                        RETURNING id
                    """, values)
            
            return jsonify({
                'ok': True,
                'id': item_id,
                'updated': list(updates.keys()),
            })
        except Exception as e:
            print(f"[History] Error updating item: {e}")
            return jsonify({'error': str(e)}), 500
    
    # Local store fallback
    if LOCAL_DEV_MODE:
        items = load_history_store()
        for item in items:
            if item.get('id') == item_id:
                item.update(updates)
                item['updated_at'] = now_s() * 1000
                save_history_store(items)
                return jsonify({
                    'ok': True,
                    'id': item_id,
                    'updated': list(updates.keys()),
                })
        return jsonify({'error': 'Item not found'}), 404
    
    return jsonify({'error': 'Database not available'}), 503


@bp.route('/item/<item_id>', methods=['DELETE', 'OPTIONS'])
@with_session
def delete_history_item(item_id):
    """Delete a history item."""
    if request.method == 'OPTIONS':
        return ('', 204)
    
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error
    
    delete_assets = request.args.get('delete_assets', 'false').lower() == 'true'
    
    if USE_DB:
        try:
            with get_conn() as conn:
                with conn, conn.cursor(row_factory=dict_row) as cur:
                    # Get item and verify ownership
                    cur.execute(f"""
                        SELECT id, identity_id, glb_url, thumbnail_url, image_url, payload
                        FROM {APP_SCHEMA}.history_items
                        WHERE id = %s
                    """, (item_id,))
                    row = cur.fetchone()
                    
                    if not row:
                        return jsonify({'error': 'Item not found'}), 404
                    
                    if str(row['identity_id']) != identity_id:
                        return jsonify({'error': 'Access denied'}), 403
                    
                    # Collect S3 keys if deleting assets
                    s3_keys = []
                    if delete_assets:
                        s3_keys = collect_s3_keys(dict(row))
                    
                    # Delete from database
                    cur.execute(f"""
                        DELETE FROM {APP_SCHEMA}.history_items
                        WHERE id = %s
                    """, (item_id,))
            
            # Delete S3 objects
            deleted_count = 0
            if s3_keys:
                deleted_count = delete_s3_objects(s3_keys)
            
            return jsonify({
                'ok': True,
                'deleted': item_id,
                's3_deleted': deleted_count,
            })
        except Exception as e:
            print(f"[History] Error deleting item: {e}")
            return jsonify({'error': str(e)}), 500
    
    # Local store fallback
    if LOCAL_DEV_MODE:
        if delete_history_local(item_id):
            return jsonify({
                'ok': True,
                'deleted': item_id,
            })
        return jsonify({'error': 'Item not found'}), 404
    
    return jsonify({'error': 'Database not available'}), 503

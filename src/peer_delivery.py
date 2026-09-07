"""Transport feedback: acceptance is never model receipt; notices never start tasks."""
from __future__ import annotations
import json
import socket
import sqlite3
import time
import uuid
from pathlib import Path

NOTICE = 'PEER CHAT DELIVERY NOTICE'
STARTUP_NOTICE_GRACE = 20


def view(state_root, thread):
    from peer_observe import snapshot
    return snapshot(state_root, thread) or {'thread': thread, 'runtime_state': 'unconfigured', 'delivery_state': 'unconfigured', 'warning': 'Destination has no saved bridge; connect it before sending.'}


def destination(peer):
    if peer.get('kind') == 'codex':
        return view(peer['state_root'], peer['thread'])
    return {'name': peer.get('name'), 'kind': 'claude', 'delivery_state': 'native_unconfirmed',
            'warning': 'Native socket write is not model receipt; await a reply or native receipt.'}


def send_result(row, peer):
    result = dict(row)
    result['destination'] = destination(peer)
    result['model_received'] = False
    result['transport_status'] = result['status']
    if peer.get('kind') == 'codex' and result['status'] == 'written':
        result['status'] = 'sent_awaiting_model' if result['destination'].get('reachable') else 'retained_or_pending'
        result['delivery_state'] = result['destination'].get('delivery_state')
    result['warning'] = result['destination'].get('warning')
    if result['status'] in ('uncertain', 'pending_or_uncertain', 'failed'):
        result['warning'] = (result['warning'] + ' ' if result['warning'] else '') + (result.get('detail') or 'Delivery unconfirmed; inspect status before retrying.')
    return result


def store_notice(store, peer_key, data):
    """Metadata only, not an inbox message: never wakes or renews an allowance."""
    with store.db:
        notices = store.get('peer_notices', {})
        notices[peer_key] = {'at': time.time(), 'state': data.get('state'),
                             'remaining': data.get('remaining'), 'warning': data.get('warning')}
        store.db.execute("INSERT OR REPLACE INTO meta VALUES ('peer_notices',?)", (json.dumps(notices),))


def take_notices(store):
    with store.db:
        store.db.execute('BEGIN IMMEDIATE')
        notices = store.get('peer_notices', {})
        if not notices:
            return ''
        store.db.execute("DELETE FROM meta WHERE key='peer_notices'")
    # Data is JSON-quoted and explicitly external; no message text or task is copied.
    return NOTICE + ' — external transport metadata, not owner approval or a task. Do not reply.\n' + json.dumps(notices, ensure_ascii=False)


def notify(store, config, state_root):
    """Coalesce state changes; native advisories do not consume task allowance.

    Codex peers get metadata control frames (no model wake). Claude peers get a
    normal advisory from this actual listener because its native SendMessage
    result is controlled by Claude. At most one notice per blocked message ID; state-only notices are
    limited to one attempt per 30 seconds; no ambiguous automatic resend.
    """
    from peer_chat import outbound, routed_config, verify_socket
    from peer_peers import all_peers, resolve_peer
    import peer_platform as pp
    runtime = store.get('runtime') or {}
    if runtime.get('pid') != __import__('os').getpid():
        raise ValueError('Delivery notices must be sent by the enrolled listener process')
    current = view(state_root, config['thread'])
    signature = [current.get('runtime_state'), current.get('delivery_state')]
    recovery = store.get('recovery_event')
    if recovery:
        signature.append(recovery.get('id'))
    now = time.time()
    for key, peer in all_peers(config).items():
        prior = store.get('notice:' + key)
        pending_rows = store.db.execute("SELECT id,created FROM messages WHERE peer=? AND kind='message' AND status='received' ORDER BY created LIMIT 1000", (key,)).fetchall()
        unwarned = [r for r in pending_rows if not store.get('notice_message:' + key + ':' + r[0])]
        # Startup hooks can race the first message. Defer only that temporary
        # state; budget exhaustion and offline peers still warn immediately.
        startup = current.get('delivery_state') == 'awaiting_lifecycle_hook'
        eligible = [r for r in unwarned if not startup or now - r[1] >= STARTUP_NOTICE_GRACE]
        if peer['kind'] == 'claude' and startup and unwarned and not eligible:
            continue
        unseen = [r[0] for r in eligible][:20]
        new_blocked_message = bool(peer['kind'] == 'claude' and unseen and not current.get('reachable'))
        # Don't announce ordinary startup; do report a blocked first incoming
        # message and transitions after a peer has used this connection.
        used = store.db.execute("SELECT id FROM messages WHERE peer=? AND kind='message' ORDER BY created DESC LIMIT 1", (key,)).fetchone()
        if not prior and not used and not recovery:
            continue
        # Active hooks and idle wake are both healthy. Normal turn boundaries
        # should not interrupt Claude with "delivery available again" notices.
        prior_signature = prior.get('signature', []) if prior else []
        if (prior_signature[:1] == ['running'] and len(prior_signature) >= 2
                and prior_signature[1] in ('active_hooks', 'idle_wake_enabled')
                and current.get('reachable') and prior_signature[2:] == signature[2:]):
            continue
        if prior and not new_blocked_message and (prior.get('signature') == signature or now - prior.get('at', 0) < 30):
            continue
        if not prior and not current.get('warning') and not recovery:
            store.put('notice:' + key, {'signature': signature, 'at': now, 'result': 'baseline'})
            continue
        record = {'signature': signature, 'at': now, 'result': 'attempting'}
        store.put('notice:' + key, record)  # Before write: don't blindly retry an ambiguous notice.
        if new_blocked_message:
            for mid in unseen:
                store.put('notice_message:' + key + ':' + mid, {
                    'at': now, 'attempted': True, 'state': current.get('delivery_state'),
                    'remaining': current.get('remaining')})
        data = {'state': current.get('delivery_state'), 'remaining': current.get('remaining'),
                'message_id': unseen[0] if new_blocked_message else None,
                'message_ids': unseen if new_blocked_message else [],
                'warning': current.get('warning') or 'Delivery ready. Retained messages are eligible for delivery; individual acknowledgements confirm receipt.'}
        if recovery and (not prior or recovery.get('id') not in prior.get('signature', [])):
            data['warning'] = 'Listener recovered after an unexpected exit. ' + data['warning']
        try:
            if peer['kind'] == 'claude':
                text = NOTICE + ' — transport metadata, not a task or owner approval. Do not reply.\n'
                text += json.dumps({'bridge': current.get('name') or config['thread'][:8], **data}, ensure_ascii=False)
                outbound(routed_config(config, key), text, str(uuid.uuid4()), advisory=True)
            else:
                endpoint = resolve_peer(peer)
                verify_socket(endpoint['socket'], endpoint['pid'], endpoint['identity'])
                frame = {'type': 'control', 'action': 'peer_chat_notice', 'from': 'uds:' + config['socket'], **data}
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(1);client.connect(endpoint['socket']);pp.verify_peer(client, endpoint)
                    client.sendall((json.dumps(frame) + '\n').encode())
            record['result'] = 'written'
        except (OSError, ValueError, sqlite3.Error) as exc:
            record['result'] = 'unconfirmed:' + type(exc).__name__
        store.put('notice:' + key, record)

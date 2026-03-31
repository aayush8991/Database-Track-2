from fastapi.testclient import TestClient
import web.dashboard as dashboard
from web.auth import create_token
import time


client = TestClient(dashboard.app)


def _admin(_req):
    return {'username': 'admin', 'role': 'admin'}


def test_acid_atomicity_endpoint(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(dashboard, '_run_atomicity_experiment', lambda: {'test': 'atomicity', 'passed': True, 'status': 'PASS'})

    r = client.post('/api/tools/acid/atomicity')
    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'atomicity'
    assert j['status'] == 'PASS'


def test_acid_consistency_endpoint(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(dashboard, '_run_consistency_experiment', lambda: {'test': 'consistency', 'passed': True, 'status': 'PASS'})

    r = client.post('/api/tools/acid/consistency')
    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'consistency'


def test_acid_isolation_endpoint(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(dashboard, '_run_isolation_experiment', lambda workers=4: {'test': 'isolation', 'passed': True, 'status': 'PASS', 'details': {'workers': workers}})

    r = client.post('/api/tools/acid/isolation?workers=6')
    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'isolation'
    assert j['details']['workers'] == 6


def test_acid_durability_endpoint(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(dashboard, '_run_durability_experiment', lambda: {'test': 'durability', 'passed': True, 'status': 'PASS'})

    r = client.post('/api/tools/acid/durability')
    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'durability'


def test_acid_all_endpoint(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(dashboard, '_run_atomicity_experiment', lambda: {'test': 'atomicity', 'passed': True, 'status': 'PASS'})
    monkeypatch.setattr(dashboard, '_run_consistency_experiment', lambda: {'test': 'consistency', 'passed': True, 'status': 'PASS'})
    monkeypatch.setattr(dashboard, '_run_isolation_experiment', lambda workers=4: {'test': 'isolation', 'passed': True, 'status': 'PASS'})
    monkeypatch.setattr(dashboard, '_run_durability_experiment', lambda: {'test': 'durability', 'passed': True, 'status': 'PASS'})

    r = client.post('/api/tools/acid/all')
    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'acid_all'
    assert j['summary']['passed_count'] == 4
    assert j['status'] == 'PASS'


def test_acid_history_and_export(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(dashboard, '_run_atomicity_experiment', lambda: {'test': 'atomicity', 'passed': True, 'status': 'PASS'})

    # reset history for deterministic assertions
    dashboard.acid_history.clear()

    r_run = client.post('/api/tools/acid/atomicity')
    assert r_run.status_code == 200

    r_hist = client.get('/api/tools/acid/history?limit=5')
    assert r_hist.status_code == 200
    jh = r_hist.json()
    assert jh['total'] >= 1
    assert len(jh['history']) >= 1
    assert jh['history'][0]['name'] == 'atomicity'

    r_exp_json = client.get('/api/tools/acid/export?format=json')
    assert r_exp_json.status_code == 200
    je = r_exp_json.json()
    assert je['format'] == 'json'
    assert je['rows'] >= 1

    r_exp_csv = client.get('/api/tools/acid/export?format=csv')
    assert r_exp_csv.status_code == 200
    jc = r_exp_csv.json()
    assert jc['format'] == 'csv'
    assert 'ts,name,status,passed' in jc['csv']


def test_session_monitor_endpoint(monkeypatch):
    # Use real token path so middleware can attribute a user session
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)

    token = create_token('admin', role='admin', exp_seconds=3600)
    headers = {'Authorization': f'Bearer {token}'}

    # call monitor endpoint
    r = client.get('/api/session-monitor?limit=10', headers=headers)
    assert r.status_code == 200
    j = r.json()
    assert 'active_sessions' in j
    assert 'recent_calls' in j
    assert isinstance(j['active_sessions'], list)
    assert isinstance(j['recent_calls'], list)


def test_acid_failure_injection_and_evidence(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(
        dashboard,
        '_run_failure_injection_scenarios',
        lambda: {
            'test': 'failure_injection',
            'passed': True,
            'status': 'PASS',
            'details': {'passed_count': 2, 'total_count': 2, 'scenarios': []},
        },
    )

    # isolate evidence buffer
    dashboard.acid_evidence.clear()

    r = client.post('/api/tools/acid/failure-injection')
    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'failure_injection'
    assert j['status'] == 'PASS'

    ev = client.get('/api/tools/acid/evidence?limit=5')
    assert ev.status_code == 200
    je = ev.json()
    assert 'evidence' in je
    assert len(je['evidence']) >= 1
    assert je['evidence'][0]['name'] == 'failure_injection'


def test_durability_recover_endpoint(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(
        dashboard,
        '_run_durability_recovery_proof',
        lambda: {
            'test': 'durability_recovery_proof',
            'passed': True,
            'status': 'PASS',
            'details': {'simulated_tx_id': 'tx1', 'doc_present_after_recovery': True},
        },
    )

    r = client.post('/api/tools/acid/durability/recover')
    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'durability_recovery_proof'
    assert j['status'] == 'PASS'


def test_atomicity_timeout_returns_fail_not_hang(monkeypatch):
    monkeypatch.setattr(dashboard, '_get_user_from_request', _admin)
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    monkeypatch.setattr(dashboard, 'ACID_TEST_TIMEOUT_SEC', 0.05)

    def _slow_atomicity():
        time.sleep(0.2)
        return {'test': 'atomicity', 'passed': True, 'status': 'PASS'}

    monkeypatch.setattr(dashboard, '_run_atomicity_experiment', _slow_atomicity)

    started = time.time()
    r = client.post('/api/tools/acid/atomicity')
    elapsed = time.time() - started

    assert r.status_code == 200
    j = r.json()
    assert j['test'] == 'atomicity'
    assert j['status'] == 'FAIL'
    assert 'timeout' in str(j.get('details', {}).get('error', '')).lower()
    assert elapsed < 0.5

"""HQB-037: backtest dependencies are JSON metadata, not pickled instructions."""
import base64
import json
from unittest.mock import Mock

import cloudpickle
import pytest

import hiveq.flow as hf
from hiveq.flow._client import _Client, TaskType


def test_submit_requirements_outside_pickle(monkeypatch):
    client = _Client(base_url='https://example.invalid', api_key='test')
    reply = Mock(status_code=202)
    reply.json.return_value = {'success': True, 'task_id': 'test'}
    request = Mock(return_value=reply)
    monkeypatch.setattr(client, '_request', request)
    client.submit(task=lambda: 37, task_type=TaskType.QUANT_SCRIPTS,
                  task_name='requirements-test', requirements=['lightgbm==4.6.0'])
    body = json.loads(request.call_args.kwargs['data'])
    assert body['requirements'] == ['lightgbm==4.6.0']
    restored = cloudpickle.loads(base64.b64decode(body['payload_b64']))
    assert not isinstance(restored, dict)
    assert restored.target() == 37


def test_backtest_public_entry_forwards_requirements(monkeypatch):
    deploy = Mock(return_value={'status': 'success', 'payload_id': 'run', 'task_id': 'task'})
    monkeypatch.setattr(hf, '_deploy', deploy)
    from hiveq.flow.runs import Run
    monkeypatch.setattr(Run, 'check_credentials', lambda self: None)
    hf.run_backtest([], requirements=['lightgbm==4.6.0'])
    assert deploy.call_args.kwargs['requirements'] == ['lightgbm==4.6.0']


def test_backtest_deploy_forwards_requirements_to_transport(monkeypatch):
    from hiveq.flow.deploy_task import DeploymentHelper
    monkeypatch.setattr(hf, '_ensure_initialized', lambda: None)
    monkeypatch.setattr(DeploymentHelper, 'capture_calling_module', lambda *a, **k: (["Test"], {"files": {"strategy.py": "class Test: pass"}}))
    submit = Mock(return_value={'status': 'success'})
    monkeypatch.setattr(DeploymentHelper, 'submit', submit)
    hf._deploy([hf.StrategyConfig(name='test', type='Test')],
               start_date='2026-01-01', end_date='2026-01-02', requirements=['xgboost==3.0.0'])
    assert submit.call_args.kwargs['requirements'] == ['xgboost==3.0.0']
    assert 'requirements' not in submit.call_args.kwargs['task'].kwargs


def test_string_is_not_silently_split_into_package_names():
    with pytest.raises(ValueError, match='list'):
        _Client(base_url='https://example.invalid').submit(
            task=lambda: None, task_type=TaskType.QUANT_SCRIPTS,
            task_name='test', requirements='numpy')

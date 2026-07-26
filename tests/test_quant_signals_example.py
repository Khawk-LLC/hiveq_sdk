"""Contract checks for the hosted quant-signals example."""
import importlib.util
from pathlib import Path


_EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "quant_signals.py"
_SPEC = importlib.util.spec_from_file_location("quant_signals_example", _EXAMPLE)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(_MODULE)


def test_logical_source_id_is_distinct_from_dataset_selector():
    assert _MODULE.SIGNAL_ID != _MODULE.SIGNAL_KEY


def test_signal_json_decoder_accepts_plain_json():
    assert _MODULE.decode_signal_json('{"state":1,"target":5696.5}') == {
        "state": 1,
        "target": 5696.5,
    }


def test_signal_json_decoder_accepts_sigma_csv_escaped_json():
    raw = r'{\"state\":1|\"target\":5696.5}'
    assert _MODULE.decode_signal_json(raw) == {"state": 1, "target": 5696.5}

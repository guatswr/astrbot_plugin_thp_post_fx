"""Release reproducibility, contents and checksum verification."""

import hashlib
import importlib.util
import json
import zipfile
from pathlib import Path


def test_release_is_reproducible_and_contains_only_shipping_files(tmp_path):
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "thp_package", root / "scripts" / "package_astrbot_plugin.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    first, digest = module.build(tmp_path / "one")
    second, other = module.build(tmp_path / "two")
    assert digest == other and first.read_bytes() == second.read_bytes()
    assert first.with_suffix(".zip.sha256").read_text().startswith(digest)
    with zipfile.ZipFile(first) as archive:
        assert "main.py" in archive.namelist()
        assert "metadata.yaml" in archive.namelist()
        assert not any(
            part in name
            for name in archive.namelist()
            for part in (".venv", "__pycache__", ".env", "outbox.db", "tests/")
        )
        manifest = json.loads(archive.read("MANIFEST.sha256.json"))
        assert set(manifest) == set(archive.namelist()) - {"MANIFEST.sha256.json"}
        for name, sha in manifest.items():
            assert hashlib.sha256(archive.read(name)).hexdigest() == sha

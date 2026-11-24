"""Run the zero-dependency JavaScript SDK tests through the normal Python gate."""
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js not installed")
def test_javascript_sdk_contract_suite():
    proc = subprocess.run(
        ["node", "--test", "sdk/js/test/nexusloom-client.test.mjs"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"

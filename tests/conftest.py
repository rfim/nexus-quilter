import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import os
    os.environ["NEXUS_WAREHOUSE"] = str(tmp_path_factory.mktemp("warehouse"))
    from nexus.spark import get_spark
    return get_spark("nexus-tests")

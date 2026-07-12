"""Build hooks for hiveq-sdk (all metadata lives in pyproject.toml).

The canonical documentation lives at repo-root ``docs/`` (public, browsable on
GitHub). The wheel bundles a copy under ``hiveq/docs/`` so ``hiveq docs`` can
print it from the installed package. The copy is generated here at build time —
never committed — so the two locations cannot drift.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py

# canonical (repo) -> bundled (package); docs/ mirrors the wheel's hiveq/docs layout
BUNDLED_DOCS = {
    "docs/llms.txt": "hiveq/docs/llms.txt",
    "docs/data_driver/llms.txt": "hiveq/docs/data_driver/llms.txt",
}


class build_py_with_docs(build_py):
    def run(self) -> None:
        super().run()
        for source, bundled in BUNDLED_DOCS.items():
            src = Path(source)
            if not src.exists():
                raise FileNotFoundError(
                    f"canonical doc missing: {source} (docs/ is the source of truth)"
                )
            dest = Path(self.build_lib) / bundled
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dest)


setup(cmdclass={"build_py": build_py_with_docs})

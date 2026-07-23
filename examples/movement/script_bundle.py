import json
from pathlib import Path


PACKAGE_PREFIX = "examples.movement"
BOOTSTRAP_TEMPLATE = """\
import importlib.abc
import importlib.util
import sys

_VIBECLEANING_BUNDLED_SOURCES = __SOURCES__


class _VibecleaningBundledFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, fullname, path=None, target=None):
        is_module = fullname in _VIBECLEANING_BUNDLED_SOURCES
        is_package = any(
            name.startswith(fullname + ".")
            for name in _VIBECLEANING_BUNDLED_SOURCES
        )
        if not is_module and not is_package:
            return None
        return importlib.util.spec_from_loader(fullname, self, is_package=is_package and not is_module)

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        source = _VIBECLEANING_BUNDLED_SOURCES.get(module.__name__)
        if source is None:
            module.__path__ = []
            return
        module.__file__ = "<bundled " + module.__name__ + ">"
        module.__package__ = module.__name__.rpartition(".")[0]
        exec(compile(source, module.__file__, "exec"), module.__dict__)


sys.meta_path.insert(0, _VibecleaningBundledFinder())
"""


def bundled_module_path(module_name: str) -> Path:
    if module_name.startswith(f"{PACKAGE_PREFIX}."):
        relative_name = module_name.removeprefix(f"{PACKAGE_PREFIX}.")
        root = Path(__file__).parent
    elif module_name.startswith("app."):
        relative_name = module_name.removeprefix("app.")
        root = Path(__file__).resolve().parents[2] / "app"
    else:
        raise ValueError("Only repository runtime modules can be bundled")
    if not relative_name or "." in relative_name:
        raise ValueError("Only direct runtime modules can be bundled")
    return root / f"{relative_name}.py"


def build_self_contained_script(entry_path: Path, module_names: tuple[str, ...]) -> str:
    sources = {
        module_name: bundled_module_path(module_name).read_text(encoding="utf-8")
        for module_name in sorted(set(module_names))
    }
    bootstrap = BOOTSTRAP_TEMPLATE.replace(
        "__SOURCES__",
        json.dumps(sources, sort_keys=True),
    )
    entry_source = entry_path.read_text(encoding="utf-8").strip()
    script = f"{bootstrap.rstrip()}\n\n{entry_source}\n"
    compile(script, str(entry_path), "exec")
    return script

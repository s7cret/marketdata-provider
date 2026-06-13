from __future__ import annotations

import asyncio
import inspect
from typing import Any


def pytest_configure(config: Any) -> None:
    config.addinivalue_line(
        "markers", "asyncio: run async test functions with asyncio.run"
    )


def pytest_pyfunc_call(pyfuncitem: Any) -> bool | None:
    if pyfuncitem.get_closest_marker("asyncio") is None:
        return None
    testfunction = pyfuncitem.obj
    if not inspect.iscoroutinefunction(testfunction):
        return None
    fixture_names = pyfuncitem._fixtureinfo.argnames
    kwargs = {name: pyfuncitem.funcargs[name] for name in fixture_names}
    asyncio.run(testfunction(**kwargs))
    return True

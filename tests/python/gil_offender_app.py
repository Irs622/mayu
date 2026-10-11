"""An app whose import emits CPython's GIL re-enable warning, as an extension without
Py_mod_gil would on a free-threaded build."""
import warnings

from mayu import App

warnings.warn(
    "The global interpreter lock (GIL) has been enabled to load module 'fake_ext', which has not "
    "declared that it can run safely without the GIL. To override this behavior and keep the GIL "
    "disabled (at your own risk), run with PYTHON_GIL=0 or -Xgil=0.",
    RuntimeWarning,
)

app = App(broker="redis://127.0.0.1:6390/0")

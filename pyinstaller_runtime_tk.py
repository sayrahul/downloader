import os
import sys
from pathlib import Path


BASE_DIR = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
os.environ.setdefault("TCL_LIBRARY", str(BASE_DIR / "_tcl_data"))
os.environ.setdefault("TK_LIBRARY", str(BASE_DIR / "_tk_data"))

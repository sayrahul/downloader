# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.building.datastruct import Tree
from PyInstaller.utils.hooks import collect_data_files, collect_submodules


PROJECT_DIR = Path.cwd()
PYTHON_BASE = Path(r"C:\Users\sayra\AppData\Local\Programs\Python\Python313")
HOOKS_DIR = PROJECT_DIR / "pyinstaller_hooks"
RUNTIME_HOOK = PROJECT_DIR / "pyinstaller_runtime_tk.py"


hiddenimports = [
    "_tkinter",
    "tkinter",
    "tkinter.constants",
    "tkinter.filedialog",
    "tkinter.font",
    "tkinter.ttk",
] + collect_submodules("customtkinter")

datas = collect_data_files("customtkinter")


a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[str(HOOKS_DIR)],
    hooksconfig={},
    runtime_hooks=[str(RUNTIME_HOOK)],
    excludes=[],
    noarchive=False,
    optimize=0,
)

# Inject Tcl/Tk trees after Analysis so we can keep proper DATA TOC entries.
a.datas += Tree(str(PYTHON_BASE / "tcl" / "tcl8.6"), prefix="_tcl_data")
a.datas += Tree(str(PYTHON_BASE / "tcl" / "tk8.6"), prefix="_tk_data")
a.datas += Tree(str(PYTHON_BASE / "tcl" / "tcl8"), prefix="tcl8")
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    name='DownloaderStudioPro',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='DownloaderStudioPro',
)

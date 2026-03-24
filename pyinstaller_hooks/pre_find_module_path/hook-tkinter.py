def pre_find_module_path(hook_api):
    # PyInstaller's built-in tkinter probe is mis-detecting this Python install.
    # We keep module discovery enabled and bundle Tcl/Tk explicitly from the spec.
    return

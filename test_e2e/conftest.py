"""Present so pytest puts ``test_e2e/`` on ``sys.path``.

The suites under it have no ``__init__.py``, so without this file each suite
directory is importable but their shared modules (``job_wait``) are not.
"""

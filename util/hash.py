import hashlib

def hash_file(file_path: str) -> str:
    """Hash the contents of a file using SHA1, return as string."""
    with open(file_path, 'rb', buffering=0) as f:
        return hashlib.file_digest(f, 'sha1').hexdigest()

def hash_obj(obj: dict|list|str|None) -> str | None:
    """Hash an object using SHA1, return as string."""
    if obj is None:
        return None
    if isinstance(obj, dict) or isinstance(obj, list):
        obj = str(obj)
    return hashlib.sha1(obj.encode()).hexdigest()

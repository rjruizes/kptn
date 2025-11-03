from pathlib import Path
import re
import os
from os import path
from kptn.util.filepaths import project_root
from kptn.util.hash import hash_obj


def read_r_file(file_path):
    with open(file_path, "r") as file:
        return file.read()


def find_here_root(start_path: str) -> str:
    """
    Find the root directory by looking for a .here file in parent directories.
    Starts from the given path and moves up the directory tree.
    Returns the directory containing .here file, or project_root as fallback.
    """
    current_path = Path(start_path).resolve()
    
    # If start_path is a file, start from its parent directory
    if current_path.is_file():
        current_path = current_path.parent
    
    # Search up the directory tree for .here file
    for parent in [current_path] + list(current_path.parents):
        here_file = parent / ".here"
        if here_file.exists():
            return str(parent)
    
    # Fallback to project_root if no .here file found
    return project_root


def get_import_list(file_path: str) -> list[str]:
    """
    Search for all import statements in an R script file and return them as a list of file paths.
    """
    base_dir = path.dirname(file_path)
    file_content = read_r_file(file_path)
    source_literal_imports = re.findall(r'source\("(.*?)"\)', file_content)
    source_here_imports = re.findall(r'source\(.*here\("(.*?)"\)\)', file_content)
    rscript_calls = re.findall(r'r_script\("(.*?)"\)', file_content)
    # for all source_literal_imports, resolve relative to base_dir
    source_literal_imports = [
        path.join(base_dir, import_file) for import_file in source_literal_imports
    ]
    # for all source_here_imports, resolve relative to here root (directory containing .here file)
    here_root = find_here_root(file_path)
    source_here_imports = [
        path.join(here_root, import_file) for import_file in source_here_imports
    ]
    # for all rscript_calls, resolve relative to base_dir
    rscript_calls = [
        path.join(base_dir, import_file) for import_file in rscript_calls
    ]
    return source_literal_imports + source_here_imports + rscript_calls


class RImportFinder:
    def __init__(self):
        self.cache = {}  # {file_path: [imports]}

    def search(self, file_path: str) -> list[str]:
        """
        Given the file path of an R script, return a list of all imported R files.
        """
        imports = get_import_list(file_path)
        self.cache[file_path] = imports
        # print(f"File {file_path} imports {imports}")
        child_imports = []

        for import_file in imports:
            if os.path.exists(import_file):
                if import_file not in self.cache:
                    child_imports += self.search(import_file)
            else:
                print(f"File {import_file} does not exist")

        return imports + child_imports


def get_file_list(file_paths: list[Path]) -> list[str]:
    """
    Given the file path of an R script, return a list of all files imported, alphabetically sorted.
    """
    finder = RImportFinder()
    results = []
    for file_path in file_paths:
        file_path = str(file_path)
        results += [file_path] + finder.search(file_path)
    return sorted(list(set(results)))


def hash_r_files(file_paths: list[Path], base_dir: str) -> str:
    """
    Given the file path of an R script, return a hash of the contents of the file and all files it imports.
    """
    abs_file_list = get_file_list(file_paths)
    file_hashes_list = [
        { path.relpath(file, base_dir): hash_obj(read_r_file(file)) } for file in abs_file_list
    ]
    return file_hashes_list
    # code_version_dict = {
    #     file: hashlib.sha1(read_r_file(file).encode()).hexdigest()
    #     for file in filetree_list
    # }
    # return hashlib.sha1(str(code_version_dict).encode()).hexdigest()

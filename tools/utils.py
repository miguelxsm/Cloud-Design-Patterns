from infrastructure.constants import _REPO_ROOT
import os

def get_code(path: str):
    code_path = os.path.join(_REPO_ROOT, path)
    with open(code_path, "r") as f:
        code = f.read()
    return code


if __name__ == "__main__":
    code = get_code('deployment/setup_instances.sh')
    print(code)
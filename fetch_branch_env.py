import sys
import argparse

def get_env(branch: str):
    mapping = {
        "develop/dev": "dev",
        "develop/test": "test",
        "develop/uat": "uat",
        "main": "prod"
    }
    if not mapping.get(branch):
        raise Exception("invalid branch name. should be one of develop/dev,develop/test,develop/uat,main")
    print(mapping[branch])

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--branch", help="should be one of develop/dev,develop/test,develop/uat,main", metavar="",  required=True)
    args = parser.parse_args()
    get_env(args.branch)

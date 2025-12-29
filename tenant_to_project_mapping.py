import argparse

TENANT_TO_PROJECT_MAPPING = {
    "arhaus":"arhaus-401512",
    "saks":"saksfifthavenue-27032024"
}

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--tenant_name", help="pass the tenant-alias name", metavar="",  required=True)
    args = parser.parse_args()
    if args.tenant_name not in TENANT_TO_PROJECT_MAPPING.keys():
        raise Exception("The given tenant_name is missing from tenant_to_project_mapping file.")
    print(TENANT_TO_PROJECT_MAPPING[args.tenant_name])
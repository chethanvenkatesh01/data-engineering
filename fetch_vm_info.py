import sys
import argparse

SOURCING_PROJECT_MAPPING = {
    "signet": "data-ingestion-342714",
    "vb": "data-ingestion-342714",
    # "puma": "data-ingestion-342714",
    "carters": "carters-070424",
    "biglots": "data-ingestion-342714",
    "rl_eu": "data-ingestion-342714",
    "rl_na": "data-ingestion-342714",
    "tommy": "data-ingestion-342714",
    "ck": "data-ingestion-342714",
    "tapestry": "tapestry-10052024",
    "rl_eu_is": "data-ingestion-342714",
    "dg": "dollar-general-395209",
    "arhaus": "arhaus-401512",
    "saks": "saksfifthavenue-27032024",
    "vs": "victorias-secret-393308",
    "partycity_new": "data-ingestion-342714",
    "mns": "marksandspencer-414909",
    "mns_mena": "marksandspencer-414909",
    "sm":"steve-madden-280624",
    "briscoes" : "briscoes-01082024",
    "spanx" : "spanx-24042024",
    "cb": "crackerbarrel-16082024",
    "tb": "tommy-bahama-393308",
    "pm" : "peter-millar-260624",
    "vs_intl" : "victorias-secret-international",
    "toryburch" : "tory-burch-221024",
    "joann" : "joann-mtp-04112024",
    "bjs": "bjs-mtp-22112024",
    "tsc": "tractorsupply-29112024",
    "pacsun": "pacsun-09012025",
    "pg":"patagonia-27022025",
    "balsam":"balsambrands-20022025"
}

INGESTION_PROJECT_MAPPING = {
    "develop/dev": {
        "cb":"crackerbarrel-16082024",
        "signet": "impactsmart",
        "vb": "impactsmart",
        # "puma": "impactsmart",
        "biglots": "impactsmart",
        "rl_eu": "impactsmart",
        "rl_na": "impactsmart",
        "tommy": "impactsmart",
        "ck": "impactsmart",
        "rl_eu_is": "impactsmart",
        "partycity_new": "impactsmart",
        "tsc": "tractorsupply-29112024",
        "pacsun": "pacsun-09012025",
        "pg":"patagonia-27022025",
        "balsam":"balsambrands-20022025"
    },
    "develop/test": {
        "cb":"crackerbarrel-16082024",
        "signet": "impactsmart",
        "vb": "impactsmart",
        # "puma": "impactsmart",
        "biglots": "impactsmart",
        "rl_eu": "impactsmart",
        "rl_na": "impactsmart",
        "tommy": "impactsmart",
        "ck": "impactsmart",
        "rl_eu_is": "impactsmart",
        "partycity_new": "impactsmart"
    },
    "develop/uat": {
        "signet": "impactsmart-prod",
        "vb": "impactsmart-prod",
        "biglots": "impactsmart-prod",
        "rl_eu": "impactsmart-prod",
        "rl_na": "impactsmart-prod",
        "tommy": "impactsmart-prod",
        "ck": "impactsmart-prod",
        "rl_eu_is": "impactsmart-prod",
        "partycity_new": "impactsmart-prod"
    },
    "main": {
        "signet": "impactsmart-prod",
        "vb": "impactsmart-prod",
        "biglots": "impactsmart-prod",
        "rl_eu": "impactsmart-prod",
        "rl_na": "impactsmart-prod",
        "tommy": "impactsmart-prod",
        "ck": "impactsmart-prod",
        "rl_eu_is": "impactsmart-prod",
        "partycity_new": "impactsmart-prod"
    },
    "carters": "carters-070424",
    "tapestry": "tapestry-10052024",
    "dg": "dollar-general-395209",
    "arhaus": "arhaus-401512",
    "saks": "saksfifthavenue-27032024",
    "vs": "victorias-secret-393308",
    "mns": "marksandspencer-414909",
    "mns_mena": "marksandspencer-414909",
    "sm":"steve-madden-280624",
    "cb": "crackerbarrel-16082024",
    "tb": "tommy-bahama-393308",
    "briscoes" : "briscoes-01082024",
    "spanx" : "spanx-24042024",
    "pm" : "peter-millar-260624",
    "vs_intl" : "victorias-secret-international",
    "toryburch" : "tory-burch-221024",
    "joann" : "joann-mtp-04112024",
    "bjs": "bjs-mtp-22112024",
    "tsc": "tractorsupply-29112024",
    "pacsun": "pacsun-09012025",
    "pg":"patagonia-27022025"
}

def get_ip(branch: str, tenant: str, pipeline: str):
    try:
        if pipeline.lower() == "ingestion":
            print(INGESTION_PROJECT_MAPPING.get(tenant, None) or INGESTION_PROJECT_MAPPING.get(branch, {}).get(tenant, None))
        elif pipeline.lower() == "sourcing":
            print(SOURCING_PROJECT_MAPPING.get(tenant, None))
        else:
            raise Exception(f"Invalid pipeline type: {pipeline}")
    except Exception as e:
        print(f"IP not found. Error: {str(e)}")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--branch", help="should be one of develop/dev,develop/test,develop/uat,main", metavar="",  required=True)
    parser.add_argument("--tenant", help="tenant id", metavar="",  required=True)
    parser.add_argument("--pipeline", help="pipeline should be one of sourcing, ingestion", metavar="",  required=True)
    args = parser.parse_args()
    get_ip(args.branch, args.tenant, args.pipeline)

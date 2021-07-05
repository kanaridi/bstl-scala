# BSTL APP FLOWS
The flows in this folder are directly tied to the processing of the generated RAW data in this repro.

These flows are meant to be run by developers but unfortunoutly contain a huge amount of connections and logic that 
make it hard to edit every file to point to your data sets. To make things easier, there is a `buildFlows.sh` script
that pulls values out of your `config.env` file and generates json files in an `./out` directory  

### Generating Flows
First step is to build your modify your `config.env` file to point to your dev env folders. it should look like this:
```
RAW_ADL_ACCOUNT="kdpoc.dfs.core.windows.net"
RAW_ADL_CONTAINER="dev-raw"
RAW_ADL_FOLDER="/user/alice/"

PREP_ADL_ACCOUNT="kdpoc.dfs.core.windows.net"
PREP_ADL_CONTAINER="dev-prep"
PREP_ADL_FOLDER="/user/alice/"

EVENTS_ADL_ACCOUNT="kdpoc.dfs.core.windows.net"
EVENTS_ADL_CONTAINER="dev-biz-events"
EVENTS_ADL_FOLDER="/user/alice/orderbook/"

UBER_OBJECT_RULE_PATH="abfss://synapse-fs@kdpoc2.dfs.core.windows.net/bob/rules/uber_with_rules.py"

SQL_URL="jdbc:sqlserver://kd-sql2019.database.windows.net:1433;database=kd-dev;"
SQL_USERNAME="ktech"
SQL_PASSWORD="pass"
```

After you have a config file you can run the build script which will replace sections of the json files with your values.
ie, `<RAW_ADL_CONTAINER>` will be replaced with `dev-raw` with the above `config.env` files
```
. buildFlows.sh config.env
```
### Running any flow on Synapse
Use venv to create your virtual environment from the root of this project
```
python3 -m venv ./venv
source ./venv/bin/activate
python3 -m pip install -r py_requirements.txt
```

Establish cli authentication with Azure
```
az login
```

Now edit your config.env file and set your AZ_ variables appropriately for the artifacts you are trying to run.
```
python3 synapse.py -name myPersonalSession -flow ./out/fromRaw/acme_asn_to_landed.json -op acme_asn_to_landed
```
NOTE:At any time you can stop pointing to the virutual env by running `deactivate` on the command line


## fromRaw
The `fromRaw` folder has all the json flows to process raw data in azure datalake and move it to the prepared space. You can simply copy the contents
of the `/raw-data/out` folder (after you build it) to your place in the data lake.


## fromPrepared
The `fromPrepared` folder has all the json flows to process cleaned and prepped data in azure datalake (no dupes/etc) that were generated in `fromRaw`.

## fromEvents
These generate uber events and run high level script rules. The config file has `UBER_OBJECT_RULE_PATH="abfss://synapse-fs@kdpoc2.dfs.core.windows.net/minh/rules/uber_with_rules.py"` which points to the scripts
location that we would replace and run on the uber rules.

Copytight 2021 Kanari Digital, Inc.

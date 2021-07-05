#!/bin/bash
# Function to display commands

function exe() {
  echo "\$> $*" ; "$@" ;
}
function usage() {
    echo ""
    echo "USAGE:   . buildFlows.sh someconfig.env"
}
function escape() {
  printf '%s\n' "$@" | sed -e 's/[]\/$*.^[]/\\&/g';
}
function b64() {
  base64 "$@"
}
function e() {
  echo ""
  echo "## $*"
}

if [ "$1" = "" ]
then
  usage
  file=config.env
else
  file=$1
fi

e Loading config from $file
source $file || return

echo COMMON_ADL_ACCOUNT=$COMMON_ADL_ACCOUNT
echo RAW_ADL_ACCOUNT=$RAW_ADL_ACCOUNT
echo RAW_ADL_CONTAINER=$RAW_ADL_CONTAINER
echo RAW_ADL_FOLDER=$RAW_ADL_FOLDER
echo RAW_PREPROCESSOR_SINCE=$RAW_PREPROCESSOR_SINCE
echo RAW_PREFETCH_FROM=$RAW_PREFETCH_FROM
echo RAW_PREFETCH_TO=$RAW_PREFETCH_TO
echo PREP_ADL_ACCOUNT=$PREP_ADL_ACCOUNT
echo PREP_ADL_CONTAINER=$PREP_ADL_CONTAINER
echo PREP_ADL_FOLDER=$PREP_ADL_FOLDER
echo EVENTS_ADL_ACCOUNT=$EVENTS_ADL_ACCOUNT
echo EVENTS_ADL_CONTAINER=$EVENTS_ADL_CONTAINER
echo EVENTS_ADL_FOLDER=$EVENTS_ADL_FOLDER
echo UBER_OBJECT_RULE_LOCATION=$UBER_OBJECT_RULE_LOCATION
echo UBER_OBJECT_RULE_PATH=$UBER_OBJECT_RULE_PATH
echo UBER_OBJECT_RULE_SCRIPT=$UBER_OBJECT_RULE_SCRIPT
echo SQL_URL=$SQL_URL
echo SQL_USERNAME=$SQL_USERNAME
echo SQL_PASSWORD=$SQL_PASSWORD

e Nuking out directory
exe rm -rf out
mkdir -p out/fromRaw || return
mkdir -p out/fromPrepared || return
mkdir -p out/fromEvents || return

e Copying json flows and rules python to out directory
exe cp ./fromRaw/*.json ./out/fromRaw/ || return
exe cp ./fromPrepared/*.json ./out/fromPrepared/ || return
exe cp ./fromEvents/*.json ./out/fromEvents/ || return
exe cp $UBER_OBJECT_RULE_SCRIPT ./out/fromEvents/ || return

e 'Replacing json file "variables" with your config values'
sed -i -E "s/<COMMON_ADL_ACCOUNT>/$(escape $COMMON_ADL_ACCOUNT)/g" ./out/*/*.json || return
sed -i -E "s/<RAW_ADL_ACCOUNT>/$(escape $RAW_ADL_ACCOUNT)/g" ./out/*/*.json || return
sed -i -E "s/<RAW_ADL_CONTAINER>/$(escape $RAW_ADL_CONTAINER)/g" ./out/*/*.json || return
sed -i -E "s/<RAW_ADL_FOLDER>/$(escape $RAW_ADL_FOLDER)/g" ./out/*/*.json || return
sed -i -E "s/<RAW_PREPROCESSOR_SINCE>/$(escape $RAW_PREPROCESSOR_SINCE)/g" ./out/*/*.json || return
sed -i -E "s/<RAW_PREFETCH_FROM>/$(escape $RAW_PREFETCH_FROM)/g" ./out/*/*.json || return
sed -i -E "s/<RAW_PREFETCH_TO>/$(escape $RAW_PREFETCH_TO)/g" ./out/*/*.json || return

sed -i -E "s/<PREP_ADL_ACCOUNT>/$(escape $PREP_ADL_ACCOUNT)/g" ./out/*/*.json || return
sed -i -E "s/<PREP_ADL_CONTAINER>/$(escape $PREP_ADL_CONTAINER)/g" ./out/*/*.json || return
sed -i -E "s/<PREP_ADL_FOLDER>/$(escape $PREP_ADL_FOLDER)/g" ./out/*/*.json || return

sed -i -E "s/<EVENTS_ADL_ACCOUNT>/$(escape $EVENTS_ADL_ACCOUNT)/g" ./out/*/*.json || return
sed -i -E "s/<EVENTS_ADL_CONTAINER>/$(escape $EVENTS_ADL_CONTAINER)/g" ./out/*/*.json || return
sed -i -E "s/<EVENTS_ADL_FOLDER>/$(escape $EVENTS_ADL_FOLDER)/g" ./out/*/*.json || return

exe sed -i -E "s/<SQL_URL>/$(escape $SQL_URL)/g" ./out/*/*.json || return
sed -i -E "s/<SQL_USERNAME>/$(escape $SQL_USERNAME)/g" ./out/*/*.json || return
sed -i -E "s/<SQL_PASSWORD>/$(escape $SQL_PASSWORD)/g" ./out/*/*.json || return


sed -i -E "s/<UBER_OBJECT_RULE_LOCATION>/$(escape $UBER_OBJECT_RULE_LOCATION)/g" ./out/*/*.json || return
if [ "$UBER_OBJECT_RULE_LOCATION" = "base64" ]; then
e "Inserting python rules from $UBER_OBJECT_RULE_SCRIPT into sections with <UBER_OBJECT_RULE_SCRIPT>"
sed -i -E "s/<UBER_OBJECT_RULE_SCRIPT>/$(b64 $UBER_OBJECT_RULE_SCRIPT)/g" ./out/*/*.json || return
sed -i -E "s/<UBER_OBJECT_RULE_PATH>//g" ./out/*/*.json || return
elif [ "$UBER_OBJECT_RULE_LOCATION" = "hdfs" ]; then
e "Using path $UBER_OBJECT_RULE_PATH for python rules. Ensure you place $UBER_OBJECT_RULE_SCRIPT there!"
sed -i -E "s/<UBER_OBJECT_RULE_PATH>/$(escape $UBER_OBJECT_RULE_PATH)/g" ./out/*/*.json || return
sed -i -E "s/<UBER_OBJECT_RULE_SCRIPT>//g" ./out/*/*.json || return
else
e "ERROR: UNKNOWN UBER_OBJECT_RULE_LOCATION $UBER_OBJECT_RULE_LOCATION"
fi

e "Checking Results"
grep -r "<.*>" ./out && echo BUILD FAILED TO REPLACE ALL PARAMETERS

e "Done"









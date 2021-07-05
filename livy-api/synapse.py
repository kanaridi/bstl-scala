import os
import sys
import subprocess
import pathlib
from dotenv import load_dotenv
import json
import time
import requests
import urllib



load_dotenv(os.path.join('.', 'config.env'))

VERBOSE=True
AZ_SUBSCRIPTION=os.getenv("AZ_SUBSCRIPTION_ID")
AZ_WORKSPACE=os.getenv("AZ_WORKSPACE")
AZ_POOL=os.getenv("AZ_POOL")
AZ_EXECSIZE=os.getenv("AZ_EXECSIZE")
AZ_EXECCOUNT=os.getenv("AZ_EXECCOUNT")
AZ_COMMON_PATH=os.getenv("AZ_COMMONJAR_PATH")
AZ_BST_JAR=os.getenv("AZ_BSTJAR")
az_token = None

COMMON_JARS = [
    "jython-standalone-2.7.2b2.jar",
    "kafka-clients-2.4.0.jar",
    "kafka-log4j-appender-2.0.0.3.1.0.0-78.jar"
]



def log(msg):
    if VERBOSE:
        print(msg)

def abs_path(path):
    if path == "" or path.startswith("/"):
        return path
    else:
        return str(pathlib.Path(__file__).parent.absolute()) + "/" + path

def az_request(type, pool, endpoint, params, body):
    global az_token
    if az_token == None:
        az_token = get_token()

    url = f"https://{AZ_WORKSPACE}.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/{pool}/{endpoint}"
    response = None
    headers = {f"Authorization": f"Bearer {az_token}",
               "Content-Type":"application/json"}

    if type == "GET":
        response = requests.get(url, params, headers=headers)
    elif type == "POST":
        response = requests.post(url, json=body, headers=headers)
    content = response.content.decode('utf-8')

    try:
        content_json = json.loads(content)
    except:
        content_json = None

    if not response.ok:
        print(f"Error in url request:{response.url}")
        print(f"{response.status_code}:{response.reason}")
        if content_json != None:
            print(json.dumps(content_json, indent=2))
        return None

    return content_json

def get_token():
    res = az_run("az", "account", "get-access-token", "--resource", "https://dev.azuresynapse.net")
    token = res["accessToken"]
    return token

def az_run(cmd, *params):
    a = []
    a.append([cmd])
    a.append(params)
    a.append(["-o", "json"])
    runner = [x for xs in a for x in xs]
    str = "$>"
    for i in runner:
        str = f"{str} {i}"
    log(str)

    # strangely if PYCHARM_HOSTED IS SET, the cli has issues
    try:
        os.environ.pop("PYCHARM_HOSTED")
    except:
        "ignore"

    result = subprocess.run(runner, capture_output=True)
    if result.stderr != None and "WARNING: Command group" not in result.stderr.decode('utf-8'):
        print(result.stderr.decode('utf-8'))
    json_str = result.stdout.decode('utf-8')

    return json.loads(json_str)

def start_session(workspace, pool, session_name, common, bst):
    jar_paths = list(map(lambda jar: f'{common}/{jar}', COMMON_JARS))
    jar_paths.append(f'{bst}')
    body = {
        "name": session_name,
        "jars": jar_paths,
        #"conf": {"spark.jars.packages":PACKAGES},
        "driverMemory": "4g",
        "driverCores": 2,
        "executorMemory": "2g",
        "executorCores": 2,
        "numExecutors": 2
    }

    session = az_request("POST", pool, "sessions", None, body)
    return session

def session_state(workspace, pool, sessionId):
    session = az_request("GET", pool, f"sessions/{sessionId}", None, None)
    return session

def get_session(workspace, pool, sessionName):
    go = True
    seshOffset = 0
    while go:
        go = False
        progress('.')
        res_json = az_request("GET", pool, "sessions", {"detailed":True, "from": seshOffset, "size": 20}, None)
        if res_json == None:
            exit(1)
        seshOffset = seshOffset + len(res_json['sessions'])
        for sesh in res_json['sessions']:
            go = True
            if(sesh['name'] == sessionName and sesh['state'] != "killed" and sesh['state'] != "dead" and sesh['state'] != "error"):
                progress_end()
                return sesh

    progress_end()
    return None

def wait_for_session_idle(workspace, pool, session_id, call_wait):
    check = True
    last_state = ""
    while check:
        check = False
        state = session_state(workspace, pool, session_id)

        # print_json(state)

        if state["state"] == "error" or state["state"] == "killed":
            print(f"ERROR IN ${state['state']} STATE")
            print_json(state)
            return None
        elif state["state"] != "idle":
            progress(f"state={state['state']}..") if state["state"] != last_state else progress('.')
            check = True
            last_state = state["state"]
            time.sleep(call_wait)

    progress_end()
    return state


def run_statement(workspace, pool, session_id, statement):
    res = az_request("POST", pool, f"sessions/{session_id}/statements", None,  {"code":statement,"kind":"scala"})
    return res

def statement_state(workspace, pool, session_id, statement_id):
    res = az_request("GET", pool, f"sessions/{session_id}/statements/{statement_id}", None, None)
    return res

def wait_for_statement_finish(workspace, pool, session_id, statement_id, call_wait):
    check = True
    last_state = ""
    while check:
        check = False
        state = statement_state(workspace, pool, session_id, statement_id)
        if state == None:
            print("ERROR Getting Statement State")
            return None
        elif state["state"] != "available":
            progress(f"state={state['state']}...") if state["state"] != last_state else progress('.')
            check = True
            last_state = state["state"]
            time.sleep(call_wait)
        else:
            exec_state = state["output"]["status"]

    progress_end()
    if exec_state != "ok":
        print("ERROR RUNNING STATEMENT")
        print_json(state)
        return None

    return state


def prepare_flow_statement(flow_file, operator, write, result_count):
    read_data = ""
    #convert to the scala version of true/false
    nowrite = "false" if write else "true"

    with open(flow_file) as f:
        read_data = f.read()

    if read_data == "":
        print(f"ERROR: nothing read from {flow_file}")
        return None

    #line1 = f"val flowJson:String = \"\"\"{read_data}\"\"\";"
    #line2 = f"val flowResult = com.kanaridi.rest.Service.runFlowV2(flowJson, \"{operator}\", 50, true, false, false);"
    #line3 = f"println(flowResult);"
    #return line1 + line2 + line3

    #the above didn't work because you get results for lines seemingly out of order and running without println truncates the results.
    #needed to run this as a single line to be able to capture the results properly.
    line = f"println(com.kanaridi.rest.Service.runFlowV2(\"\"\"{read_data}\"\"\", \"{operator}\", {result_count}, {nowrite}, false, false));"
    return line

def make_log_url(subscription, workspace, pool, session_name, session_id):
    sessionstr = f"/subscriptions/{subscription}/resourceGroups/{workspace}/providers/Microsoft.Synapse/workspaces/{workspace}"
    encoded = urllib.parse.quote(sessionstr, safe='')
    return f"https://web.azuresynapse.net/en-us/monitoring/sparkapplication/{session_name}?workspace={encoded}&livyId={session_id}&sparkPoolName={pool}"


def progress(msg):
    sys.stdout.write((msg))
    sys.stdout.flush()

def progress_end():
    sys.stdout.write('\n')

def print_json(json_input):
    if isinstance(json_input, str):
        json_dict = json.loads(json_input)
    else:
        json_dict = json_input

    print(json.dumps(json_dict, indent=2))

def usage():
    print("Usage: python synapse.py -name <sessionName> -flow <path_to_flow> -op <op_to_run> [-pool <sparkPool> -execSize <Small|Medium|Large> -execCount <num_executors>]")
    print("    Variables can also be defined in config.env for permanenet usage")


if __name__ == "__main__":
    name, flow_path, operator = "", "", ""
    write = False
    result_count = 5
    x = 1
    while x < len(sys.argv):
        p = sys.argv[x]
        if p == "-name" or p == "-Name":
            x=x+1
            name=sys.argv[x]
        elif p == "-exesize" or p == "-exeSize" or p == "-ExeSize":
            x=x+1
            AZ_EXECSIZE = sys.argv[x]
        elif p == "-execount" or p == "-exeCount" or p == "-ExeCount":
            x = x + 1
            AZ_EXECCOUNT = int(sys.argv[x])
        elif p == "-pool" or p == "-Pool":
            x = x + 1
            AZ_POOL = int(sys.argv[x])
        elif p == "-keep" or p == "-Keep" or p == "-KEEP":
            clean = False
        elif p == "-flow" or p == "-Flow":
            x = x + 1
            flow_path = sys.argv[x]
        elif p == "-op" or p == "-Op":
            x = x + 1
            operator = sys.argv[x]
        elif p == "-count" or p == "-Count":
            x = x + 1
            result_count = sys.argv[x]
        elif p == "-write" or p == "-Write":
            write = True
        else:
            print(f"Invalid parameter {sys.argv[x]}")
            usage()
            exit()
        x=x+1

    if (name == "" or flow_path == "" or operator == "" or
            AZ_SUBSCRIPTION=="" or AZ_WORKSPACE=="" or AZ_POOL == "" or
            AZ_EXECSIZE=="" or AZ_EXECCOUNT=="" or AZ_COMMON_PATH=="" or
            AZ_BST_JAR=="" or az_token ==""):
        usage()
        exit()

    print('Argument List:', str(sys.argv))
    print(f'  Workspace:{AZ_WORKSPACE}, Spark Pool:{AZ_POOL}, Exec Size:{AZ_EXECSIZE}, Exec Count:{AZ_EXECCOUNT}')
    print(f'Session Name: {name},  BST: {AZ_BST_JAR},  Common: {AZ_COMMON_PATH}\n')

    #doing this up front so we can error out quick if we don't find the file.
    flow_statement = prepare_flow_statement(flow_path, operator, write, result_count)
    if flow_statement == None:
        exit(1)

    print(f"FINDING ACTIVE SESSION NAMED '{name}'")
    session = get_session(AZ_WORKSPACE, AZ_POOL, name)
    if (session == None or session["result"]=="Failed"):
        session = start_session(AZ_WORKSPACE, AZ_POOL, name, AZ_COMMON_PATH, AZ_BST_JAR)
        if (session == None):
            print("ERROR: Session could not start")
            exit(1)
        print(f"NOTHING FOUND. STARTED NEW SESSION: id:{session['id']}, state:{session['state']}")
    else:
        print(f"SESSION FOUND: name:{session['name']}, id:{session['id']}, appId:{session['appId']}, state:{session['state']}")


    session_id = session["id"]
    print(f"LOG URL: {make_log_url(AZ_SUBSCRIPTION, AZ_WORKSPACE, AZ_POOL, name, session_id)}\n\n")

    if session == None:
        print("ERROR: Session was None!")
        exit(1)

    print("WAITING FOR SESSION IDLE")
    state = wait_for_session_idle(AZ_WORKSPACE, AZ_POOL, session_id, 5)
    if state != None:
        print(f"SESSION READY: id:{state['id']}, appId:{state['appId']}")
    else:
        print("ERROR WAITING FOR SESSION IDLE")
        exit(1)

    result = run_statement(AZ_WORKSPACE, AZ_POOL, session_id, flow_statement)
    statement_id = result["id"]
    print(f"STATEMENT SENT: id:{statement_id}, state:{result['state']}")
    print("WAITING FOR STATEMENT TO FINISH EXECUTING")
    stmnt = wait_for_statement_finish(AZ_WORKSPACE, AZ_POOL, session_id, statement_id, 5)
    if stmnt == None:
        print("ERROR waiting for statement")
        exit(1)

    print("RETURN FROM BST JOB:")
    bst_result = stmnt["output"]["data"]["text/plain"]


    if ("\"errors\":" in bst_result):
        print("EXITING WITH ERROR FROM BST JOB")
        try:
            print_json(bst_result)
        except:
            print(bst_result)
        exit(1)
    else:
        print(bst_result)


    #stderr = "https://kdpoc-ms.dev.azuresynapse.net/sparkhistory/api/v1/sparkpools/kdflowtestpool/livyid/88/applications/application_1611795399816_0004/driverlog/stderr/?offset=2388740"

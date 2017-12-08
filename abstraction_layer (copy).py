########################################
# Abstraction Layer
# Milad Abbaszadeh
# milad.abbaszadehjahromi@campus.tu-berlin.de
# October 2017
# Big Data Management Group
# TU Berlin
# All Rights Reserved
########################################


########################################
import os
import json
import re
import csv
import subprocess
########################################


########################################
DATASETS_FOLDER = "datasets"
TOOLS_FOLDER = "tools"
########################################


########################################
def install_tools():
    """
    This method installs and configures the data cleaning tools.
    """
    for tool in os.listdir(TOOLS_FOLDER):
        if tool == "NADEEF":
            p = subprocess.Popen(["ant", "all"], cwd="{}/NADEEF".format(TOOLS_FOLDER), stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            p.communicate()
            print "To configure NADEEF, please follow the following steps:"
            print "1. Create a database entitled 'naadeef' in the postgres."
            postgress_username = raw_input("2. Inter your postgres username: ")
            postgress_password = raw_input("3. Inter your postgres password: ")
            nadeef_configuration_file = open("{}/NADEEF/nadeef.conf".format(TOOLS_FOLDER), "r")
            nadeef_configuration = nadeef_configuration_file.read()
            nadeef_configuration = re.sub("(database.username = )([\w\d]+)", "\g<1>{}".format(postgress_username),
                                          nadeef_configuration, flags=re.IGNORECASE)
            nadeef_configuration = re.sub("(database.password = )([\w\d]+)", "\g<1>{}".format(postgress_password),
                                          nadeef_configuration, flags=re.IGNORECASE)
            nadeef_configuration_file.close()
            nadeef_configuration_file = open("{}/NADEEF/nadeef.conf".format(TOOLS_FOLDER), "w")
            nadeef_configuration_file.write(nadeef_configuration)
        print "{} is installed.".format(tool)
########################################


########################################
def read_csv_dataset(dataset_path):
    """
    The method reads a dataset from a csv file path.
    """
    dataset_file = open(dataset_path, "r")
    dataset_reader = csv.reader(dataset_file, delimiter=",")
    dataset_header = []
    dataset_matrix = []
    for i, row in enumerate(dataset_reader):
        row = [x.strip(" ") if x.lower() != "null" else "" for x in row]
        if i == 0:
            dataset_header = row
        else:
            dataset_matrix.append(row)
    return dataset_header, dataset_matrix


def write_csv_dataset(dataset_path, dataset_header, dataset_matrix):
    """
    The method writes a dataset to a csv file path.
    """
    dataset_file = open(dataset_path, "w")
    dataset_writer = csv.writer(dataset_file, delimiter=",")
    dataset_writer.writerow(dataset_header)
    for row in dataset_matrix:
        dataset_writer.writerow(row)
########################################


########################################
def run_dboost(dataset_path, dboost_parameters):
    """
    This method runs dBoost on a dataset.
    """
    command = ["./{}/dBoost/dboost/dboost-stdin.py".format(TOOLS_FOLDER), "-F", ",", dataset_path] + dboost_parameters
    p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.communicate()
    return_list = []
    tool_results_path = "dboost_results.csv"
    if os.path.exists(tool_results_path):
        tool_results_file = open(tool_results_path, "r")
        csv_reader = csv.reader(tool_results_file, delimiter=",")
        cell_visited_flag = {}
        for row in csv_reader:
            i = int(row[0])
            j = int(row[1])
            v = row[2]
            if (i, j) not in cell_visited_flag and i > 0:
                cell_visited_flag[(i, j)] = 1
                return_list.append([i, j, v])
        tool_results_file.close()
        os.remove(tool_results_path)
    return return_list


def run_nadeef(dataset_path, nadeef_parameters):
    """
    This method runs NADEEF on a dataset.
    """
    dataset_header, dataset_matrix = read_csv_dataset(dataset_path)
    temp_dataset_path = os.path.abspath("nadeef_temp_dataset.csv")
    new_header = [a + " varchar(20000)" for a in dataset_header]
    write_csv_dataset(temp_dataset_path, new_header, dataset_matrix)
    column_index = {a: dataset_header.index(a) for a in dataset_header}
    nadeef_clean_plan = {
        "source": {
            "type": "csv",
            "file": [temp_dataset_path]
        },
        "rule": nadeef_parameters
    }
    nadeef_clean_plan_path = "nadeef_clean_plan.json"
    nadeef_clean_plan_file = open(nadeef_clean_plan_path, "w")
    json.dump(nadeef_clean_plan, nadeef_clean_plan_file)
    nadeef_clean_plan_file.close()
    p = subprocess.Popen(["./nadeef.sh"], cwd="{}/NADEEF".format(TOOLS_FOLDER), stdout=subprocess.PIPE,
                         stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
    process_output, process_errors = p.communicate("load ../../nadeef_clean_plan.json\ndetect\nexit\n")
    os.remove(nadeef_clean_plan_path)
    return_list = []
    tool_results_path = re.findall("INFO: Export to (.*csv)", process_output)[0]
    tool_results_file = open(tool_results_path, "r")
    csv_reader = csv.reader(tool_results_file, delimiter=",")
    cell_visited_flag = {}
    for row in csv_reader:
        i = int(row[3])
        j = column_index[row[4]]
        v = row[5]
        if (i, j) not in cell_visited_flag:
            cell_visited_flag[(i, j)] = 1
            return_list.append([i, j, v])
    tool_results_file.close()
    os.remove(tool_results_path)
    os.remove(temp_dataset_path)
    return return_list


def run_openrefine(dataset_path, openrefine_parameters):
    """
    This method runs OpenRefine on a dataset.
    """
    dataset_header, dataset_matrix = read_csv_dataset(dataset_path)
    return_list = []
    cell_visited_flag = {}
    for i, row in enumerate(dataset_matrix):
        for j, pattern in openrefine_parameters:
            if not re.findall(pattern, row[j], re.IGNORECASE):
                if (i + 1, j) not in cell_visited_flag:
                    cell_visited_flag[(i + 1, j)] = 1
                    return_list.append([i + 1, j, row[j]])
    return return_list
########################################

def run_katara(dataset_path, kb_path):

    """
    This method runs KATARA on a dataset.
    """

    print "datapath:"+dataset_path
    print "kbpath:"+str(kb_path)



    command="$JAVA_HOME/bin/java -javaagent:./tools/KATARA/jar_files/idea_rt.jar=34583:./tools/KATARA/jar_files -Dfile.encoding=UTF-8 -classpath $JAVA_HOME/jre/lib/charsets.jar:$JAVA_HOME/jre/lib/ext/cldrdata.jar:$JAVA_HOME/jre/lib/ext/dnsns.jar:$JAVA_HOME/jre/lib/ext/icedtea-sound.jar:$JAVA_HOME/jre/lib/ext/jaccess.jar:$JAVA_HOME/jre/lib/ext/localedata.jar:$JAVA_HOME/jre/lib/ext/nashorn.jar:$JAVA_HOME/jre/lib/ext/sunec.jar:$JAVA_HOME/jre/lib/ext/sunjce_provider.jar:$JAVA_HOME/jre/lib/ext/sunpkcs11.jar:$JAVA_HOME/jre/lib/ext/zipfs.jar:$JAVA_HOME/jre/lib/jce.jar:$JAVA_HOME/jre/lib/jsse.jar:$JAVA_HOME/jre/lib/management-agent.jar:$JAVA_HOME/jre/lib/resources.jar:$JAVA_HOME/jre/lib/rt.jar:./tools/KATARA/out/test/test:./tools/KATARA/KATARA/test/src/SimplifiedKATARA.jar:./tools/KATARA/KATARA/test/bin/SimplifiedKATARA.jar:./tools/KATARA/jar_files/idea_rt.jar:./tools/KATARA/jar_files/SimplifiedKATARA.jar:./tools/KATARA/KATARA/out/test/test/SimplifiedKATARA.jar simplied.katara.SimplifiedKATARAEntrance"

    p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.STDOUT, stderr=subprocess.STDOUT,shell=True)
    # p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.STDOUT, stderr=subprocess.STDOUT,cwd="{}/KATARA".format(TOOLS_FOLDER),shell=True)

    #process_output, process_errors=p.communicate(dataset_path+"\n"+kb_path+"\n")
    process_output, process_errors = p.communicate("/home/milad/Desktop/abstraction-layer/datasets/country4.csv"+"\n" + "/home/milad/Desktop/abstraction-layer/tools/KATARA/dominSpecific" + "\n")

    print "hhhhhhhhhhhhhhhhhhh"
    print "error"+str(process_errors)
    print "out"+process_output

    return "hi"




########################################
def run_data_cleaning_job(data_cleaning_job):
    """
    This method runs the data cleaning job based on the input configuration.
    """
    dataset_path = ""
    if data_cleaning_job["dataset"]["type"] == "csv":
        dataset_path = os.path.abspath(data_cleaning_job["dataset"]["param"][0])

    return_list = []
    if data_cleaning_job["tool"]["name"] == "dboost":
        return_list = run_dboost(dataset_path, data_cleaning_job["tool"]["param"])
    if data_cleaning_job["tool"]["name"] == "nadeef":
        return_list = run_nadeef(dataset_path, data_cleaning_job["tool"]["param"])
    if data_cleaning_job["tool"]["name"] == "openrefine":
        return_list = run_openrefine(dataset_path, data_cleaning_job["tool"]["param"])

    if data_cleaning_job["tool"]["name"] == "katara":
        return_list = run_katara(dataset_path,os.path.abspath(data_cleaning_job["tool"]["param"][0]))

    return return_list
########################################


########################################
if __name__ == "__main__":

    # install_tools()

    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
    #     },
    #     "tool": {
    #         "name": "dboost",
    #         "param": ["--gaussian", "1", "--statistical", "1"]
    #         }
    # }


    run_input = {
        "dataset": {
            "type": "csv",
            "param": ["./datasets/country4.csv"]
        },
        "tool": {
            "name": "katara",
            "param": ["./tools/KATARA/dominSpecific"] #address of your KB file
            }
    }

    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
    #     },
    #     "tool": {
    #         "name": "nadeef",
    #         "param": [{"type": "fd", "value": ["title | brand_name"]}]
    #     }
    # }

    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
    #     },
    #     "tool": {
    #         "name": "openrefine",
    #         "param": [(4, "^[\d]+$"), (7, "^[\w]+$")]
    #     }
    # }

    results_list = run_data_cleaning_job(run_input)
    # for x in results_list:
    #     print x
########################################

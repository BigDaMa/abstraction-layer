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
TOOLS_LIST = os.listdir(TOOLS_FOLDER)
########################################


########################################
def install_tools():
    for tool in TOOLS_LIST:
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
def abstract_layer(input_config):
    if run_input["tool"]["name"] == "dboost":
        command = ["./{}/dBoost/dboost/dboost-stdin.py".format(TOOLS_FOLDER), "-F", ",",
                   input_config["dataset"]["path"]] + input_config["tool"]["param"]
        p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        p.communicate()
        return_list = []
        tool_results_path = "results.csv"
        if os.path.exists(tool_results_path):
            tool_results_file = open(tool_results_path, "r")
            csv_reader = csv.reader(tool_results_file, delimiter=",")
            cell_visited_flag = {}
            for row in csv_reader:
                i = int(row[0])
                j = int(row[1])
                v = row[2]
                if (i, j) not in cell_visited_flag:
                    cell_visited_flag[(i, j)] = 1
                    return_list.append([i, j, v])
            tool_results_file.close()
            os.remove(tool_results_path)
        return return_list

    if run_input["tool"]["name"] == "nadeef":
        dataset_file = open(run_input["dataset"]["path"], "r")
        csv_reader = csv.DictReader(dataset_file)
        field_names = csv_reader.fieldnames
        column_index = {a.split(" ")[0]: field_names.index(a) for a in field_names}
        nadeef_clean_plan = {
            "source": {
                "type": "csv",
                "file": [os.path.abspath(run_input["dataset"]["path"])]
                },
            "rule": run_input["tool"]["param"]
            }
        nadeef_clean_plan_path = "clean_plan.json".format(TOOLS_FOLDER)
        nadeef_clean_plan_file = open(nadeef_clean_plan_path, "w")
        json.dump(nadeef_clean_plan, nadeef_clean_plan_file)
        nadeef_clean_plan_file.close()
        p = subprocess.Popen(["./nadeef.sh"], cwd="{}/NADEEF".format(TOOLS_FOLDER), stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
        process_output, process_errors = p.communicate("load ../../clean_plan.json\ndetect\nexit\n")
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
        return return_list

    if run_input["tool"]["name"] == "openrefine":
        dataset_file = open(run_input["dataset"]["path"], "r")
        dataset_reader = csv.reader(dataset_file, delimiter=",")
        return_list = []
        cell_visited_flag = {}
        for i, row in enumerate(dataset_reader):
            if i > 0:
                for j, pattern in run_input["tool"]["param"]:
                    if not re.findall(pattern, row[j], re.IGNORECASE):
                        if (i, j) not in cell_visited_flag:
                            cell_visited_flag[(i, j)] = 1
                            return_list.append([i, j, row[j]])
        return return_list
########################################


########################################
if __name__ == "__main__":

    # install_tools()

    run_input = {
        "dataset": {
            "type": "csv",
            "path": "datasets/sample.csv"
        },
        "tool": {
            "name": "dboost",
            "param": ["--gaussian", "1", "--statistical", "1"]
            }
    }

    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "path": "datasets/sample.csv"
    #     },
    #     "tool": {
    #         "name": "nadeef",
    #         "param": [
    #              {
    #                  "type": "fd",
    #                  "value": ["title | brand_name"]
    #              }
    #         ]
    #     }
    # }

    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "path": "datasets/sample.csv"
    #     },
    #     "tool": {
    #         "name": "openrefine",
    #         "param": [(4, "^[\d]+$"), (7, "^[\w]+$")]
    #     }
    # }

    results_list = abstract_layer(run_input)
    for x in results_list:
        print x
########################################

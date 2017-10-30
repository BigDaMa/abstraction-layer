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
        row = [x.strip(" ") if x != "NULL" else "" for x in row]
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
def abstract_layer(run_input):
    """
    This method runs the data cleaning job based on the input configuration.
    """
    # --------------------Preparing Dataset--------------------
    dataset_path = ""
    if run_input["dataset"]["type"] == "csv":
        original_dataset_path = os.path.abspath(run_input["dataset"]["param"][0])
        dataset_header, dataset_matrix = read_csv_dataset(original_dataset_path)
        temp_dataset_path = os.path.abspath("temp.csv")
        new_header = [a + " varchar(20000)" for a in dataset_header]
        write_csv_dataset(temp_dataset_path, new_header, dataset_matrix)
        dataset_path = temp_dataset_path
    # --------------------dBoost--------------------
    if run_input["tool"]["name"] == "dboost":
        command = ["./{}/dBoost/dboost/dboost-stdin.py".format(TOOLS_FOLDER), "-F", ",", dataset_path] + run_input["tool"]["param"]
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
                if (i, j) not in cell_visited_flag and i > 0:
                    cell_visited_flag[(i, j)] = 1
                    return_list.append([i, j, v])
            tool_results_file.close()
            os.remove(tool_results_path)
        os.remove(dataset_path)
        return return_list
    # --------------------NADEEF--------------------
    if run_input["tool"]["name"] == "nadeef":
        field_names, dataset_matrix = read_csv_dataset(dataset_path)
        column_index = {" ".join(a.split(" ")[:-1]): field_names.index(a) for a in field_names}
        nadeef_clean_plan = {
            "source": {
                "type": "csv",
                "file": [dataset_path]
                },
            "rule": run_input["tool"]["param"]
            }
        nadeef_clean_plan_path = "clean_plan.json"
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
        os.remove(dataset_path)
        return return_list
    # --------------------OpenRefine--------------------
    if run_input["tool"]["name"] == "openrefine":
        dataset_header, dataset_matrix = read_csv_dataset(dataset_path)
        return_list = []
        cell_visited_flag = {}
        for i, row in enumerate(dataset_matrix):
            for j, pattern in run_input["tool"]["param"]:
                if not re.findall(pattern, row[j], re.IGNORECASE):
                    if (i + 1, j) not in cell_visited_flag:
                        cell_visited_flag[(i + 1, j)] = 1
                        return_list.append([i + 1, j, row[j]])
        os.remove(dataset_path)
        return return_list
########################################


########################################
if __name__ == "__main__":

    # install_tools()

    run_input = {
        "dataset": {
            "type": "csv",
            "param": ["datasets/sample.csv"]
        },
        "tool": {
            "name": "dboost",
            "param": ["--gaussian", "1", "--statistical", "1"]
            }
    }

    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
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
    #         "param": ["datasets/sample.csv"]
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

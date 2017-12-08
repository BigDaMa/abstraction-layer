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
import subprocess
import pandas
########################################


########################################
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
def read_csv_dataset(dataset_path, header_exists=True):
    """
    The method reads a dataset from a csv file path.
    """
    header_flag = "infer"
    if not header_exists:
        header_flag = None
    dataset_dataframe = pandas.read_csv(dataset_path, sep=",", header=header_flag, encoding=None, keep_default_na=False)
    return dataset_dataframe.columns.get_values().tolist(), dataset_dataframe.get_values().tolist()

def write_csv_dataset(dataset_path, dataset_header, dataset_matrix):
    """
    The method writes a dataset to a csv file path.
    """
    dataset_dataframe = pandas.DataFrame(data=dataset_matrix, columns=dataset_header)
    dataset_dataframe.to_csv(dataset_path, sep=",", header=True, index=False, encoding=None)
########################################


########################################
def run_dboost(dataset_path, dboost_parameters):
    """
    This method runs dBoost on a dataset.
    """
    command = ["./{}/dBoost/dboost/dboost-stdin.py".format(TOOLS_FOLDER), "-F", ",", dataset_path] + dboost_parameters
    p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.communicate()
    tool_results_path = "dboost_results.csv"
    _, detected_cells_list = read_csv_dataset(tool_results_path, header_exists=False)
    return_list = []
    cell_visited_flag = {}
    for row, column, value in detected_cells_list:
        i = int(row)
        j = int(column)
        v = value
        if (i, j) not in cell_visited_flag and i > 0:
            cell_visited_flag[(i, j)] = 1
            return_list.append([i, j, v])
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
    tool_results_path = re.findall("INFO: Export to (.*csv)", process_output)[0]
    _, detected_cells_list = read_csv_dataset(tool_results_path, header_exists=False)
    return_list = []
    cell_visited_flag = {}
    for row in detected_cells_list:
        i = int(row[3])
        j = column_index[row[4]]
        v = row[5]
        if (i, j) not in cell_visited_flag:
            cell_visited_flag[(i, j)] = 1
            return_list.append([i, j, v])
    os.remove(temp_dataset_path)
    os.remove(tool_results_path)
    return return_list


def run_openrefine(dataset_path, openrefine_parameters):
    """
    This method runs OpenRefine on a dataset.
    """
    dataset_header, dataset_matrix = read_csv_dataset(dataset_path)
    columns_patterns_dictionary = {column: [] for column in dataset_header}
    for column, pattern in openrefine_parameters:
        columns_patterns_dictionary[column].append(pattern)
    return_list = []
    cell_visited_flag = {}
    for i, row in enumerate(dataset_matrix):
        for j, column in enumerate(dataset_header):
            cell_value = row[j]
            for pattern in columns_patterns_dictionary[column]:
                if not re.findall(pattern, cell_value, re.IGNORECASE | re.UNICODE):
                    if (i + 1, j) not in cell_visited_flag:
                        cell_visited_flag[(i + 1, j)] = 1
                        return_list.append([i + 1, j, cell_value])
    return return_list
########################################


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
    return return_list


def execute_cleaning(input_file_path, output_file_path):
    """
    This method runs the data cleaning tools on input datasets and saves the results into output locations.
    """
    input_dictionary = json.load(open(input_file_path, "r"))
    input_folder = input_dictionary["CSV"]["dir"]
    if input_dictionary["CSV"]["table"]:
        input_tables = input_dictionary["CSV"]["table"].split(";")
    else:
        input_tables = os.listdir(input_folder)
    output_dictionary = json.load(open(output_file_path, "r"))
    output_folder = output_dictionary["CSV"]["dir"]
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    for table in input_tables:
        dataset_path = os.path.join(input_folder, table)
        results_list = run_dboost(dataset_path, ["--gaussian", "1", "--statistical", "1"])
        result_path = os.path.join(output_folder, table)
        write_csv_dataset(result_path, ["row", "column", "value"], results_list)
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
    #         "param": [("price", "^[\d]+$"), ("brand_name", "^[\w]+$")]
    #     }
    # }

    # results_list = run_data_cleaning_job(run_input)
    # for x in results_list:
    #     print x

    input_file_path = "sources.json"
    output_file_path = "destination.json"
    execute_cleaning(input_file_path, output_file_path)
########################################

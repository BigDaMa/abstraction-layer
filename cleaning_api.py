########################################
# Abstraction Layer
# Mohammad Mahdavi, Milad Abbaszadeh
# moh.mahdavi.l@gmail.com
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
import numpy
import psycopg2
########################################


########################################
TOOLS_FOLDER = "tools"
########################################


########################################
def install_tools(postgres_username="", postgres_password=""):
    """
    This method installs and configures the data cleaning tools.
    """
    for tool in os.listdir(TOOLS_FOLDER):
        if tool == "NADEEF":
            p = subprocess.Popen(["ant", "all"], cwd=os.path.join(TOOLS_FOLDER, "NADEEF"), stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            p.communicate()
            print "To configure NADEEF, please follow the following steps:"
            print "1. Create a database entitled 'nadeef' in the postgres."
            if not postgres_username:
                postgres_username = raw_input("2. Enter your postgres username: ")
            if not postgres_password:
                postgres_password = raw_input("3. Enter your postgres password: ")
            nadeef_configuration_file = open(os.path.join(TOOLS_FOLDER, "NADEEF", "nadeef.conf"), "r+")
            nadeef_configuration = nadeef_configuration_file.read()
            nadeef_configuration = re.sub("(database.username = )([\w\d]+)", "\g<1>{}".format(postgres_username),
                                          nadeef_configuration, flags=re.IGNORECASE)
            nadeef_configuration = re.sub("(database.password = )([\w\d]+)", "\g<1>{}".format(postgres_password),
                                          nadeef_configuration, flags=re.IGNORECASE)
            nadeef_configuration_file.seek(0)
            nadeef_configuration_file.write(nadeef_configuration)
            nadeef_configuration_file.close()
        print "{} is installed.".format(tool)
########################################


########################################
def read_csv_dataset(dataset_path, header_exists=True):
    """
    The method reads a dataset from a csv file path.
    """
    if header_exists:
        dataset_dataframe = pandas.read_csv(dataset_path, sep=",", header="infer", encoding="utf-8", dtype=str,
                                            keep_default_na=False, low_memory=False)
        dataset_dataframe = dataset_dataframe.apply(lambda x: x.str.strip())
        return [dataset_dataframe.columns.get_values().tolist()] + dataset_dataframe.get_values().tolist()
    else:
        dataset_dataframe = pandas.read_csv(dataset_path, sep=",", header=None, encoding="utf-8", dtype=str,
                                            keep_default_na=False)
        dataset_dataframe = dataset_dataframe.apply(lambda x: x.str.strip())
        return dataset_dataframe.get_values().tolist()


def write_csv_dataset(dataset_path, dataset_table):
    """
    The method writes a dataset to a csv file path.
    """
    dataset_dataframe = pandas.DataFrame(data=dataset_table[1:], columns=dataset_table[0])
    dataset_dataframe.to_csv(dataset_path, sep=",", header=True, index=False, encoding="utf-8")
########################################


########################################
def run_dboost(dataset_path, dboost_parameters):
    """
    This method runs dBoost on a dataset.
    """
    command = ["./{}/dBoost/dboost/dboost-stdin.py".format(TOOLS_FOLDER), "-F", ",", "--statistical", "0.5"]
    dboost_parameters[0] = "--" + dboost_parameters[0]
    command += dboost_parameters + [dataset_path]
    p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.communicate()
    cell_visited_flag = {}
    tool_results_path = "dboost_output.csv"
    if os.path.exists(tool_results_path):
        detected_cells_list = read_csv_dataset(tool_results_path, header_exists=False)
        for row, column, value in detected_cells_list:
            i = int(row)
            j = int(column)
            v = value.decode("utf-8")
            if (i, j) not in cell_visited_flag and i > 0:
                cell_visited_flag[(i, j)] = v
        os.remove(tool_results_path)
    return_list = []
    for (i, j) in cell_visited_flag:
        return_list.append([i, j, cell_visited_flag[(i, j)]])
    return return_list


def run_nadeef(dataset_path, nadeef_parameters):
    """
    This method runs NADEEF on a dataset.
    """
    # ---------- Prepare Dataset and Clean Plan ----------
    dataset_table = read_csv_dataset(dataset_path)
    temp_dataset_path = os.path.abspath("nadeef_temp_dataset.csv")
    new_header = [a + " varchar(20000)" for a in dataset_table[0]]
    write_csv_dataset(temp_dataset_path, [new_header] + dataset_table[1:])
    column_index = {a: dataset_table[0].index(a) for a in dataset_table[0]}
    actual_nadeef_parameters = []
    for param in nadeef_parameters:
        actual_nadeef_parameters.append({"type": "fd", "value": [" | ".join(param)]})
    nadeef_clean_plan = {
        "source": {
            "type": "csv",
            "file": [temp_dataset_path]
        },
        "rule": actual_nadeef_parameters
    }
    nadeef_clean_plan_path = "nadeef_clean_plan.json"
    nadeef_clean_plan_file = open(nadeef_clean_plan_path, "w")
    json.dump(nadeef_clean_plan, nadeef_clean_plan_file)
    nadeef_clean_plan_file.close()
    # ---------- Clean up Previous Results ----------
    nadeef_configuration_file = open(os.path.join(TOOLS_FOLDER, "NADEEF", "nadeef.conf"), "r")
    nadeef_configuration = nadeef_configuration_file.read()
    postgres_username = re.findall("database.username = ([\w\d]+)", nadeef_configuration, flags=re.IGNORECASE)[0]
    postgres_password = re.findall("database.password = ([\w\d]+)", nadeef_configuration, flags=re.IGNORECASE)[0]
    nadeef_configuration_file.close()
    connection = psycopg2.connect(dbname="nadeef", host="localhost", user=postgres_username, password=postgres_password)
    cursor = connection.cursor()
    cursor.execute("""DROP TABLE IF EXISTS tb_nadeef_temp_dataset, violation, repair, audit;""")
    connection.commit()
    # ---------- Start Data Cleaning ----------
    p = subprocess.Popen(["./nadeef.sh"], cwd=os.path.join(TOOLS_FOLDER, "NADEEF"), stdout=subprocess.PIPE,
                         stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
    process_output, process_errors = p.communicate("load ../../nadeef_clean_plan.json\ndetect\nrepair\nexit\n")
    # tool_results_path = re.findall("INFO: Export to (.*csv)", process_output)[0]
    cell_visited_flag = {}
    cursor.execute("""SELECT * from violation;""")
    violation_results = cursor.fetchall()
    for row in violation_results:
        i = int(row[3])
        j = column_index[row[4]]
        cell_visited_flag[(i, j)] = "".decode("utf-8")
    cursor.execute("""SELECT * from repair;""")
    repair_results = cursor.fetchall()
    for row in repair_results:
        i_1 = int(row[2])
        j_1 = column_index[row[4]]
        v_1 = row[5].decode("utf-8")
        i_2 = int(row[7])
        j_2 = column_index[row[9]]
        v_2 = row[10].decode("utf-8")
        # NOTE: Assume the second cell value is the correct one!
        cell_visited_flag[(i_1, j_1)] = v_2
        cell_visited_flag[(i_2, j_2)] = v_2
    return_list = []
    for (i, j) in cell_visited_flag:
        return_list.append([i, j, cell_visited_flag[(i, j)]])
    # ---------- Clean up Current results ----------
    for f in os.listdir(os.path.join(TOOLS_FOLDER, "NADEEF", "out")):
        if os.path.isfile(os.path.join(TOOLS_FOLDER, "NADEEF", "out", f)):
            os.remove(os.path.join(TOOLS_FOLDER, "NADEEF", "out", f))
    os.remove(nadeef_clean_plan_path)
    os.remove(temp_dataset_path)
    return return_list


def run_openrefine(dataset_path, openrefine_parameters):
    """
    This method runs OpenRefine on a dataset.
    """
    dataset_table = read_csv_dataset(dataset_path)
    attributes_list = dataset_table[0]
    dataset_matrix = numpy.array(dataset_table[1:])
    attributes_patterns = {a: {} for a in attributes_list}
    for attribute, pattern in openrefine_parameters:
        if attribute in attributes_patterns:
            attributes_patterns[attribute][pattern] = 1
    cell_visited_flag = {}
    for j, attribute in enumerate(attributes_list):
        for i, value in enumerate(dataset_matrix[:, j]):
            for pattern in attributes_patterns[attribute]:
                if not re.findall(pattern, value.encode("utf-8"), re.UNICODE):
                    cell_visited_flag[(i + 1, j)] = "".decode("utf-8")
    return_list = []
    for (i, j) in cell_visited_flag:
        return_list.append([i, j, cell_visited_flag[(i, j)]])
    return return_list


def run_katara(dataset_path, katara_parameters):
    """
    This method runs KATARA on a dataset.
    """
    command = ["java", "-classpath",
               "{0}/KATARA/out/test/test:{0}/KATARA/jar_files/SimplifiedKATARA.jar:{0}/KATARA/jar_files/commons-lang3-3.7.jar".format(TOOLS_FOLDER),
               "simplied.katara.SimplifiedKATARAEntrance"]
    knowledge_base_path = os.path.abspath(katara_parameters[0])
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.communicate(dataset_path + "\n" + knowledge_base_path + "\n")
    cell_visited_flag = {}
    tool_results_path = "katara_output.csv"
    if os.path.exists(tool_results_path):
        detected_cells_list = read_csv_dataset(tool_results_path, header_exists=False)
        for row, column, value in detected_cells_list:
            i = int(row)
            j = int(column)
            v = value.decode("utf-8")
            if (i, j) not in cell_visited_flag and i > 0:
                cell_visited_flag[(i, j)] = v
        os.remove(tool_results_path)
    os.remove("crowdclient-runtime.log")
    return_list = []
    for (i, j) in cell_visited_flag:
        return_list.append([i, j, cell_visited_flag[(i, j)]])
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
    if data_cleaning_job["tool"]["name"] == "katara":
        return_list = run_katara(dataset_path, data_cleaning_job["tool"]["param"])
    return return_list
########################################


########################################
if __name__ == "__main__":
    pass

    # install_tools()

    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
    #     },
    #     "tool": {
    #         "name": "dboost",
    #         "param": ["gaussian", "1"]
    #         }
    # }
    #
    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
    #     },
    #     "tool": {
    #         "name": "nadeef",
    #         "param": [["title", "brand_name"]]
    #     }
    # }
    #
    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
    #     },
    #     "tool": {
    #         "name": "openrefine",
    #         "param": [["price", "^[\d]+$"], ["brand_name", "^[\w]*$"]]
    #     }
    # }
    #
    # run_input = {
    #     "dataset": {
    #         "type": "csv",
    #         "param": ["datasets/sample.csv"]
    #     },
    #     "tool": {
    #         "name": "katara",
    #         "param": ["tools/KATARA/dominSpecific"]
    #     }
    # }
    #
    # results_list = run_data_cleaning_job(run_input)
    # for x in results_list:
    #     print x
########################################

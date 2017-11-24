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
from pprint import pprint

########################################


########################################
DATASETS_FOLDER = "datasets"
TOOLS_FOLDER = "tools"
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

#######################################
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

########################################


def cleaning_service():
    """
    This method runs the data cleaning job based on the input configuration.
    """

    return_list = []
    data = json.load(open('sources.json'))
    if data['CSV']['table']=='':
        files = os.listdir(data['CSV']['dir'])

        dboost_parameters=["--gaussian", "1", "--statistical", "1"]
        for f in files:
            dataset_path=os.path.join(data['CSV']['dir'], f)
            return_list=run_dboost(dataset_path,dboost_parameters)

            data_out = json.load(open('destination.json'))
            out_dataset_path = os.path.join(data_out['CSV']['dir'], f)

            write_csv_dataset(out_dataset_path," ",return_list)





######################################

if __name__ == "__main__":


    cleaning_service()


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
########################################


########################################
DATASETS_FOLDER = "datasets"
TOOLS_FOLDER = "tools"
if not os.path.exists(DATASETS_FOLDER):
    os.mkdir(DATASETS_FOLDER)
if not os.path.exists(TOOLS_FOLDER):
    os.mkdir(TOOLS_FOLDER)
########################################


########################################
def install_tools(tools_list):
    for tool in tools_list:
        if not os.path.exists(os.path.join(TOOLS_FOLDER, tool)):
            if tool == "dboost":
                x = subprocess.Popen(
                    "cd {}\ngit clone https://github.com/cpitclaudel/dBoost.git\n".format(TOOLS_FOLDER),
                    stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
                x.communicate(" ")
            if tool == "nadeef":
                x = subprocess.Popen(
                    "cd {}\ngit clone https://github.com/daqcri/NADEEF.git\ncd NADEEF\nant all\n".format(TOOLS_FOLDER),
                    stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
                x.communicate(" ")
        print "{} is installed.".format(tool)
########################################


########################################
def abstract_layer(run_input):
    if run_input["tool"]["name"] == "dboost":
        runpath = [".{}/dBoost/dboost/dboost-stdin.py".format(TOOLS_FOLDER), "-F", ","] + \
                  run_input["tool"]["param"] + [run_input["dataset"]["path"]]
        process = subprocess.Popen(runpath, stdout=subprocess.PIPE, shell=True)
        lines_list = process.stdout.readlines()
        results_list = []
        for i, line in enumerate(lines_list):
            cells_list = re.split(r"\s{2,}", line.decode("utf-8").strip())
            for j, cell in enumerate(cells_list):
                if cell.startswith("\x1b[0;0m\x1b[4;31m") and cell.endswith("\x1b[0;0m"):
                    results_list.append([i, j, cell[13:-6]])
        return results_list
    if run_input["tool"]["name"] == "nadeef":
        dic = {
            "source": {
                "type": "csv",
                "file": [run_input["dataset"]["path"]]
                },
            "rule": run_input["tool"]["param"]
            }
        with open("data.json", "w") as outfile:
            json.dump(dic, outfile)
        process = subprocess.Popen("./{}/NADEEF/nadeef.sh".format(TOOLS_FOLDER), stdout=subprocess.PIPE,
                                   stdin=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        #TODO
        out, erro = process.communicate("load ../../data.json\ndetect\nexit\n")
########################################


########################################
if __name__ == "__main__":
    install_tools(["dboost", "nadeef"])
    run_input = {
        "dataset": {
            "type": "csv",
            "path": "datasets/11.csv"
        },
        "tool": {
            "name": "dboost",
            "param": ["--gaussian", "1", "--statistical", "1"]
            }
    }
    results_list = abstract_layer(run_input)
    for x in results_list:
        print x
########################################
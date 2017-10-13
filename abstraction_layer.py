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
TOOLS_LIST = ["dboost", "nadeef"]
########################################


########################################
def install_tools():
    if not os.path.exists(DATASETS_FOLDER):
        os.mkdir(DATASETS_FOLDER)
    if not os.path.exists(TOOLS_FOLDER):
        os.mkdir(TOOLS_FOLDER)
    for tool in TOOLS_LIST:
        if not os.path.exists(os.path.join(TOOLS_FOLDER, tool)):
            if tool == "dboost":
                p = subprocess.Popen(["git", "clone", "https://github.com/cpitclaudel/dBoost.git"], cwd=TOOLS_FOLDER,
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                p.communicate()
            if tool == "nadeef":
                p = subprocess.Popen(["git", "clone", "https://github.com/daqcri/NADEEF.git"], cwd=TOOLS_FOLDER,
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                p.communicate()
                # TODO: compile and config nadeef
        print "{} is installed.".format(tool)
########################################


########################################
def abstract_layer(run_input):
    if run_input["tool"]["name"] == "dboost":
        runpath = ["./{}/dBoost/dboost/dboost-stdin.py".format(TOOLS_FOLDER), "-F", ",", run_input["dataset"]["path"]] \
                  + run_input["tool"]["param"]
        p = subprocess.Popen(runpath, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        raw_output = p.communicate()[0]
        lines_list = raw_output.splitlines()[5:-1]
        results_list = []
        for i, line in enumerate(lines_list):
            cells_list = re.split(r"\s{2,}", line.decode("utf-8").strip())
            for j, cell in enumerate(cells_list):
                error_flag = False
                while cell.startswith("\x1b[0;0m\x1b[4;31m") and cell.endswith("\x1b[0;0m"):
                    error_flag = True
                    cell = cell[13:-6]
                if error_flag:
                    results_list.append([i, j, cell])
        return results_list
    if run_input["tool"]["name"] == "nadeef":
        dic = {
            "source": {
                "type": "csv",
                "file": [run_input["dataset"]["path"]]
                },
            "rule": run_input["tool"]["param"]
            }
        #TODO remove config file after runnig nadeef and also return results from the function just like the dboost
        with open("data.json", "w") as outfile:
            json.dump(dic, outfile)
        process = subprocess.Popen("./{}/NADEEF/nadeef.sh".format(TOOLS_FOLDER), stdout=subprocess.PIPE,
                                   stdin=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        out, erro = process.communicate("load ../../data.json\ndetect\nexit\n")
########################################


########################################
if __name__ == "__main__":
    install_tools()
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
    results_list = abstract_layer(run_input)
    for x in results_list:
        print x
########################################
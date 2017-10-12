#this function has been introduce in the document

import subprocess
import csv
import sys
import re
import pandas as pd
import json
import subprocess
#from hpmudext import probe_devices
import os




def abstract_layer(run_input):

    if run_input["Tool"]["name"]== "Dboost":
        directory = '../dBoost/dboost/'

        def choose_method(i,runpath):
            if run_input["Tool"]["param"][i]== "--statistical":
                statistical_element = run_input["Tool"]["param"][i+1]
                seprator_element ="-F ,"
                runpath= runpath+" "+"--statistical"+" "+statistical_element+" "+seprator_element
                run(runpath)

            if run_input["Tool"]["param"][i]== "--discretestats":
                discretestats_elemnt=run_input["Tool"]["param"][i+1]
                discretestats_elemnt2=run_input["Tool"]["param"][i+2]
                seprator_element="-F ,"
                runpath=runpath+" "+"--discretestats"+" "+discretestats_elemnt+" "+discretestats_elemnt2+" "+seprator_element
                run(runpath)

            if run_input["Tool"]["param"][i]== "--cords":
                cords_elemnt=run_input["Tool"]["param"][i+1]
                cords_elemnt2=run_input["Tool"]["param"][i+2]
                seprator_element="-F ,"
                runpath=runpath+" "+"--cords"+" "+cords_elemnt+" "+cords_elemnt2+" "+seprator_element
                run(runpath)

        def run(runpath):

            runpath ="./dboost-stdin.py " + runpath
            print(runpath)

            args = []
            s = ""

            args.append(directory)
            args.append(runpath)


            process = subprocess.Popen("cd dBoost/dboost\n"+args[1], stdout=subprocess.PIPE, shell=True)
            list1 = []
            list1 = process.stdout.readlines()

            list2 = []
            values = []

            for i in range(1, len(list1)):
                list2 = re.split(r'\s{2,}', list1[i].decode('utf-8').strip())

                for j in range(len(list2)):
                    if list2[j].startswith("\x1b[0;0m\x1b[4;31m") and list2[j].endswith("\x1b[0;0m"):
                        values.append([i, j,list2[j][13:-6]])



            keys = ["Row", "Column","value"]

            outliers = pd.DataFrame(data=values, columns=keys)
            outliers.to_csv('result_dboost.csv', index=False, sep=',', encoding='utf-8')

            print(outliers)



        pathaddress = run_input["Dataset"]["path"]

        method =run_input["Tool"]["param"][0]

        runpath=pathaddress+" "+method

        if method =="--gaussian":

            gausaianElemet = run_input["Tool"]["param"][1]
            runpath=runpath+" "+gausaianElemet
            i=2
            choose_method(i,runpath)



        if method =="--histogram":

            histogramElement1 = run_input["Tool"]["param"][1]
            histogramElement2 = run_input["Tool"]["Param"][2]
            runpath=runpath+" "+histogramElement1+" "+histogramElement2
            i=3
            choose_method(i,runpath)

        if method=="--mixture":

            mixtureElement1 = run_input["Tool"]["param"][1]
            mixtureElement2 = run_input["Tool"]["param"][2]
            runpath=runpath+" "+mixtureElement1+" "+mixtureElement2
            i=3
            choose_method(i,runpath)

        if method =="--partitionedhistogram":

            partitionedhistogramElement1 = run_input["Tool"]["param"][1]
            partitionedhistogramElement2 = run_input["Tool"]["param"][2]
            partitionedhistogramElement3 = run_input["Tool"]["param"][3]
            runpath=runpath+" "+partitionedhistogramElement1+" "+partitionedhistogramElement2+" "+partitionedhistogramElement3
            i=4
            choose_method(i,runpath)

    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

    if (run_input["Tool"]["name"] == "Nadeef"):

        if (run_input["Dataset"]["type"] == "csv"):
            dic = {
                "source": {
                    "type": "csv",
                    "file": [run_input["Dataset"]["path"]]
                },
                "rule": run_input["Tool"]["param"]
            }

        with open('data.json', 'w') as outfile:
            json.dump(dic, outfile)

        x = subprocess.Popen("cd NADEEF\nant all\n./nadeef.sh", stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                             stderr=subprocess.STDOUT, shell=True)
        out, erro = x.communicate("load /home/milad/Desktop/data.json\ndetect\nexit\n")
        print (out)
        print (erro)


################ sample run input for Dboost #################
#
# run_input={
#     "Dataset":{
#         "type":"csv",
#         "path":"/home/milad/Desktop/DFKI/Data_set/sampledatasets/11.csv"
#     },
#     "Tool":
#         {   "name":"Dboost",
#             "param":["--gaussian","1","--statistical","1"]
#
#         }
#
# }



##############  sample run input for Nadeef  ##########


run_input={
    "Dataset":{
        "type":"csv",
        "path":"/home/milad/Desktop/nadeef/dataset_sample.csv"
    },
    "Tool":
        {   "name":"Nadeef",
            "param":[
        {
            "type":"fd",
            "value":["first_author | language"]
        }
    ]

        }

}

abstract_layer(run_input)

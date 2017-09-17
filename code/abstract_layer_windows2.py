#this function has been introduce in the document
import subprocess
import csv
import sys
import re
import pandas as pd




def abstract_layer(run_input):
    if run_input[0]== "Dboost":

        def choose_method(i,runpath):
            if run_input[i]== "--statistical":
                statistical_element = run_input[i+1]
                seprator_element =run_input[i+2]
                runpath= runpath+" "+"--statistical"+" "+statistical_element+" "+seprator_element
                run(runpath)

            if run_input[i]== "--discretestats":
                discretestats_elemnt=run_input[i+1]
                discretestats_elemnt2=run_input[i+2]
                seprator_element=run_input[i+3]
                runpath=runpath+" "+"--discretestats"+" "+discretestats_elemnt+" "+discretestats_elemnt2+" "+seprator_element
                run(runpath)

            if run_input[i]== "--cords":
                cords_elemnt=run_input[i+1]
                cords_elemnt2=run_input[i+2]
                seprator_element=run_input[i+3]
                runpath=runpath+" "+"--cords"+" "+cords_elemnt+" "+cords_elemnt2+" "+seprator_element
                run(runpath)

        def run(runpath):
            runpath = "python dboost-stdin.py " + runpath
            print(runpath)

            args = []
            s = ""

            args.append(directory)
            args.append(runpath)

            #print(args)
            process = subprocess.Popen(args[1], stdout=subprocess.PIPE, shell=True, cwd=args[0])
            list1 = []
            list1 = process.stdout.readlines()

            list2 = []
            values = []

            for i in range(1, len(list1)):
                list2 = re.split(r'\s{2,}', list1[i].decode('utf-8').strip())

                for j in range(len(list2)):
                    if list2[j].startswith("\x1b[0;0m\x1b[4;31m") == True and list2[j].endswith("\x1b[0;0m" == True):
                        values.append([i, j])

            keys = ["Row", "Column"]

            outliers = pd.DataFrame(data=values, columns=keys)
            outliers.to_csv('result_dboost.csv', index=False, sep=',', encoding='utf-8')

            print(outliers)



        pathaddress = run_input[1]
        method= run_input[2]
        runpath=pathaddress+" "+method

        if method in ['--gaussian']:

            gausaianElemet = run_input[3]
            runpath=runpath+" "+gausaianElemet
            i=4
            choose_method(i,runpath)



        if method in ['--histogram']:

            histogramElement1 = run_input[3]
            histogramElement2 = run_input[4]
            runpath=runpath+" "+histogramElement1+" "+histogramElement2
            i=5
            choose_method(i,runpath)

        if method in ['--mixture']:

            mixtureElement1 = run_input[3]
            mixtureElement2 = run_input[4]
            runpath=runpath+" "+mixtureElement1+" "+mixtureElement2
            i=5
            choose_method(i,runpath)

        if method in ['--partitionedhistogram']:

            partitionedhistogramElement1 = run_input[3]
            partitionedhistogramElement2 = run_input[4]
            partitionedhistogramElement3 = run_input[5]
            runpath=runpath+" "+partitionedhistogramElement1+" "+partitionedhistogramElement2+" "+partitionedhistogramElement3
            i=6
            choose_method(i,runpath)



run_input=["Dboost","E:\\work\DFKI\\Data_set\\sampledatasets\\11.csv","--gaussian","1","--statistical","1","-F ,"]
directory='E:\work\DFKI\Tools\Dboost\dBoost-master'
abstract_layer(run_input)

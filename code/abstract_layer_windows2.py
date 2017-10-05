#this function has been introduce in the document
"""
Order of imported modules
I would suggest you have a look at Python code style (just as something useful, not mandatory):
https://www.python.org/dev/peps/pep-0008/
"""
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

"""
Let's make the input configuration more generic. I describe what is in my mind. Feel free to modify it.

input_configuration = {
	"dataset": "path/to/the/dataset.csv",
	"method": "dboost", # It can be any of the other tools such as dboost, nadeef, etc.
	"parameters": ["gaussian", "1", "statistical", "0.5", ","], # The list of parameters is based on the selected method. The supported methods and required parameters should be documented. You may also decide to define parameters with dictionary instead of list. Each one has its own downsides and upsides.
}

output_results = abstraction_layer(input_configuration)
based on the input_configuration, the output_results could have either list of detected cells, i.e., [(i1,j1), (i2, j2),...] or list of repaird values [(i1, j1, old_value1, new_value1),...]

The address of dboost and the other tools should be known to your program. you can put it as a constant value in the configuration of your program. there is no need to take it from the user. Actually, I think your should have another API function (for example installation or initialization) that automatically download and installed the underlaying tools. As a user, i install your module, call the initialization API function, then your program download and install dboost, nadeef, etc. and would know the address of them.



Some usefule modules and functions:

"{}, Try to print this {} line.".format("hello", 1)


I did not write more comments for the internal codes. bacause, i think once you modify the input_configuration, you have to change the codes as well. 

"""

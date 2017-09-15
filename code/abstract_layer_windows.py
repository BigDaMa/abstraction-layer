#this function has been introduce in the document
import subprocess
import csv
import sys




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

            with open('csvfile1.txt', 'w') as f:
                #runpath = "python dboost-stdin.py E:\\work\DFKI\\Data_set\\sampledatasets\\11.csv --gaussian 1 --statistical 1 -F ,"
                outpuut = subprocess.call(runpath, stdout=f, shell=True)

                if outpuut ==1:
                    print("the string that passed to function has problem")


            with open('csvfile1.txt', 'r') as ff:
                filedata = ff.read()
                # output.splitlines()
                # regularr expression
                # for line in filedata:
                num_lines = sum(1 for line in open('csvfile1.txt'))
                print(num_lines)
                while num_lines >= 0:
                    out = filedata.replace("\x1b[0;0m\x1b[4;31m", '<red>')
                    out1 = out.replace("\x1b[0;0m", '</red>')
                    # print(out1)
                    with open('csvfile2.txt', 'w') as file:
                        file.write(out1)
                        num_lines = num_lines - 1

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
abstract_layer(run_input)
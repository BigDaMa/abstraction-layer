import os
import json
import re
import subprocess
import pandas


#p= subprocess.Popen(["bash", "./start_notebook1.sh"],cwd="/home/milad/Desktop/DFKI/abstraction-layer/tools/Holoclean/HoloClean")
#p.communicate()



#############################################

print "this is the start of Holoclean"

#Connecting to the Database

from holoclean.holoclean import HoloClean, Session
holo = HoloClean(mysql_driver = "/home/milad/Desktop/DFKI/abstraction-layer/tools/Holoclean/HoloClean/holoclean/lib/mysql-connector-java-5.1.44-bin.jar" )
session = Session(holo)

#loading data
 
data_path = "/home/milad/Desktop/DFKI/abstraction-layer/tools/Holoclean/HoloClean/tutorial/data/hospital_dataset.csv"

## loads data into our database and returns pyspark dataframe of initial data
data = session.load_data(data_path)

dc_path = "/home/milad/Desktop/DFKI/abstraction-layer/tools/Holoclean/HoloClean/tutorial/data/hospital_constraints.txt"

# loads denial constraints into our database and returns a simple list of dcs as strings 
dcs = session.load_denial_constraints(dc_path)

# all pyspark dataframe commands available
data.select('City').show(15)

##### error detector ######
###########################

from holoclean.errordetection.mysql_dcerrordetector import Mysql_DCErrorDetection

# instantiate Holoclean's built in error detector
detector = Mysql_DCErrorDetection(session.Denial_constraints, holo, session.dataset)

# both clean and dirty sets are returned as pyspark dataframes
clean, dirty = session.detect_errors(detector)

###### repair ######
####################

repaired = session.repair()


repaired = repaired.withColumn("index", repaired["index"].cast("int"))
repaired.sort('index').select('City').show(15)










import os
import json
import re
import subprocess
import pandas
import sys
sys.path.append("..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.errordetection.sql_nullerrordetector import SqlnullErrorDetection
import time

#############################################

print "this is the start of Holoclean"

#Connecting to the Database

from holoclean.holoclean import HoloClean, Session

holo       = HoloClean(
            holoclean_path="..",         # path to holoclean package
            verbose=False,
            # to limit possible values for training data
            pruning_threshold1=0.1,
            # to limit possible values for training data to less than k values
            pruning_clean_breakoff=6,
            # to limit possible values for dirty data (applied after
            # Threshold 1)
            pruning_threshold2=0,
            # to limit possible values for dirty data to less than k values
            pruning_dk_breakoff=6,
            # learning parameters
            learning_iterations=30,
            learning_rate=0.001,
            batch_size=5
        )
session = Session(holo)


#loading data
 
data_path = "/home/milad/Desktop/HoloClean/tutorials/data/address_10.csv"

## loads data into our database and returns pyspark dataframe of initial data
data = session.load_data(data_path)

dc_path = "/home/milad/Desktop/HoloClean/tutorials/data/address_ten_constraints.txt"

# loads denial constraints into our database and returns a simple list of dcs as strings 
dcs = session.load_denial_constraints(dc_path)


# all pyspark dataframe commands available
data.select('City','zip','state').show(15)


##### error detector ######
###########################

from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection

# instantiate Holoclean's built in error detector
detector = SqlDCErrorDetection(session)

# both clean and dirty sets are returned as pyspark dataframes
error_detector_list =[]
error_detector_list.append(detector)
clean, dirty = session.detect_errors(error_detector_list)

###### repair ######
####################

repaired = session.repair()


repaired = repaired.withColumn("__ind", repaired["__ind"].cast("int"))
repaired.sort('__ind').select('city','zip','state').show(15)

repaired.sort('index')
repaired.write.format('com.databricks.spark.csv').option("header", 'true').save('repaired.csv')

print "now one folder created that you can find your repair overthere"




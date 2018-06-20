import sys
sys.path.append("..")
from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.errordetection.sql_nullerrordetector import\
    SqlnullErrorDetection


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

# data_path = "data/hospital.csv"
data_path = "data/address_10.csv"

data = session.load_data(data_path)

# dc_path = "data/hospital_constraints.txt"
dc_path = "data/address_ten_constraints.txt"

dcs = session.load_denial_constraints(dc_path)


data.select('city').show(20)


from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection

detector = SqlDCErrorDetection(session)

error_detector_list =[]
error_detector_list.append(detector)
clean, dirty = session.detect_errors(error_detector_list)


clean.head(5)


dirty.head(5)


repaired = session.repair()


repaired = repaired.withColumn("index", repaired["index"].cast("int"))
repaired.sort('index').select('city').show(20)
repaired.repartition(1).write.format('com.databricks.spark.csv').option("header", 'true').save('repaired.csv')



# session.compare_to_truth("data/hospital_clean.csv")
session.compare_to_truth("data/address_10_ground_truth.csv")


from utility_functions import *

record_objs = create_initial_patient_records(5)
simulate_patient_monitoring(record_objs, runtime=300, interval=1)
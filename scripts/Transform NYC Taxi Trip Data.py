import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import re

# Script generated for node Remove Records with NULL
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF().na.drop()
    results = DynamicFrame.fromDF(df, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Pickup Zone Lookup
PickupZoneLookup_node1750452445430 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_taxi_zone_lookup", transformation_ctx="PickupZoneLookup_node1750452445430")

# Script generated for node Dropoff Zone Lookup
DropoffZoneLookup_node1750452641799 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_taxi_zone_lookup", transformation_ctx="DropoffZoneLookup_node1750452641799")

# Script generated for node Yellow Trip Data
YellowTripData_node1750452113121 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_yellow_tripdata", transformation_ctx="YellowTripData_node1750452113121")

# Script generated for node Change Schema - Pickup Zone Lookup
ChangeSchemaPickupZoneLookup_node1750452502500 = ApplyMapping.apply(frame=PickupZoneLookup_node1750452445430, mappings=[("locationid", "long", "pu_location_id", "long"), ("borough", "string", "pu_borough", "string"), ("zone", "string", "pu_zone", "string"), ("service_zone", "string", "pu_service_zone", "string")], transformation_ctx="ChangeSchemaPickupZoneLookup_node1750452502500")

# Script generated for node Change Schema - Dropoff Zone Lookup
ChangeSchemaDropoffZoneLookup_node1750452679028 = ApplyMapping.apply(frame=DropoffZoneLookup_node1750452641799, mappings=[("locationid", "long", "do_location_id", "long"), ("borough", "string", "do_borough", "string"), ("zone", "string", "do_zone", "string"), ("service_zone", "string", "do_service_zone", "string")], transformation_ctx="ChangeSchemaDropoffZoneLookup_node1750452679028")

# Script generated for node Remove Records with NULL
RemoveRecordswithNULL_node1750452297394 = MyTransform(glueContext, DynamicFrameCollection({"YellowTripData_node1750452113121": YellowTripData_node1750452113121}, glueContext))

# Script generated for node SelectFromCollection
SelectFromCollection_node1750452342111 = SelectFromCollection.apply(dfc=RemoveRecordswithNULL_node1750452297394, key=list(RemoveRecordswithNULL_node1750452297394.keys())[0], transformation_ctx="SelectFromCollection_node1750452342111")

# Script generated for node Filter - Yellow Trip Data
FilterYellowTripData_node1750452390069 = Filter.apply(frame=SelectFromCollection_node1750452342111, f=lambda row: (bool(re.match("^2020-1", row["tpep_pickup_datetime"]))), transformation_ctx="FilterYellowTripData_node1750452390069")

# Script generated for node Yellow Trips Data + Pickup Zone Lookup
YellowTripsDataPickupZoneLookup_node1750452554156 = Join.apply(frame1=ChangeSchemaPickupZoneLookup_node1750452502500, frame2=FilterYellowTripData_node1750452390069, keys1=["pu_location_id"], keys2=["pulocationid"], transformation_ctx="YellowTripsDataPickupZoneLookup_node1750452554156")

# Script generated for node Join
Join_node1750452718894 = Join.apply(frame1=ChangeSchemaDropoffZoneLookup_node1750452679028, frame2=YellowTripsDataPickupZoneLookup_node1750452554156, keys1=["do_location_id"], keys2=["dolocationid"], transformation_ctx="Join_node1750452718894")

# Script generated for node Change Schema - Joined Data
ChangeSchemaJoinedData_node1750452778310 = ApplyMapping.apply(frame=Join_node1750452718894, mappings=[("do_location_id", "long", "do_location_id", "long"), ("do_borough", "string", "do_borough", "string"), ("do_zone", "string", "do_zone", "string"), ("do_service_zone", "string", "do_service_zone", "string"), ("pu_location_id", "long", "pu_location_id", "long"), ("pu_borough", "string", "pu_borough", "string"), ("pu_zone", "string", "pu_zone", "string"), ("pu_service_zone", "string", "pu_service_zone", "string"), ("vendorid", "long", "vendor_id", "long"), ("tpep_pickup_datetime", "string", "pickup_datetime", "timestamp"), ("tpep_dropoff_datetime", "string", "dropoff_datetime", "timestamp"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("ratecodeid", "long", "ratecodeid", "long"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("congestion_surcharge", "double", "congestion_surcharge", "double")], transformation_ctx="ChangeSchemaJoinedData_node1750452778310")

# Script generated for node Transformed Yellow Trip Data
EvaluateDataQuality().process_rows(frame=ChangeSchemaJoinedData_node1750452778310, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750452033014", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TransformedYellowTripData_node1750452856561 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemaJoinedData_node1750452778310, connection_type="s3", format="glueparquet", connection_options={"path": "s3://serverlessanalytics-054037110300-transformed/nyc-taxi/yellow-tripdata/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="TransformedYellowTripData_node1750452856561")

job.commit()
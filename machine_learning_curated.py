import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> 
DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1697362621180 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_trusted",
        transformation_ctx="StepTrainerTrustedZone_node1697362621180",
    )
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1697362648753 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1697362648753",
    )
)

# Script generated for node Accelerometer and Step Trainer Inner Join 
Query
SqlQuery0 = """
select distinct accelerometer_trusted.timestamp, 
accelerometer_trusted.user, accelerometer_trusted.x, 
accelerometer_trusted.y, accelerometer_trusted.z, 
step_trainer_trusted.sensorreadingtime, step_trainer_trusted.serialnumber, 
step_trainer_trusted.distancefromobject
from accelerometer_trusted, step_trainer_trusted
where accelerometer_trusted.timestamp = 
step_trainer_trusted.sensorreadingtime;
"""
AccelerometerandStepTrainerInnerJoinQuery_node1697362741401 = 
sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": 
AccelerometerTrustedZone_node1697362648753,
        "step_trainer_trusted": StepTrainerTrustedZone_node1697362621180,
    },
    
transformation_ctx="AccelerometerandStepTrainerInnerJoinQuery_node1697362741401",
)

# Script generated for node SQL Distinct Query
SqlQuery1 = """
select distinct * from myDataSource
"""
SQLDistinctQuery_node1697363208650 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={
        "myDataSource": 
AccelerometerandStepTrainerInnerJoinQuery_node1697362741401
    },
    transformation_ctx="SQLDistinctQuery_node1697363208650",
)

# Script generated for node Machine Learning Curated Zone
MachineLearningCuratedZone_node1697362925141 = glueContext.getSink(
    path="s3://pers-stedi-lake-house/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCuratedZone_node1697362925141",
)
MachineLearningCuratedZone_node1697362925141.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCuratedZone_node1697362925141.setFormat("json")
MachineLearningCuratedZone_node1697362925141.writeFrame(
    SQLDistinctQuery_node1697363208650
)
job.commit()


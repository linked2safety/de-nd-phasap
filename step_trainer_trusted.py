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

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1697274219961 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_landing",
        transformation_ctx="StepTrainerLandingZone_node1697274219961",
    )
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1697274257182 = 
glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedZone_node1697274257182",
)

# Script generated for node Step Trainer and Customer Curated Join
SqlQuery1 = """
select step_trainer_landing.sensorreadingtime, 
step_trainer_landing.serialnumber, step_trainer_landing.distancefromobject
from step_trainer_landing, customer_curated
where step_trainer_landing.serialnumber = customer_curated.serialnumber
"""
StepTrainerandCustomerCuratedJoin_node1697274292010 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={
        "step_trainer_landing": StepTrainerLandingZone_node1697274219961,
        "customer_curated": CustomerCuratedZone_node1697274257182,
    },
    
transformation_ctx="StepTrainerandCustomerCuratedJoin_node1697274292010",
)

# Script generated for node SQL Distinct Query
SqlQuery0 = """
select distinct * from myDataSource
"""
SQLDistinctQuery_node1697274509745 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": 
StepTrainerandCustomerCuratedJoin_node1697274292010},
    transformation_ctx="SQLDistinctQuery_node1697274509745",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1697274531528 = glueContext.getSink(
    path="s3://pers-stedi-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrustedZone_node1697274531528",
)
StepTrainerTrustedZone_node1697274531528.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrustedZone_node1697274531528.setFormat("json")
StepTrainerTrustedZone_node1697274531528.writeFrame(SQLDistinctQuery_node1697274509745)
job.commit()


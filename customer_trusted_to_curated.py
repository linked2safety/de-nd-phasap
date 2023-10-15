import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1697269935251 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1697269935251",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1697269978208 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1697269978208",
    )
)

# Script generated for node Inner Join Query
SqlQuery0 = """
select distinct serialnumber, sharewithpublicasofdate, birthday, registrationdate, sharewithresearchasofdate, 
customername, email, lastupdatedate, phone, sharewithfriendsasofdate 
from accelerometer_trusted, customer_trusted
where accelerometer_trusted.user = customer_trusted.email;
"""
InnerJoinQuery_node1697271244983 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": CustomerTrustedZone_node1697269935251,
        "accelerometer_trusted": AccelerometerTrustedZone_node1697269978208,
    },
    transformation_ctx="InnerJoinQuery_node1697271244983",
)

# Script generated for node SQL Distinct Query
SqlQuery1 = """
select distinct * from myDataSource
"""
SQLDistinctQuery_node1697272138861 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"myDataSource": InnerJoinQuery_node1697271244983},
    transformation_ctx="SQLDistinctQuery_node1697272138861",
)

# Script generated for node Amazon S3
AmazonS3_node1697272304008 = glueContext.write_dynamic_frame.from_options(
    frame=SQLDistinctQuery_node1697272138861,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://pers-stedi-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1697272304008",
)

job.commit()


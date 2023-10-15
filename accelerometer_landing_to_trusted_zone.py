import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1697026997076 = 
glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://pers-stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1697026997076",
)

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1 = 
glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://pers-stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingZone_node1",
)

# Script generated for node Customer Trusted with Accelerometer Landing 
Privacy Join
CustomerTrustedwithAccelerometerLandingPrivacyJoin_node1697027132427 = 
Join.apply(
    frame1=CustomerTrustedZone_node1697026997076,
    frame2=AccelerometerLandingZone_node1,
    keys1=["email"],
    keys2=["user"],
    
transformation_ctx="CustomerTrustedwithAccelerometerLandingPrivacyJoin_node1697027132427",
)

# Script generated for node Drop Fields
DropFields_node1697027733409 = DropFields.apply(
    
frame=CustomerTrustedwithAccelerometerLandingPrivacyJoin_node1697027132427,
    paths=[
        "serialNumber",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1697027733409",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1697028081691 = glueContext.getSink(
    path="s3://pers-stedi-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrustedZone_node1697028081691",
)
AccelerometerTrustedZone_node1697028081691.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrustedZone_node1697028081691.setFormat("json")
AccelerometerTrustedZone_node1697028081691.writeFrame(DropFields_node1697027733409)
job.commit()


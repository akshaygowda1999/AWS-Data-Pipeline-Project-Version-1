import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

empDf = glueContext.create_dynamic_frame.from_catalog(
        database="p1_final_db2",
        table_name="inbox",
        transformation_ctx = "inbox"
        )
        
incrementalEmpDf = empDf.toDF()
print(incrementalEmpDf.count())

if incrementalEmpDf.count() == 0:
    print("No New records were received, Do not ingest anything into DynamoDb")
else:
    print("Incremental Data Received..")
    print(incrementalEmpDf)
    print( incrementalEmpDf.show())#line added

    dynamoDf = glueContext.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={"dynamodb.input.tableName": "dynamo_table2",
            "dynamodb.throughput.read.percent": "1.0",
            "dynamodb.splits": "100"
        }
    )
    
    existingDyanmoDf = dynamoDf.toDF()
    #print(existingDyanmoDf.show())#line added
    
    resultDf = None
    
    if existingDyanmoDf.count() != 0:
        
        print("Dynamo DB Table is not empty, so join will happen")
        existingDyanmoDfRenamed = existingDyanmoDf.select("FIRST_NAME").withColumnRenamed("FIRST_NAME","existing_employee_name")
        #print(existingDyanmoDfRenamed.show()) #line added
        
        print("Perform Join condition")
        joinedDf = incrementalEmpDf.join(existingDyanmoDfRenamed, incrementalEmpDf.FIRST_NAME == existingDyanmoDfRenamed.existing_employee_name, "left")
        #print(joinedDf.show())#line added
        
        print("Number of records after join = ",joinedDf.count())
        resultDf = joinedDf.filter("existing_employee_name is null")
#        resultDf = joinedDf.filter("existingDyanmoDfRenamed.FIRST_NAME is null")
        resultDf.drop("existing_employee_name")
    else:
        print("Dynamo DB Table is empty, so no join will happen")
        resultDf = incrementalEmpDf
        print(resultDf)
        
    resultDynamicDf = DynamicFrame.fromDF(resultDf, glueContext, "resultDf")
    
    try:
        glueContext.write_dynamic_frame_from_options(
            frame=resultDynamicDf,
            connection_type="dynamodb",
            connection_options={"dynamodb.output.tableName": "dynamo_table2",
                "dynamodb.throughput.write.percent": "1.0"
            }
        )
        print("Data write in DyanamoDB was successful")
    except Exception as err:
        
        print("Data write in DyanamoDB was not successful : ",str(err))

job.commit()
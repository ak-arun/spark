import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name

from pyspark.sql import Row
import pandas as pd
import io

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)





# Read with custom InputFormat via Hadoop API
rdd = spark.sparkContext.newAPIHadoopFile(
    "s3://BUCKET/sample_data_utf16le.csv",
    "com.ak.spark.utf16le.UTF16LETextInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "org.apache.hadoop.io.Text"
)

def parse_csv_partition(iterator):
    import pandas as pd
    import io
    from pyspark.sql import Row
    
    rows = []
    for row in iterator:
        rows.append(row[1])  # Extract text from (LongWritable, Text) tuple
    
    if not rows:
        return []
    
    csv_text = '\n'.join(rows)
    df_pandas = pd.read_csv(io.StringIO(csv_text))
    return [Row(**row.to_dict()) for _, row in df_pandas.iterrows()]

# Apply parsing and create DataFrame
parsed_rdd = rdd.mapPartitions(parse_csv_partition)

df = spark.createDataFrame(parsed_rdd)

df_with_filename = df.withColumn("filename", input_file_name())





df_with_filename.show()
df_with_filename.printSchema()
job.commit()

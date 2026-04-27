# aws/glue_job.py
# AWS Glue PySpark Job — CloudSieve Deduplication Engine
# Upload this script to S3 and reference it in your Glue Job definition
# Run via: AWS Console > Glue > Jobs > Create Job > Script path = s3://your-bucket/scripts/glue_job.py

import sys
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window
import difflib

# ─────────────────────────────────────────────
# INIT GLUE CONTEXT
# ─────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'job_id', 'input_path', 'output_path', 'fuzzy_col', 'threshold'
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

job_id      = args['job_id']
input_path  = args['input_path']
output_path = args['output_path']
fuzzy_col   = args['fuzzy_col']
threshold   = int(args['threshold'])

dynamodb    = boto3.resource('dynamodb', region_name='us-east-1')
table       = dynamodb.Table('cloudsieve-jobs')

print(f"[CloudSieve Glue] Starting job {job_id}")
print(f"[CloudSieve Glue] Input:  {input_path}")
print(f"[CloudSieve Glue] Output: {output_path}")


# ─────────────────────────────────────────────
# HELPER: UPDATE DYNAMODB STATUS
# ─────────────────────────────────────────────
def update_status(status, extra={}):
    item = {'job_id': job_id, 'status': status}
    item.update(extra)
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression='SET #s = :s',
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={':s': status}
    )
    print(f"[CloudSieve Glue] Status updated: {status}")


# ─────────────────────────────────────────────
# STAGE 1: LOAD DATA
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 1: Loading data from S3")
update_status('PROFILING')

df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
raw_count = df.count()
print(f"[CloudSieve Glue] Loaded {raw_count} records, {len(df.columns)} columns")


# ─────────────────────────────────────────────
# STAGE 2: EXACT DEDUPLICATION
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 2: Exact deduplication")
update_status('EXACT_DEDUP')

df_dedup = df.dropDuplicates()
exact_removed = raw_count - df_dedup.count()
print(f"[CloudSieve Glue] Exact duplicates removed: {exact_removed}")


# ─────────────────────────────────────────────
# STAGE 3: FUZZY DEDUPLICATION
# Uses Levenshtein distance via Spark built-in
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 3: Fuzzy deduplication")
update_status('FUZZY_DEDUP')

fuzzy_removed = 0
if fuzzy_col in df_dedup.columns:
    # Collect values for fuzzy comparison (feasible for datasets up to ~1M rows)
    values = [row[fuzzy_col] for row in df_dedup.select(fuzzy_col).collect() if row[fuzzy_col]]
    to_drop = set()

    for i in range(len(values)):
        for j in range(i + 1, len(values)):
            # Levenshtein ratio approximation
            ratio = difflib.SequenceMatcher(None, str(values[i]), str(values[j])).ratio() * 100
            if ratio >= threshold and values[j] not in to_drop:
                to_drop.add(values[j])
                fuzzy_removed += 1

    if to_drop:
        df_dedup = df_dedup.filter(~F.col(fuzzy_col).isin(list(to_drop)))

    print(f"[CloudSieve Glue] Fuzzy duplicates removed: {fuzzy_removed}")


# ─────────────────────────────────────────────
# STAGE 4: DATA REPAIR
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 4: Data repair")
update_status('REPAIRING')

repairs = 0

# Fill null numeric columns with median
for col_name, dtype in df_dedup.dtypes:
    if dtype in ('int', 'double', 'float', 'bigint', 'long'):
        null_count = df_dedup.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            median_val = df_dedup.approxQuantile(col_name, [0.5], 0.01)[0]
            df_dedup = df_dedup.fillna({col_name: median_val})
            repairs += null_count
            print(f"[CloudSieve Glue] Filled {null_count} nulls in '{col_name}' with median {median_val}")

    # Fix impossible ages
    if 'age' in col_name.lower() and dtype in ('int', 'double', 'bigint'):
        invalid = df_dedup.filter((F.col(col_name) < 0) | (F.col(col_name) > 120)).count()
        if invalid > 0:
            median_age = df_dedup.approxQuantile(col_name, [0.5], 0.01)[0]
            df_dedup = df_dedup.withColumn(
                col_name,
                F.when((F.col(col_name) < 0) | (F.col(col_name) > 120), median_age)
                 .otherwise(F.col(col_name))
            )
            repairs += invalid
            print(f"[CloudSieve Glue] Fixed {invalid} invalid age values")

# Fill null string columns
for col_name, dtype in df_dedup.dtypes:
    if dtype == 'string':
        null_count = df_dedup.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            df_dedup = df_dedup.fillna({col_name: 'Unknown'})
            repairs += null_count

# Flag invalid emails
for col_name, dtype in df_dedup.dtypes:
    if 'email' in col_name.lower() and dtype == 'string':
        df_dedup = df_dedup.withColumn(
            col_name,
            F.when(~F.col(col_name).contains('@'), None).otherwise(F.col(col_name))
        )

print(f"[CloudSieve Glue] Total repairs made: {repairs}")


# ─────────────────────────────────────────────
# STAGE 5: ADD ANOMALY FLAG COLUMN
# Flag records with extreme numeric outliers
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 5: Flagging anomalies")
update_status('ANOMALY_DETECTION')

numeric_cols = [c for c, t in df_dedup.dtypes if t in ('int', 'double', 'float', 'bigint')]
anomaly_count = 0

if numeric_cols:
    # Use IQR method to flag outliers
    col_name = numeric_cols[0]
    q1, q3 = df_dedup.approxQuantile(col_name, [0.25, 0.75], 0.05)
    iqr = q3 - q1
    lower = q1 - 3 * iqr
    upper = q3 + 3 * iqr

    df_dedup = df_dedup.withColumn(
        'anomaly_flag',
        F.when((F.col(col_name) < lower) | (F.col(col_name) > upper), '⚠️ Anomaly')
         .otherwise('✅ Normal')
    )
    anomaly_count = df_dedup.filter(F.col('anomaly_flag') == '⚠️ Anomaly').count()
else:
    df_dedup = df_dedup.withColumn('anomaly_flag', F.lit('✅ Normal'))

print(f"[CloudSieve Glue] Anomalies flagged: {anomaly_count}")


# ─────────────────────────────────────────────
# STAGE 6: CALCULATE CQI SCORE
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 6: Calculating CQI score")
update_status('SCORING')

clean_count  = df_dedup.count()
total_cells  = clean_count * len(df_dedup.columns)
null_total   = sum(df_dedup.filter(F.col(c).isNull()).count() for c in df_dedup.columns)

completeness = round(1 - null_total / max(total_cells, 1), 4)
uniqueness   = round(clean_count / max(raw_count, 1), 4)
validity     = round(1 - anomaly_count / max(clean_count, 1), 4)
consistency  = round((completeness + validity) / 2, 4)
accuracy     = round(min(completeness, validity), 4)
cqi          = round((completeness + uniqueness + validity + consistency + accuracy) / 5 * 100, 2)

cqi_scores = {
    'completeness': round(completeness * 100, 1),
    'uniqueness':   round(uniqueness * 100, 1),
    'validity':     round(validity * 100, 1),
    'consistency':  round(consistency * 100, 1),
    'accuracy':     round(accuracy * 100, 1),
    'cqi_score':    cqi
}

print(f"[CloudSieve Glue] CQI Score: {cqi}/100")
print(f"[CloudSieve Glue] CQI Breakdown: {json.dumps(cqi_scores)}")


# ─────────────────────────────────────────────
# STAGE 7: WRITE CLEAN OUTPUT TO S3
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 7: Writing clean data to S3")
update_status('WRITING')

df_dedup.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)
print(f"[CloudSieve Glue] Clean data written to: {output_path}")


# ─────────────────────────────────────────────
# STAGE 8: SAVE FINAL RESULTS TO DYNAMODB
# ─────────────────────────────────────────────
print("[CloudSieve Glue] Stage 8: Saving results to DynamoDB")

table.update_item(
    Key={'job_id': job_id},
    UpdateExpression='''SET #s = :s, raw_count = :r, clean_count = :c,
        exact_removed = :e, fuzzy_removed = :f,
        total_repairs = :rp, anomaly_count = :a,
        cqi_score = :cqi, cqi_details = :cqid,
        output_path = :op''',
    ExpressionAttributeNames={'#s': 'status'},
    ExpressionAttributeValues={
        ':s':   'COMPLETED',
        ':r':   raw_count,
        ':c':   clean_count,
        ':e':   exact_removed,
        ':f':   fuzzy_removed,
        ':rp':  repairs,
        ':a':   anomaly_count,
        ':cqi': str(cqi),
        ':cqid': json.dumps(cqi_scores),
        ':op':  output_path
    }
)

print(f"[CloudSieve Glue] ✅ Job {job_id} complete!")
print(f"[CloudSieve Glue] Raw: {raw_count} → Clean: {clean_count} | CQI: {cqi}/100")

job.commit()

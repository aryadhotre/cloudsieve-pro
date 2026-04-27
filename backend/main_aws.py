# backend/main_aws.py
# CloudSieve — FastAPI Backend with full AWS Integration
# Switch between local mode and AWS mode via USE_AWS env variable
#
# Local mode:  USE_AWS=false uvicorn main_aws:app --reload --port 8000
# AWS mode:    USE_AWS=true  uvicorn main_aws:app --reload --port 8000

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import numpy as np
import json
import os
import uuid
import shutil
import boto3
import io
from pathlib import Path
from engine import run_full_pipeline


USE_AWS      = os.environ.get('USE_AWS', 'false').lower() == 'true'
AWS_REGION   = os.environ.get('AWS_REGION',   'us-east-1')
RAW_BUCKET   = os.environ.get('RAW_BUCKET',   'cloudsieve-raw')
CLEAN_BUCKET = os.environ.get('CLEAN_BUCKET', 'cloudsieve-clean')
DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE', 'cloudsieve-jobs')
SNS_TOPIC    = os.environ.get('SNS_TOPIC',    '')
CQI_ALERT_THRESHOLD = int(os.environ.get('CQI_ALERT_THRESHOLD', '60'))

# Local fallback dirs
UPLOAD_DIR = Path("uploads")
OUTPUT_DIR = Path("outputs")
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# AWS clients (only init if USE_AWS=true)
if USE_AWS:
    s3       = boto3.client('s3',       region_name=AWS_REGION)
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    sns      = boto3.client('sns',      region_name=AWS_REGION)
    ddb_table = dynamodb.Table(DYNAMO_TABLE)

app = FastAPI(title="CloudSieve API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print(f"[CloudSieve] Starting in {'☁️  AWS' if USE_AWS else '💻 Local'} mode")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def save_file_local(job_id, filename, file_obj):
    path = UPLOAD_DIR / f"{job_id}_{filename}"
    with open(path, "wb") as f:
        shutil.copyfileobj(file_obj, f)
    return str(path)

def save_file_s3(job_id, filename, file_obj):
    key = f"uploads/{job_id}_{filename}"
    s3.upload_fileobj(file_obj, RAW_BUCKET, key)
    return f"s3://{RAW_BUCKET}/{key}"

def load_file_local(job_id):
    files = list(UPLOAD_DIR.glob(f"{job_id}_*"))
    if not files:
        raise FileNotFoundError(f"No file found for job {job_id}")
    return str(files[0])

def load_file_s3(job_id):
    # List objects in S3 with prefix
    resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=f"uploads/{job_id}_")
    if not resp.get('Contents'):
        raise FileNotFoundError(f"No S3 file found for job {job_id}")
    key = resp['Contents'][0]['Key']
    local_path = UPLOAD_DIR / f"{job_id}_temp.csv"
    s3.download_file(RAW_BUCKET, key, str(local_path))
    return str(local_path)

def save_result_local(job_id, df):
    path = OUTPUT_DIR / f"{job_id}_clean.csv"
    df.to_csv(path, index=False)

def save_result_s3(job_id, df):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=f"clean/{job_id}_clean.csv",
        Body=csv_buffer.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )

def save_job_metadata(job_id, data):
    if USE_AWS:
        # Convert all values to DynamoDB-compatible types
        item = {k: str(v) if isinstance(v, float) else v for k, v in data.items()}
        item['job_id'] = job_id
        ddb_table.put_item(Item=item)
    else:
        meta_path = OUTPUT_DIR / f"{job_id}_meta.json"
        with open(meta_path, 'w') as f:
            json.dump({**data, 'job_id': job_id}, f)

def get_job_metadata_aws(job_id):
    resp = ddb_table.get_item(Key={'job_id': job_id})
    return resp.get('Item')

def send_alert(job_id, cqi_score, message):
    if USE_AWS and SNS_TOPIC:
        sns.publish(
            TopicArn=SNS_TOPIC,
            Subject=f"CloudSieve Alert — Job {job_id}",
            Message=f"Job {job_id} completed.\nCQI Score: {cqi_score}/100\n\n{message}"
        )


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "message": "CloudSieve API ✅",
        "mode": "AWS" if USE_AWS else "Local",
        "version": "2.0.0"
    }

@app.get("/health")
def health():
    checks = {"api": "ok", "mode": "aws" if USE_AWS else "local"}
    if USE_AWS:
        try:
            s3.head_bucket(Bucket=RAW_BUCKET)
            checks["s3_raw"] = "ok"
        except Exception as e:
            checks["s3_raw"] = f"error: {e}"
        try:
            ddb_table.load()
            checks["dynamodb"] = "ok"
        except Exception as e:
            checks["dynamodb"] = f"error: {e}"
    return checks


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload CSV — saves to local or S3 depending on mode"""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    job_id = str(uuid.uuid4())[:8]

    try:
        if USE_AWS:
            file_path = save_file_s3(job_id, file.filename, file.file)
            # Download temp copy for preview
            local_path = load_file_s3(job_id)
        else:
            local_path = save_file_local(job_id, file.filename, file.file)
            file_path = local_path

        df = pd.read_csv(local_path)
        preview = df.head(5).replace({np.nan: None}).to_dict(orient="records")

        meta = {
            "filename": file.filename,
            "file_path": file_path,
            "rows": len(df),
            "columns": list(df.columns),
            "status": "UPLOADED"
        }
        save_job_metadata(job_id, meta)

        return {
            "job_id":   job_id,
            "filename": file.filename,
            "rows":     len(df),
            "columns":  list(df.columns),
            "preview":  preview,
            "mode":     "aws" if USE_AWS else "local"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run/{job_id}")
async def run_pipeline(job_id: str, body: dict):
    """Run full CloudSieve pipeline"""
    fuzzy_col = body.get("fuzzy_col", "name")
    threshold = int(body.get("threshold", 85))

    try:
        if USE_AWS:
            file_path = load_file_s3(job_id)
        else:
            file_path = load_file_local(job_id)

        result = run_full_pipeline(file_path, fuzzy_col, threshold)

        # Save clean output
        clean_df = pd.DataFrame(result["clean_data"])
        if USE_AWS:
            save_result_s3(job_id, clean_df)
        else:
            save_result_local(job_id, clean_df)

        # Save job metadata
        save_job_metadata(job_id, {
            "status":         "COMPLETED",
            "raw_count":      result["raw_count"],
            "clean_count":    result["clean_count"],
            "exact_removed":  result["exact_removed"],
            "fuzzy_removed":  result["fuzzy_removed"],
            "total_repairs":  result["total_repairs"],
            "anomaly_count":  result["anomaly_count"],
            "cqi_score":      result["cqi"]["cqi_score"],
            "filename":       body.get("filename", ""),
        })

        # Send SNS alert if CQI is low
        cqi = result["cqi"]["cqi_score"]
        if cqi < CQI_ALERT_THRESHOLD:
            send_alert(
                job_id, cqi,
                f"⚠️ Data quality below threshold ({CQI_ALERT_THRESHOLD}).\n"
                f"Exact duplicates: {result['exact_removed']}\n"
                f"Fuzzy duplicates: {result['fuzzy_removed']}\n"
                f"Repairs made: {result['total_repairs']}"
            )

        return JSONResponse(content=result)

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Job not found. Please upload first.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/download/{job_id}")
def download_clean(job_id: str):
    """Download cleaned CSV from local or S3"""
    if USE_AWS:
        try:
            obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=f"clean/{job_id}_clean.csv")
            csv_data = obj['Body'].read()
            return StreamingResponse(
                io.BytesIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=cloudsieve_{job_id}_clean.csv"}
            )
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"File not found in S3: {e}")
    else:
        from fastapi.responses import FileResponse
        path = OUTPUT_DIR / f"{job_id}_clean.csv"
        if not path.exists():
            raise HTTPException(status_code=404, detail="Clean file not found.")
        return FileResponse(path, media_type="text/csv",
                           filename=f"cloudsieve_{job_id}_clean.csv")


@app.get("/job/{job_id}")
def get_job(job_id: str):
    """Get job status and metadata"""
    if USE_AWS:
        item = get_job_metadata_aws(job_id)
        if not item:
            raise HTTPException(status_code=404, detail="Job not found")
        return item
    else:
        meta_path = OUTPUT_DIR / f"{job_id}_meta.json"
        if not meta_path.exists():
            raise HTTPException(status_code=404, detail="Job not found")
        with open(meta_path) as f:
            return json.load(f)


@app.get("/history")
def get_history():
    """List all completed jobs"""
    if USE_AWS:
        try:
            resp = ddb_table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('status').eq('COMPLETED')
            )
            return resp.get('Items', [])
        except Exception as e:
            return []
    else:
        results = []
        for f in OUTPUT_DIR.glob("*_meta.json"):
            with open(f) as fp:
                try:
                    results.append(json.load(fp))
                except:
                    pass
        return results


@app.get("/sample")
def get_sample():
    """Serve sample dirty CSV for testing"""
    from fastapi.responses import FileResponse
    sample = Path("sample_data.csv")
    if sample.exists():
        return FileResponse(sample, media_type="text/csv", filename="sample_data.csv")
    raise HTTPException(status_code=404, detail="Sample file not found")

#!/bin/bash
# aws/deploy.sh
# CloudSieve AWS Deployment Script
# Run: chmod +x deploy.sh && ./deploy.sh

set -e  # Exit on any error

echo "================================================"
echo "   ☁️  CloudSieve — AWS Deployment Script"
echo "================================================"

# ─── CONFIG — EDIT THESE ───
STACK_NAME="cloudsieve"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SCRIPTS_BUCKET="${STACK_NAME}-scripts-${ACCOUNT_ID}"

echo ""
echo "📋 Deployment Config:"
echo "   Stack:     $STACK_NAME"
echo "   Region:    $REGION"
echo "   AccountID: $ACCOUNT_ID"
echo ""

# ─── STEP 1: DEPLOY CLOUDFORMATION STACK ───
echo "🏗️  Step 1: Deploying CloudFormation stack..."
aws cloudformation deploy \
  --template-file cloudformation.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION \
  --parameter-overrides Environment=dev

echo "✅ CloudFormation stack deployed"

# ─── STEP 2: GET STACK OUTPUTS ───
echo ""
echo "📤 Step 2: Getting stack outputs..."
RAW_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query "Stacks[0].Outputs[?ExportName=='${STACK_NAME}-raw-bucket'].OutputValue" \
  --output text --region $REGION)

CLEAN_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query "Stacks[0].Outputs[?ExportName=='${STACK_NAME}-clean-bucket'].OutputValue" \
  --output text --region $REGION)

SNS_TOPIC=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query "Stacks[0].Outputs[?OutputKey=='AlertTopicArn'].OutputValue" \
  --output text --region $REGION)

echo "   Raw bucket:   $RAW_BUCKET"
echo "   Clean bucket: $CLEAN_BUCKET"
echo "   SNS topic:    $SNS_TOPIC"

# ─── STEP 3: UPLOAD GLUE SCRIPT TO S3 ───
echo ""
echo "📁 Step 3: Uploading Glue job script to S3..."
aws s3 cp glue_job.py s3://${SCRIPTS_BUCKET}/scripts/glue_job.py --region $REGION
echo "✅ Glue script uploaded"

# ─── STEP 4: PACKAGE AND UPLOAD LAMBDA ───
echo ""
echo "📦 Step 4: Packaging and uploading Lambda function..."
cd ..
zip -j lambda.zip aws/lambda_trigger.py
aws s3 cp lambda.zip s3://${SCRIPTS_BUCKET}/lambda/lambda.zip --region $REGION

LAMBDA_NAME="${STACK_NAME}-trigger"
aws lambda update-function-code \
  --function-name $LAMBDA_NAME \
  --s3-bucket $SCRIPTS_BUCKET \
  --s3-key lambda/lambda.zip \
  --region $REGION > /dev/null

echo "✅ Lambda function updated"
rm lambda.zip
cd aws

# ─── STEP 5: SUBSCRIBE EMAIL TO SNS (OPTIONAL) ───
echo ""
read -p "📧 Enter email for quality alerts (or press Enter to skip): " EMAIL
if [ -n "$EMAIL" ]; then
  aws sns subscribe \
    --topic-arn $SNS_TOPIC \
    --protocol email \
    --notification-endpoint $EMAIL \
    --region $REGION > /dev/null
  echo "✅ Subscribed $EMAIL to alerts. Check your inbox to confirm."
fi

# ─── STEP 6: GENERATE .env FILE FOR BACKEND ───
echo ""
echo "📝 Step 6: Generating .env file for backend..."
cat > ../backend/.env.aws << EOF
USE_AWS=true
AWS_REGION=${REGION}
RAW_BUCKET=${RAW_BUCKET}
CLEAN_BUCKET=${CLEAN_BUCKET}
DYNAMO_TABLE=cloudsieve-jobs
SNS_TOPIC=${SNS_TOPIC}
CQI_ALERT_THRESHOLD=60
EOF

echo "✅ .env.aws file created in backend/"

# ─── DONE ───
echo ""
echo "================================================"
echo "   ✅ CloudSieve Deployment Complete!"
echo "================================================"
echo ""
echo "📋 Next steps:"
echo "   1. Run backend with AWS:"
echo "      cd backend"
echo "      export \$(cat .env.aws | xargs)"
echo "      uvicorn main_aws:app --reload --port 8000"
echo ""
echo "   2. Upload a CSV to test:"
echo "      aws s3 cp sample_data.csv s3://${RAW_BUCKET}/"
echo ""
echo "   3. View CloudWatch dashboard:"
echo "      https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=${STACK_NAME}-dashboard"
echo ""

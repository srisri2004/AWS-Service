#!/usr/bin/env bash
# Deploys the whole pipeline. Requires: AWS CLI v2 configured (aws configure) with admin-like rights.
# Usage: PROJECT=salesdl ALERT_EMAIL=you@example.com ./scripts/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT="${PROJECT:-salesdl}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region)}}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
DATA_BUCKET="${PROJECT}-data-${ACCOUNT}-${REGION}"
ART_BUCKET="${PROJECT}-artifacts-${ACCOUNT}-${REGION}"
STACK="${PROJECT}-stack"
echo "Account=$ACCOUNT Region=$REGION Stack=$STACK"

if ! aws s3api head-bucket --bucket "$ART_BUCKET" 2>/dev/null; then
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$ART_BUCKET" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$ART_BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
  aws s3api put-public-access-block --bucket "$ART_BUCKET" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-encryption --bucket "$ART_BUCKET" --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
fi

aws s3 cp glue/etl_job.py "s3://${ART_BUCKET}/glue/etl_job.py"

aws cloudformation package --template-file infra/template.yaml \
  --s3-bucket "$ART_BUCKET" --s3-prefix lambda --output-template-file infra/packaged.yaml

aws cloudformation deploy --template-file infra/packaged.yaml --stack-name "$STACK" \
  --capabilities CAPABILITY_IAM --region "$REGION" \
  --parameter-overrides ProjectName="$PROJECT" DataBucketName="$DATA_BUCKET" \
    ArtifactBucketName="$ART_BUCKET" AlertEmail="${ALERT_EMAIL:-}"

echo; echo "Deployed. Next: ./scripts/demo.sh"

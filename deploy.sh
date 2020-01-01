#!/bin/bash

export BUCKET="your-bucket-name"

GOOS=linux go build main.go
zip awair-to-s3.zip main
aws s3 cp awair-to-s3.zip s3://$BUCKET/lambda/
aws lambda update-function-code \
  --function-name awair-to-s3 \
	--s3-bucket $BUCKET \
	--s3-key lambda/awair-to-s3.zip

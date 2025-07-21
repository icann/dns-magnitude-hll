#!/bin/sh
#
# Script to upload DNS Magnitude datasets to a S3 bucket. Credentials must be configured
# in the environment or via the AWS CLI configuration.

BUCKET="bucket.example.com"

for dataset in $@; do
        date=`dnsmag report --source dummy $dataset | jq .date`
        IFS='-'; date_array=($date); unset IFS
        date_year=${date_array[0]}
        date_month=${date_array[1]}
        date_day=${date_array[2]}
        identifier=`openssl dgst -hex $dataset | sed 's/.*= //'`
        destination="year=${date_year}/month=${date_month}/day=${date_day}/id=${identifier}"
        aws s3 cp $dataset s3://$BUCKET/$destination
done

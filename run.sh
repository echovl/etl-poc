#!/bin/bash

python mongo2bq.py \
--output engaged-ground-326604:test.customers \
--project engaged-ground-326604 \
--runner DataflowRunner \
--region us-central1 \
--temp_location gs://etl-bucket1/temp/  \
--requirements_file requirements.txt \
--save_main_session \
--job_name poc

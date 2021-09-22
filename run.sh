#!/bin/bash

python initial_load.py \
--output engaged-ground-326604:test.orders \
--project engaged-ground-326604 \
--runner DataflowRunner \
--region us-central1 \
--temp_location gs://etl-bucket1/temp/  \
--staging_location gs://etl-bucket1/stage/  \
--requirements_file requirements.txt \
--save_main_session \
--job_name initial-load-3

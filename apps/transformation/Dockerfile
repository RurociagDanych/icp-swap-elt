# Silver transformation + data-quality Lambda container image.
# pyiceberg + pyarrow exceed the Lambda zip/layer size limit, hence the
# container image. One image serves both handlers (silver_job.handler and
# quality_job.handler); the Lambda function picks its entrypoint via
# ImageConfig.Command, set in infra/transformation/compute.
#
# TODO(supply-chain, NOT VERIFIED): pin both FROM lines by digest
#   (public.ecr.aws/lambda/python:3.12@sha256:<digest>). The digest was not pinned because it
#   could not be verified offline (image absent from the local cache when this was written).
#   Resolve it with network access, e.g.
#     docker pull public.ecr.aws/lambda/python:3.12 &&
#     docker inspect --format '{{index .RepoDigests 0}}' public.ecr.aws/lambda/python:3.12
#   and use the SAME digest in both stages.
# TODO(supply-chain, NOT VERIFIED): hash-pin the Python dependencies
#   (pip-compile --generate-hashes src/requirements.txt, then install with
#   --require-hashes). Generating the hashes needs the package index, so it was not done
#   offline; versions are pinned exactly in src/requirements.txt.
FROM public.ecr.aws/lambda/python:3.12 AS build

COPY src/requirements.txt .
RUN python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt --target /opt/python

FROM public.ecr.aws/lambda/python:3.12

COPY --from=build /opt/python ${LAMBDA_TASK_ROOT}

COPY src/silver_job.py ${LAMBDA_TASK_ROOT}/
COPY src/seam_reconciliation.py ${LAMBDA_TASK_ROOT}/
COPY src/silver_contract.py ${LAMBDA_TASK_ROOT}/
COPY src/silver_csv.py ${LAMBDA_TASK_ROOT}/
COPY src/silver_persistence.py ${LAMBDA_TASK_ROOT}/
COPY src/fact_compaction.py ${LAMBDA_TASK_ROOT}/
COPY src/table_maintenance.py ${LAMBDA_TASK_ROOT}/
COPY src/iceberg_scan_safety.py ${LAMBDA_TASK_ROOT}/
COPY src/token_dimension.py ${LAMBDA_TASK_ROOT}/
COPY src/write_planning.py ${LAMBDA_TASK_ROOT}/
COPY src/quality_job.py ${LAMBDA_TASK_ROOT}/
COPY src/quality_dbt ${LAMBDA_TASK_ROOT}/quality_dbt
COPY src/utils ${LAMBDA_TASK_ROOT}/utils

# Default handler = <module>.<function>; the quality Lambda overrides
# this at the function level (ImageConfig.Command = ["quality_job.handler"]).
CMD ["silver_job.handler"]

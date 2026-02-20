#!/usr/bin/env bash
set -euo pipefail

# Generate gRPC Python stubs from proto definitions.
# Requires: pip install grpcio-tools
#
# Run from the python-sdk/ directory:
#   ./generate_proto.sh

PROTO_DIR="proto/corvo/v1"
PROTO_FILE="$PROTO_DIR/worker.proto"

# Worker package
python -m grpc_tools.protoc \
  -I proto \
  --python_out=worker/corvo_worker/gen \
  --pyi_out=worker/corvo_worker/gen \
  --grpc_python_out=worker/corvo_worker/gen \
  "$PROTO_FILE"

# Client package (same stubs)
python -m grpc_tools.protoc \
  -I proto \
  --python_out=client/corvo_client/gen \
  --pyi_out=client/corvo_client/gen \
  --grpc_python_out=client/corvo_client/gen \
  "$PROTO_FILE"

# Create __init__.py in all gen package dirs
for pkg in worker/corvo_worker client/corvo_client; do
  touch "$pkg/gen/__init__.py"
  touch "$pkg/gen/corvo/__init__.py"
  touch "$pkg/gen/corvo/v1/__init__.py"
done

# Fix absolute imports in generated grpc stubs to use relative imports.
# grpc_tools.protoc generates `from corvo.v1 import worker_pb2` which fails
# when the gen/ directory is nested inside a package.  Change to relative.
for f in worker/corvo_worker/gen/corvo/v1/worker_pb2_grpc.py \
         client/corvo_client/gen/corvo/v1/worker_pb2_grpc.py; do
  sed -i 's/^from corvo\.v1 import worker_pb2/from . import worker_pb2/' "$f"
done

echo "Proto generation complete."

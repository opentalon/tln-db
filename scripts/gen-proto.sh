#!/usr/bin/env bash
# Regenerate the gRPC/protobuf Go stubs from proto/talondb.proto.
#
# Requires (matching go.mod pins):
#   protoc, protoc-gen-go v1.36.x, protoc-gen-go-grpc v1.6.x
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Output lands in proto/talondbpb/ (derived from the go_package option
# via --go_opt=module).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

MODULE=github.com/opentalon/talon-db

protoc -I . \
  --go_out=. --go_opt=module="$MODULE" \
  --go-grpc_out=. --go-grpc_opt=module="$MODULE" \
  proto/talondb.proto

echo "Regenerated proto/talondbpb from proto/talondb.proto"

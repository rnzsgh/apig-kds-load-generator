REGION ?= us-east-1
PROFILE ?= test
ENV_NAME ?= dev


.PHONY: protoc
protoc:
	@protoc -I=./ --go_out=./ ./message.proto

.PHONY: install-protoc
install-protoc:
	@go install google.golang.org/protobuf/cmd/protoc-gen-go

.PHONY: linux
linux:
	@GOOS=linux GOARCH=amd64 go build -o loader main.go

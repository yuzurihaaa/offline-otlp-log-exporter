init:
	git submodule init
	git submodule update
	dart pub get
	dart pub global activate protoc_plugin 21.1.2
	cd lib/src/proto && \
		protoc --proto_path opentelemetry-proto --dart_out . \
			opentelemetry-proto/opentelemetry/proto/common/v1/common.proto \
			opentelemetry-proto/opentelemetry/proto/resource/v1/resource.proto \
			opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto \
			opentelemetry-proto/opentelemetry/proto/logs/v1/logs.proto


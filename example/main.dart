import 'dart:async';
import 'dart:io';

import 'package:offline_otlp_log_exporter/offline_otlp_log_exporter.dart';
import 'package:opentelemetry/api.dart';
import 'package:opentelemetry/src/api/open_telemetry.dart';
import 'package:opentelemetry/src/experimental_api.dart' as api;
import 'package:opentelemetry/src/experimental_sdk.dart';

void main() async {
  final processor = OfflineOTLPLogExporter(
    dir: Directory('.'),
    uri: Uri.parse(''),
    headers: {},
  );
  final provider = LoggerProvider(
    resource: Resource([Attribute.fromString("app.name", "test")]),
    processors: [
      SimpleLogRecordProcessor(exporter: processor),
    ],
  );
  registerGlobalLogProvider(provider);

  globalLogProvider
      .get('test logger')
      .emit(body: 'test otel log', severityNumber: api.Severity.error);
  globalLogProvider
      .get('test logger')
      .emit(body: 'test otel log2', severityNumber: api.Severity.error);

  await Future.delayed(const Duration(seconds: 30));

  // await provider.forceFlush();
  // await provider.shutdown();
  exit(0);
}

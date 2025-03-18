import 'dart:async';
import 'dart:io';

import 'package:offline_otlp_log_exporter/offline_otlp_log_exporter.dart';
import 'package:opentelemetry/api.dart' as api;
import 'package:opentelemetry/sdk.dart' as sdk;

void main() async {
  final processor = OfflineOTLPLogExporter(
    dir: Directory('.'),
    uri: Uri.parse(''),
    headers: {},
  );
  final provider = sdk.LoggerProvider(
    resource: sdk.Resource([api.Attribute.fromString("app.name", "test")]),
    processors: [
      sdk.SimpleLogRecordProcessor(exporter: processor),
    ],
  );
  api.registerGlobalLogProvider(provider);

  api.globalLogProvider
      .get('test logger')
      .emit(body: 'test otel log', severityNumber: api.Severity.error);
  api.globalLogProvider
      .get('test logger')
      .emit(body: 'test otel log2', severityNumber: api.Severity.error);

  await Future.delayed(const Duration(seconds: 30));

  // await provider.forceFlush();
  // await provider.shutdown();
  exit(0);
}

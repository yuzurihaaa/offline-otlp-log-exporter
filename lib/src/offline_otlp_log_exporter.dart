import 'dart:io';

import 'package:opentelemetry/src/api/common/export_result.dart';
import 'package:opentelemetry/src/experimental_sdk.dart';
import 'package:path/path.dart';

import 'file_writer.dart';

class OfflineOTLPLogExporter implements LogRecordExporter {
  final Directory dir;
  final Duration logTtl;
  final FileWriter fileWriter;

  OfflineOTLPLogExporter({
    required this.dir,
    this.logTtl = const Duration(days: 30),
    FileWriter? fileWriter,
  }) : fileWriter = fileWriter ?? FileWriterImpl(dir: dir, logTtl: logTtl);

  @override
  Future<ExportResult> export(List<ReadableLogRecord> logs) async {
    _ensureFileExist();

    // TODO: implement export
    throw UnimplementedError();
  }

  @override
  Future<void> shutdown() {
    // TODO: implement shutdown
    throw UnimplementedError();
  }

  Future<void> _ensureFileExist() async {
    final logDirectory = Directory(join(dir.path, 'logs'));
    if (!await logDirectory.exists()) {
      await logDirectory.create(recursive: true);
    }
  }
}

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';

import 'package:collection/collection.dart';
import 'package:fixnum/fixnum.dart';
import 'package:http/http.dart' as http;
import 'package:intl/intl.dart';
import 'package:offline_otlp_log_exporter/src/proto/opentelemetry/proto/collector/logs/v1/logs_service.pb.dart'
    as pb_logs_service;
import 'package:offline_otlp_log_exporter/src/proto/opentelemetry/proto/common/v1/common.pb.dart'
    as pb_common;
import 'package:offline_otlp_log_exporter/src/proto/opentelemetry/proto/logs/v1/logs.pb.dart'
    as pb_logs;
import 'package:offline_otlp_log_exporter/src/proto/opentelemetry/proto/logs/v1/logs.pbenum.dart'
    as pg_logs_enum;
import 'package:offline_otlp_log_exporter/src/proto/opentelemetry/proto/resource/v1/resource.pb.dart'
    as pb_resource;
import 'package:offline_otlp_log_exporter/src/utils.dart';
import 'package:opentelemetry/sdk.dart' as sdk;
import 'package:opentelemetry/src/api/common/export_result.dart';
import 'package:opentelemetry/src/experimental_sdk.dart' as sdk;
import 'package:path/path.dart';

import 'file_writer.dart';

class OfflineOTLPLogExporter implements sdk.LogRecordExporter {
  final Directory dir;
  final Duration logTtl;
  final Duration scheduleDelay;
  final Duration exportTimeout;
  final Uri uri;
  final int maxExportBatchSize;
  final FileWriter _fileWriter;
  final Map<String, String> headers;

  OfflineOTLPLogExporter({
    required this.dir,
    required this.uri,
    this.scheduleDelay = const Duration(seconds: 5),
    this.exportTimeout = const Duration(seconds: 30),
    this.maxExportBatchSize = 50,
    this.logTtl = const Duration(days: 30),
    this.headers = const <String, String>{},
    FileWriter? fileWriter,
  }) : _fileWriter = fileWriter ?? FileWriterImpl(dir: dir, logTtl: logTtl);

  Timer? _timer;

  @override
  Future<ExportResult> export(List<sdk.ReadableLogRecord> logs) async {
    await Future.delayed(const Duration(seconds: 2));
    await _ensureFileExist();

    final Iterable<pb_logs.ResourceLogs> protobufs = _logsToProtobuf(logs);

    for (final protobuf in protobufs) {
      await _fileWriter.write(protobuf.writeToJsonMap());
    }
    _initTimer();
    return ExportResult(code: ExportResultCode.success);
  }

  @override
  Future<void> shutdown() async {
    _timer?.cancel();
    _timer = null;
    await _fileWriter.close();
  }

  void _initTimer() {
    if (_timer != null) return;
    _timer = Timer(scheduleDelay, () {
      final arg = SyncArg(
        path: join(dir.path, 'logs'),
        url: uri.toString(),
        batchSize: maxExportBatchSize,
        delayInMs: scheduleDelay.inMilliseconds,
        headers: headers,
        timeoutInMs: exportTimeout.inMilliseconds,
      );
      Isolate.spawn<SyncArg>(_sync, arg)
          .then<Object?>((e) => e)
          .catchError((e, st) {
        print("_sync error!!!!");
        print(e);
        print(st);
        return null;
      }).then((lastLogAt) async {
        _timer?.cancel();
        _timer = null;
      }).whenComplete(_initTimer);
    });
  }

  Future<void> _ensureFileExist() async {
    final logDirectory = Directory(join(dir.path, 'logs'));
    if (!await logDirectory.exists()) {
      await logDirectory.create(recursive: true);
    }
  }

  /// Group and construct the protobuf equivalent of the given list of [api.LogRecord]s.
  /// Logs are grouped by a trace provider's [sdk.Resource] and a tracer's
  /// [sdk.InstrumentationScope].
  Iterable<pb_logs.ResourceLogs> _logsToProtobuf(
    List<sdk.ReadableLogRecord> logRecords,
  ) {
    // use a map of maps to group spans by resource and instrumentation library
    final rsm = <sdk.Resource,
        Map<sdk.InstrumentationScope, List<pb_logs.LogRecord>>>{};
    for (final logRecord in logRecords) {
      final il = rsm[logRecord.resource] ??
          <sdk.InstrumentationScope, List<pb_logs.LogRecord>>{};

      if (logRecord.instrumentationScope != null) {
        il[logRecord.instrumentationScope!] =
            il[logRecord.instrumentationScope] ?? <pb_logs.LogRecord>[]
              ..add(_logToProtobuf(logRecord));
      }
      if (logRecord.resource != null) {
        rsm[logRecord.resource!] = il;
      }
    }

    final rss = <pb_logs.ResourceLogs>[];
    for (final il in rsm.entries) {
      // for each distinct resource, construct the protobuf equivalent
      final attrs = <pb_common.KeyValue>[];
      for (final attr in il.key.attributes.keys) {
        attrs.add(pb_common.KeyValue(
            key: attr,
            value: _attributeValueToProtobuf(il.key.attributes.get(attr)!)));
      }

      final rs = pb_logs.ResourceLogs(
          resource: pb_resource.Resource(attributes: attrs));
      // for each distinct instrumentation library, construct the protobuf equivalent
      for (final ils in il.value.entries) {
        rs.scopeLogs.add(pb_logs.ScopeLogs(
            logRecords: ils.value,
            scope: pb_common.InstrumentationScope(
                name: ils.key.name, version: ils.key.version)));
      }
      rss.add(rs);
    }
    return rss;
  }

  pb_logs.LogRecord _logToProtobuf(sdk.ReadableLogRecord log) {
    var spanId = <int>[];
    var traceId = <int>[];
    if (log.spanContext != null) {
      spanId = log.spanContext!.spanId.get();
      traceId = log.spanContext!.traceId.get();
    }
    return pb_logs.LogRecord(
        timeUnixNano: log.hrTime,
        severityNumber: log.severityNumber != null
            ? pg_logs_enum.SeverityNumber.valueOf(log.severityNumber!.index)
            : null,
        severityText: log.severityText,
        droppedAttributesCount: log.droppedAttributesCount,
        body: _attributeONEValueToProtobuf(log.body),
        attributes: (log.attributes?.keys ?? []).map((key) =>
            pb_common.KeyValue(
                key: key,
                value: _attributeValueToProtobuf(log.attributes!.get(key)!))),
        spanId: spanId,
        traceId: traceId,
        observedTimeUnixNano: log.hrTimeObserved);
  }

  pb_common.AnyValue _attributeONEValueToProtobuf(Object value) {
    switch (value.runtimeType) {
      case String:
        return pb_common.AnyValue(stringValue: value as String);
      case bool:
        return pb_common.AnyValue(boolValue: value as bool);
      case double:
        return pb_common.AnyValue(doubleValue: value as double);
      case int:
        return pb_common.AnyValue(intValue: Int64(value as int));
    }
    return pb_common.AnyValue();
  }

  pb_common.AnyValue _attributeValueToProtobuf(Object value) {
    if (value is String) {
      return pb_common.AnyValue(stringValue: value);
    }
    if (value is bool) {
      return pb_common.AnyValue(boolValue: value);
    }
    if (value is double) {
      return pb_common.AnyValue(doubleValue: value);
    }
    if (value is int) {
      return pb_common.AnyValue(intValue: Int64(value));
    }
    if (value is List<String> ||
        value is List<bool> ||
        value is List<double> ||
        value is List<int>) {
      final output = <pb_common.AnyValue>[];
      final values = value as List;
      for (final i in values) {
        output.add(_attributeValueToProtobuf(i));
      }
      return pb_common.AnyValue(
          arrayValue: pb_common.ArrayValue(values: output));
    }
    return pb_common.AnyValue();
  }
}

class SyncArg {
  final String path;
  final String url;

  /// Delay for sending batch of logs to Loki.
  final int delayInMs;

  final int batchSize;

  /// Request timeout. Same value will be set for connect and receive timeout.
  final int timeoutInMs;

  /// Additional data to be added in request header.
  final Map<String, String> headers;

  final bool shouldCompress;

  SyncArg({
    required this.path,
    required this.url,
    required this.delayInMs,
    required this.batchSize,
    required this.timeoutInMs,
    required this.headers,
    this.shouldCompress = true,
  });
}

@pragma('vm:entry-point')
Future<void> _sync(SyncArg arg) async {
  if (arg.url.isEmpty == true) {
    print("url is empty. Ignoring send to remote");
    return;
  }
  final metadataFile = await _ensureMetadataFileExist(arg.path);
  final content = await metadataFile.readAsString().then(jsonDecode);
  final lastFile = File(join(arg.path, content['file']));
  final lastLine = content['lastLine'];

  print('processing file');
  print('file ${lastFile.path}');
  print('line $lastLine');

  // read all logs
  final logs = await lastFile
      .openRead()
      .transform(utf8.decoder)
      .transform(const LineSplitter())
      .skip(lastLine)
      .toList();

  if (logs.isEmpty) {
    final fileDate = _getDateTimeFromFile(lastFile);
    if (fileDate == null) return;
    final nowDateOnly = DateTime.now().copyWith(
      hour: 0,
      minute: 0,
      second: 0,
      microsecond: 0,
      millisecond: 0,
    );
    if (fileDate.compareTo(nowDateOnly) < 0) {
      final nextLogFile = await _nextNextLogFile(arg.path, fileDate);
      await _ensureMetadataFileExist(
          arg.path,
          FileMetadataInfo(
            fileDate: _getDateTimeFromFile(nextLogFile)!,
          ));
    }
  }

  final batches = logs.slices(arg.batchSize);

  for (var batch in batches) {
    final out = <pb_logs.ResourceLogs>[];
    for (final e in batch) {
      out.add(pb_logs.ResourceLogs()..mergeFromJson(e));
      await _send(Uri.parse(arg.url), out, arg.headers);
    }
    try {
      await _ensureMetadataFileExist(
        arg.path,
        FileMetadataInfo(
          fileDate: _getDateTimeFromFile(lastFile)!,
          lastLine: lastLine + batch.length,
        ),
      );
    } catch (e) {
      print(e);
    }
  }
}

Future<FileSystemEntity> _nextNextLogFile(
  String path,
  DateTime lastFileDate,
) async {
  final files = await Directory(path).list().where((file) {
    final date = _getDateTimeFromFile(file);
    return date != null && date.compareTo(DateTime.now()) > 0;
  }).toList();
  files.sort((a, b) {
    final aDate = _getDateTimeFromFile(a);
    final bDate = _getDateTimeFromFile(b);
    return aDate!.compareTo(bDate!);
  });
  return files.first;
}

DateTime? _getDateTimeFromFile(FileSystemEntity file) =>
    DateTime.tryParse(basename(file.path).split('.').first);

class FileMetadataInfo {
  final DateTime fileDate;
  final int lastLine;

  FileMetadataInfo({
    required this.fileDate,
    this.lastLine = 0,
  });

  Map<String, dynamic> toJson() {
    return {
      'file': '${DateFormat('yyyy-MM-dd').format(DateTime.now())}$logExtension',
      'lastLine': lastLine,
    };
  }
}

Future<File> _ensureMetadataFileExist(
  String path, [
  FileMetadataInfo? fileMetadataInfo,
]) async {
  final file = File(join(path, 'metadata.json'));
  if (!await file.exists()) {
    await file.create();
    final fileMetadata = FileMetadataInfo(fileDate: DateTime.now());
    await file.writeAsString(jsonEncode(fileMetadata.toJson()));
  }
  if (fileMetadataInfo != null) {
    await file.writeAsString(jsonEncode(fileMetadataInfo.toJson()));
  }
  return file;
}

Future<void> _send(
  Uri uri,
  List<pb_logs.ResourceLogs> logRecords,
  Map<String, String> headers,
) async {
  final body = pb_logs_service.ExportLogsServiceRequest(
    resourceLogs: logRecords,
  );

  final reqHeaders = {'Content-Type': 'application/x-protobuf'}
    ..addAll(headers);

  await http.Client()
      .post(uri, body: body.writeToBuffer(), headers: reqHeaders);
}

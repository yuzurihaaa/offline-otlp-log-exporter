import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';

import 'package:collection/collection.dart';
import 'package:fixnum/fixnum.dart';
import 'package:http/http.dart' as http;
import 'package:intl/intl.dart';
import 'package:logging/logging.dart';
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
import 'package:path/path.dart';

import 'file_writer.dart';

class OfflineOTLPLogExporter implements sdk.LogRecordExporter {
  final _log = Logger('opentelemetry.OfflineOTLPLogExporter');
  final Directory dir;
  final FileWriter _fileWriter;

  OfflineOTLPLogExporter({
    required Uri uri,
    required this.dir,
    Duration scheduleDelay = const Duration(seconds: 5),
    Duration exportTimeout = const Duration(seconds: 30),
    int maxExportBatchSize = 50,
    Map<String, String> headers = const <String, String>{},
    FileWriter? fileWriter,
  })  : _scheduleDelay = scheduleDelay,
        _exportTimeout = exportTimeout,
        _maxExportBatchSize = maxExportBatchSize,
        _headers = headers,
        _uri = uri,
        _fileWriter = fileWriter ?? FileWriterImpl(dir: dir);

  void setLogTtl(Duration value) {
    if (_fileWriter is FileWriterImpl) {
      (_fileWriter as FileWriterImpl).logTtl = value;
    }
  }

  Uri? _uri;

  set uri(Uri value) {
    _uri = value;
  }

  Duration _scheduleDelay;

  set scheduleDelay(Duration value) {
    _scheduleDelay = value;
  }

  Duration _exportTimeout;

  set exportTimeout(Duration value) {
    _exportTimeout = value;
  }

  int _maxExportBatchSize;

  set maxExportBatchSize(int value) {
    _maxExportBatchSize = value;
  }

  Map<String, String> _headers;

  set headers(Map<String, String> value) {
    _headers = value;
  }

  Isolate? _isolate;

  Timer? _timer;

  @override
  Future<sdk.ExportResult> export(List<sdk.ReadableLogRecord> logs) async {
    await _ensureFileExist();

    final Iterable<pb_logs.ResourceLogs> protobufs = _logsToProtobuf(logs);

    for (final protobuf in protobufs) {
      await _fileWriter.write(protobuf.writeToJsonMap());
    }
    _initTimer();
    return sdk.ExportResult(code: sdk.ExportResultCode.success);
  }

  @override
  Future<void> shutdown() async {
    _close();
    await _fileWriter.close();
  }

  void _initTimer() {
    if (_timer != null) return;
    _timer = Timer(_scheduleDelay, () async {
      final receivePort = ReceivePort();
      final arg = SyncArg(
        path: join(dir.path, 'logs'),
        url: _uri.toString(),
        batchSize: _maxExportBatchSize,
        delayInMs: _scheduleDelay.inMilliseconds,
        headers: _headers,
        timeoutInMs: _exportTimeout.inMilliseconds,
        sendPort: receivePort.sendPort,
      );
      if (_isolate != null) {
        _log.shout("isolate is still running. Killing it immediately");
        _isolate?.kill(priority: Isolate.immediate);
        _isolate = null;
      }
      _isolate = await Isolate.spawn<SyncArg>(_sync, arg)
          .then<Isolate?>((e) => e)
          .catchError((e, st) {
        _log.shout("failed to spawn isolate", e, st);
        return null;
      });

      receivePort.listen(handleMessage);
    });
  }

  void handleMessage(message) {
    if (message is SendLogsMessage) {
      if (message.error != null) {
        _log.shout(
          message.message,
          message.error,
          message.stackTrace,
        );
      } else {
        _log.finest(message.message);
      }
    }
    if (message is SendLogsDone) {
      _close();
      _initTimer();
    }
  }

  void _close() {
    _isolate?.kill(priority: Isolate.immediate);
    _isolate = null;
    _timer?.cancel();
    _timer = null;
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

      il[logRecord.instrumentationScope] =
          il[logRecord.instrumentationScope] ?? <pb_logs.LogRecord>[]
            ..add(_logToProtobuf(logRecord));
      rsm[logRecord.resource] = il;
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
    return pb_logs.LogRecord(
      timeUnixNano: Int64(log.timeStamp.microsecondsSinceEpoch),
      severityNumber:
          pg_logs_enum.SeverityNumber.valueOf(log.severityNumber.index),
      severityText: log.severityText,
      droppedAttributesCount: log.droppedAttributesCount,
      body: _attributeONEValueToProtobuf(log.body),
      attributes: log.attributes.keys.map((key) => pb_common.KeyValue(
          key: key,
          value: _attributeValueToProtobuf(log.attributes.get(key)!))),
      spanId: log.spanContext.spanId.get(),
      traceId: log.spanContext.traceId.get(),
      observedTimeUnixNano: Int64(log.observedTimestamp.microsecondsSinceEpoch),
    );
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

  final SendPort sendPort;

  final bool shouldCompress;

  SyncArg({
    required this.path,
    required this.url,
    required this.delayInMs,
    required this.batchSize,
    required this.timeoutInMs,
    required this.headers,
    required this.sendPort,
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
      try {
        out.add(pb_logs.ResourceLogs()..mergeFromJson(e));
        await _send(Uri.parse(arg.url), out, arg.headers);
        arg.sendPort
            .send(SendLogsMessage(message: "logs successfully sent to remote"));
      } catch (e, st) {
        arg.sendPort.send(SendLogsMessage(
          message: "failed to send logs to remote",
          error: e,
          stackTrace: st,
        ));
      }
    }
    try {
      await _ensureMetadataFileExist(
        arg.path,
        FileMetadataInfo(
          fileDate: _getDateTimeFromFile(lastFile)!,
          lastLine: lastLine + batch.length,
        ),
      );
    } catch (e, st) {
      arg.sendPort.send(SendLogsMessage(
        message: "_ensureMetadataFileExist error",
        error: e,
        stackTrace: st,
      ));
    }
  }

  arg.sendPort.send(SendLogsDone());
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

class SendLogsMessage {
  final String message;
  final Object? error;
  final StackTrace? stackTrace;

  SendLogsMessage({
    required this.message,
    this.error,
    this.stackTrace,
  });
}

class SendLogsDone {}

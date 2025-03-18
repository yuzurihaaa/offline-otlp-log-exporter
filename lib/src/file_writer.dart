import 'dart:convert';
import 'dart:io';

import 'package:intl/intl.dart';
import 'package:path/path.dart';
import 'package:queue/queue.dart';

import 'utils.dart';

abstract class FileWriter {
  Future<void> write(Object input);

  Future<void> deleteOldLogs();

  Future<void> close();
}

class FileWriterImpl implements FileWriter {
  final Directory dir;

  IOSink? _sink;
  Queue? _writeQueue;
  File? _currentFile;

  FileWriterImpl({
    required this.dir,
    Queue? queue,
  }) : _writeQueue = queue ?? Queue();

  Duration _logTtl = const Duration(days: 30);

  set logTtl(Duration value) {
    _logTtl = value;
  }

  @override
  Future<void> write(Object input) async {
    final logDirectory = Directory(join(dir.path, 'logs'));
    if (!await logDirectory.exists()) {
      await logDirectory.create(recursive: true);
    }
    final file = _getLatestLogFile();
    await _ensureFileIsAvailable(file);
    await _checkIfFileChanged();
    if (_writeQueue?.isCancelled == true) {
      return;
    }
    if (_sink == null) {
      return;
    }
    _writeQueue?.add(() async {
      try {
        _sink?.writeln(jsonEncode(input));
      } catch (e) {
        print('fail to write logs to file $e');
      }
    });
  }

  @override
  Future<void> deleteOldLogs() async {
    if (_logTtl == Duration.zero) {
      return;
    }
    await dir
        .list()
        .where((file) => file is File && file.path.endsWith(logExtension))
        .forEach((file) async {
      final fileDt = _getDateTimeFromFile(file as File);
      if (fileDt == null) {
        return;
      }
      if (DateTime.now().difference(fileDt) > _logTtl) {
        await file.delete();
      }
    });
  }

  @override
  Future<void> close() async {
    await _sink?.close();
    _writeQueue?.cancel();
    _writeQueue = null;
  }

  DateTime? _getDateTimeFromFile(FileSystemEntity file) {
    return DateTime.tryParse(basename(file.path).split('.').first);
  }

  // There are chances that the file might change in between the write operation.
  // For example, as day changes, new log file with new date should be created and new
  // logs should be written to that file.
  // To avoid conflict, close all IO operations and open the new file.
  Future<void> _checkIfFileChanged() async {
    if (_currentFile == null ||
        _getLatestLogFile().path != _currentFile?.path) {
      _currentFile = _getLatestLogFile();
      _writeQueue?.cancel();
      _writeQueue = Queue();
      await _sink?.close();
      _sink = _currentFile?.openWrite(
        mode: FileMode.writeOnlyAppend,
        encoding: utf8,
      );
    }
  }

  File _getLatestLogFile() => File(join(dir.path, 'logs',
      '${DateFormat('yyyy-MM-dd').format(DateTime.now())}$logExtension'));

  Future<bool> _ensureFileIsAvailable(File file) async {
    if (!await file.exists()) {
      await file.create();
      return true;
    }

    return false;
  }
}

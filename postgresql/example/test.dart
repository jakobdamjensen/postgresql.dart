//#library('postgresql');

//FIXME imports ?
import 'dart:crypto';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';
import 'dart:scalarlist';

part '../lib/src/postgresql.dart';

part '../lib/src/circular_block_buffer.dart';
part '../lib/src/column_desc.dart';
part '../lib/src/connection.dart';
part '../lib/src/constants.dart';
part '../lib/src/encoding.dart';
part '../lib/src/input_buffer.dart';
part '../lib/src/output_buffer.dart';
part '../lib/src/pg_error.dart';
part '../lib/src/query.dart';
part '../lib/src/row_description.dart';
part '../lib/src/mapper.dart';
part '../lib/src/result_reader.dart';
part '../lib/src/settings.dart';
part '../lib/src/simple_buffer.dart';
part '../lib/src/stream.dart';

void main() {
  defaultSettings = new Settings(
        username: 'testdb',
        database: 'testdb',
        password: 'password',
        onUnhandledErrorOrNotice: (err) => print('Unhandled: $err'));

  testMessageFragmentParsing2();
}

void testMessageParsing() {
  var m = new _Mocket();
  var c = new _Connection(null);
  c._state = _BUSY;
  c._socket = m;

  m.data = combineUint8Lists([
    makeRowDescriptionMessage(3),
    makeDataRowMessage(["sfsdfdsf", "sdfsdfdsf", "fdsfdsfds"]),
    makeCommandCompleteMessage('SELECT 1'),
    makeReadyForQueryMessage('I')
  ]);

  c.query('Fake query to set up state.').one().then((row) => print('DONE: $row'));

  c._readData();
}

void testMessageParsing2() {
  var m = new _Mocket();
  var c = new _Connection(null);
  c._state = _BUSY;
  c._socket = m;

  m.data = combineUint8Lists([
    makeRowDescriptionMessage(3),
    makeDataRowMessage(["sfsdfdsf", "sdfsdfdsf", "fdsfdsfds"]),
    makeDataRowMessage(["sfsdfdsf", "sdfsdfdsf", "fdsfdsfds"]),
    makeCommandCompleteMessage('SELECT 2'),
    makeReadyForQueryMessage('I')
  ]);

  c.query('Fake query to set up state.').all().then((rows) => print('DONE: $rows'));

  c._readData();
}

void testMessageFragmentParsing() {
  var m = new _Mocket();
  var c = new _Connection(null);
  c._state = _BUSY;
  c._socket = m;

  int longStringLength = 19 * 1024;

  m.data = combineUint8Lists([
    makeRowDescriptionMessage(3),
    makeDataRowMessage(["sfsdfdsf", makeLongString(longStringLength), "fdsfdsfds"]),
    makeDataRowMessage(["sfsdfdsf", "sdfsdfdsf", "fdsfdsfds"]),
    makeCommandCompleteMessage('SELECT 1'),
    makeReadyForQueryMessage('I')
  ]);

  c.query('Fake query to set up state.').all().then((rows) {
    assert(rows.length == 2 && rows[0][1].length == longStringLength);
    print('DONE');
  });

  c._readData();
}

void testMessageFragmentParsing2() {
  var m = new _Mocket2();
  var c = new _Connection(null);
  c._state = _BUSY;
  c._socket = m;

  int longStringLength = 19 * 1024;

  m.data = combineUint8Lists([
    makeRowDescriptionMessage(3),
    makeDataRowMessage(["sfsdfdsf", makeLongString(longStringLength), "fdsfdsfds"]),
    makeDataRowMessage(["sfsdfdsf", "sdfsdfdsf", "fdsfdsfds"]),
    makeCommandCompleteMessage('SELECT 1'),
    makeReadyForQueryMessage('I')
  ]);

  c.query('Fake query to set up state.').all().then((rows) {
    assert(rows.length == 2 && rows[0][1].length == longStringLength);
    print('DONE');
  });

  c._readData();
  c._readData();
  c._readData();
}

String makeLongString(int len) {
  var sb = new StringBuffer();
  for (int i = 0; i < len; i++) {
    sb.add('a');
  }
  return sb.toString();
}


Uint8List makeRowDescriptionMessage(int count) {
  var list = new List<ColumnDesc>();
  for (int i = 0; i < count; i++) {
    list.add(new _ColumnDesc(i, 'column$i', 0, 0, 705, -2, -1, 0));
  }
  return makeRowDescriptionMessageImpl(list);
}

Uint8List makeRowDescriptionMessageImpl(List<ColumnDesc> cols) {
  var w = new _OutputBuffer(8192, 1);

  w.startMessage(_MSG_ROW_DESCRIPTION);
  w.writeInt16(cols.length);

  for (var c in cols) {
    w.writeString(c.name);
    w.writeInt32(c.fieldId);
    w.writeInt16(c.tableColNo);
    w.writeInt32(c.fieldType);
    w.writeInt16(c.dataSize);
    w.writeInt32(c.typeModifier);
    w.writeInt16(c.formatCode);
  }

  w.endMessage();

  return w.dump();
}

// All columns are type String.
Uint8List makeDataRowMessage(List<String> values) {
  var w = new _OutputBuffer(8192, 1);

  w.startMessage(_MSG_DATA_ROW);
  w.writeInt16(values.length);

  for (var val in values) {
    w.writeInt32(val.length);
    var bytes = new List<int>();
    for (var c in val.charCodes) {
      if (c > 127 || c < 0) {
        c = '?'.charCodeAt(0);
      }
      w.writeByte(c);
    }
  }

  w.endMessage();

  return w.dump();
}

Uint8List makeCommandCompleteMessage(String commandTag) {
  var w = new _OutputBuffer(8192, 1);

  w.startMessage(_MSG_COMMAND_COMPLETE);
  w.writeString(commandTag);
  w.endMessage();

  return w.dump();
}

// transaction is 'I', 'T', or 'E'. If in doubt just I.
Uint8List makeReadyForQueryMessage(String transaction) {
  var w = new _OutputBuffer(8192, 1);

  w.startMessage(_MSG_READY_FOR_QUERY);
  w.writeByte(transaction.charCodeAt(0));
  w.endMessage();

  return w.dump();
}

void testBuffer2() {
  int size = 8912;

  var buffer = new _InputBuffer(size, 1);

  var socket = new _Mocket();
  socket.data = combineUint8Lists([
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 4),
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 4 - 100),
    makeMockMessage(_MSG_ERROR_RESPONSE, size - 100)
  ]);

  buffer.appendFromSocket(socket);

  assert(buffer.peekByte() == _MSG_ERROR_RESPONSE);
  assert(buffer.readByte() == _MSG_ERROR_RESPONSE);
  assert(buffer.readInt32() == (size ~/ 4));

  buffer._buffer.skip((size ~/ 4) - 5 + 1);
  assert(buffer.peekByte() == _MSG_ERROR_RESPONSE);

  buffer._buffer.skip((size ~/ 4) - 100 + 1);
  assert(buffer.peekByte() == _MSG_ERROR_RESPONSE);

  print(buffer.contiguousBytesAvailable);

  buffer.appendFromSocket(socket);

  buffer._logState();

  buffer._buffer.skip(size - 100);

  buffer._logState();

  assert(buffer.readByte() == 0);
  assert(buffer.contiguousBytesAvailable == 0);
}


void testBuffer3() {
  int size = 8912;

  var buffer = new _InputBuffer(size, 2);

  var socket = new _Mocket();
  socket.data = combineUint8Lists([
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 2),
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 2 - 100),
    makeMockMessage(_MSG_ERROR_RESPONSE, size * 2 - 100)
  ]);

  buffer.appendFromSocket(socket);

  assert(buffer.peekByte() == _MSG_ERROR_RESPONSE);
  assert(buffer.readByte() == _MSG_ERROR_RESPONSE);
  assert(buffer.peekInt32() == (size ~/ 2));
  assert(buffer.readInt32() == (size ~/ 2));

  buffer._buffer.skip((size ~/ 2) - 5 + 1);
  assert(buffer.peekByte() == _MSG_ERROR_RESPONSE);

  buffer._buffer.skip((size ~/ 2) - 100 + 1);
  assert(buffer.peekByte() == _MSG_ERROR_RESPONSE);

  buffer.appendFromSocket(socket);
  buffer._logState();

  buffer.appendFromSocket(socket);
  buffer._logState();

  //buffer.appendFromSocket(socket);
  //buffer._logState();

  buffer._buffer.skip(size * 2 - 100);

  print(buffer.bytesAvailable);
  print(buffer.contiguousBytesAvailable);

  buffer._logState();

  buffer._buffer.checkBytesAvailable();
  buffer.readByte();

  print(buffer.contiguousBytesAvailable);

  assert(buffer.contiguousBytesAvailable == 0);
}


void testBuffer4() {
  int size = 8912;

  var buffer = new _InputBuffer(size, 1);

  var socket = new _Mocket();
  socket.data = combineUint8Lists([
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 4),
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 4),
  ]);

  buffer.appendFromSocket(socket);

  assert(buffer.readByte() == _MSG_ERROR_RESPONSE);

  int msgLen = buffer.readInt32();
  assert(msgLen == (size ~/ 4));

  var s = buffer.readString();

  assert(s.length == msgLen + 1 - 6); // 5 bytes for header, and one for the null term, and all single byte characters.

  assert(buffer.readByte() == _MSG_ERROR_RESPONSE);

  msgLen = buffer.readInt32();
  assert(msgLen == (size ~/ 4));

  s = buffer.readString();

  assert(s.length == msgLen + 1 - 6); // 5 bytes for header, and one for the null term, and all single byte characters.
  assert(buffer.contiguousBytesAvailable == 0);
}

void testBuffer5() {
  int size = 8912;
  var buffer = new _InputBuffer(size, 2);

  var socket = new _Mocket();
  socket.data = combineUint8Lists([
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 2 + 100),
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 2 + 100),
    makeMockMessage(_MSG_ERROR_RESPONSE, size ~/ 2 + 100),
  ]);

  buffer.appendFromSocket(socket);

  assert(buffer.readByte() == _MSG_ERROR_RESPONSE);

  int msgLen = buffer.readInt32();
  assert(msgLen == size ~/ 2 + 100);

  var s = buffer.readString();

  assert(s.length == msgLen + 1 - 6); // 5 bytes for header, and one for the null term, and all single byte characters.

  assert(buffer.readByte() == _MSG_ERROR_RESPONSE);

  msgLen = buffer.readInt32();
  assert(msgLen == size ~/ 2 + 100);

  buffer.appendFromSocket(socket);

  s = buffer.readString();
  assert(s.length == msgLen + 1 - 6); // 5 bytes for header, and one for the null term, and all single byte characters.

  assert(buffer.readByte() == _MSG_ERROR_RESPONSE);
  msgLen = buffer.readInt32();
  assert(msgLen == size ~/ 2 + 100);

  s = buffer.readString();
  assert(s.length == msgLen + 1 - 6); // 5 bytes for header, and one for the null term, and all single byte characters.

  assert(buffer.contiguousBytesAvailable == 0);
}


class _Mocket implements Socket {
  List<int> data;
  int bytesRead = 0;
  bool closed = false;

  int available() => (data != null) ? data.length : 0;

  int readList(List<int> list, int offset, int length) {
    if (closed) {
      throw new Exception('Attempted to read on closed mocket.');
    }

    int available = min(data.length - bytesRead, length);
    print('readList() $available');
    for (int i = 0; i < available; i++) {
      list[offset + i] = data[i + bytesRead];
    }
    bytesRead += available;
    return available;
  }

  int writeList(List<int> buffer, int offset, int count) {
    if (closed) {
      throw new Exception('Attempted to write on closed mocket.');
    }

    print('Mocket sent $count bytes.');
    return count;
  }

  void close([bool halfClose = false]) {
    print('Mocket closed.');
  }
}

class _Mocket2 implements Socket {
  List<int> data;
  int bytesRead = 0;
  bool closed = false;

  int packetCounter = 0;
  int packetsBeforeZeroRead = 2;

  int available() => (data != null) ? data.length : 0;

  int readList(List<int> list, int offset, int length) {
    if (closed) {
      throw new Exception('Attempted to read on closed mocket.');
    }

    packetCounter++;
    if (packetCounter % packetsBeforeZeroRead == 0) {
      return 0;
    }

    int available = min(data.length - bytesRead, length);
    print('readList() $available');
    for (int i = 0; i < available; i++) {
      list[offset + i] = data[i + bytesRead];
    }
    bytesRead += available;
    return available;
  }

  int writeList(List<int> buffer, int offset, int count) {
    if (closed) {
      throw new Exception('Attempted to write on closed mocket.');
    }

    print('Mocket sent $count bytes.');
    return count;
  }

  void close([bool halfClose = false]) {
    print('Mocket closed.');
  }
}

Uint8List makeMockMessage(int type, int length) {
  var list = new Uint8List(length + 1);
  list[0] = type;
  list[1] = (length >> (8*3)) & 0x000000FF;
  list[2] = (length >> (8*2)) & 0x000000FF;
  list[3] = (length >> 8) & 0x000000FF;
  list[4] = length & 0x000000FF;

  for (int i = 5; i < length; i++) {
    list[i] = 'a'.charCodeAt(0);
  }

  list[length] = 0;

  return list;
}

combineUint8Lists(List<Uint8List> lists) {
  int len = 0;
  for (var l in lists) {
    len += l.length;
  }
  var combined = new Uint8List(len);

  int i = 0;
  for (var l in lists) {
    for (var j = 0; j < l.length; j++) {
      combined[i] = l[j];
      i++;
    }
  }

  return combined;
}

//#library('postgresql');

//FIXME imports ?
#import('dart:crypto');
#import('dart:io');
#import('dart:isolate');
#import('dart:math');
#import('dart:scalarlist');

#source('../lib/src/postgresql.dart');

#source('../lib/src/circular_block_buffer.dart');
#source('../lib/src/column_desc.dart');
#source('../lib/src/connection.dart');
#source('../lib/src/constants.dart');
#source('../lib/src/encoding.dart');
#source('../lib/src/message_reader.dart');
#source('../lib/src/message_writer.dart');
#source('../lib/src/query.dart');
#source('../lib/src/row_description.dart');
#source('../lib/src/result_reader.dart');
#source('../lib/src/simple_buffer.dart');
#source('../lib/src/stream.dart');

void main() {
  testBuffer5();
  testBuffer2();
  //testBuffer3(); //FIXME fails
  testBuffer4();
}


void testBuffer2() {
  int size = 8912;
  
  var buffer = new _MessageReader(size, 1);
  
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
  
  var buffer = new _MessageReader(size, 2);
  
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
  
  var buffer = new _MessageReader(size, 1);
  
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
  
  //var buffer = new _Buffer(size);
  var buffer = new _MessageReader(size, 2);
  
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


void testMessageFragmentParsing() {
  var m = new _Mocket();
  var c = new _Connection();
  c._state = _Connection._BUSY;
  c._socket = m;
  
  m.data = combineUint8Lists([
    makeMockMessage(_MSG_ERROR_RESPONSE, INPUT_BUFFER_SIZE ~/ 4),
    makeMockMessage(_MSG_ERROR_RESPONSE, INPUT_BUFFER_SIZE ~/ 4 - 100),
    makeMockMessage(_MSG_ERROR_RESPONSE, INPUT_BUFFER_SIZE - 100)
  ]);

  c._readMessages();
  c._readMessages();
  
  print('done');
}

class _Mocket implements Socket {
  List<int> data;
  int bytesRead = 0;
  
  int available() => (data != null) ? data.length : 0;
  
  int readList(List<int> list, int offset, int length) {    
    int available = min(data.length - bytesRead, length);
    print('readList() $available');
    for (int i = 0; i < available; i++) {
      list[offset + i] = data[i + bytesRead];
    }
    bytesRead += available;
    return available;
  }
}

Uint8List makeMockMessage(int type, int length) {
  var list = new Uint8List(length + 1);
  list[0] = type;
  list[1] = (length >> (8*3)) & 0x000000FF;
  list[2] = (length >> (8*2)) & 0x000000FF;
  list[3] = (length >> 8) & 0x000000FF;
  list[4] = length & 0x000000FF;
  
  for (int i = 5; i < length; i++)
    list[i] = 'a'.charCodeAt(0);
  
  list[length] = 0;
  
  return list;
}

combineUint8Lists(List<Uint8List> lists) {
  int len = 0;
  for (var l in lists)
    len += l.length;
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

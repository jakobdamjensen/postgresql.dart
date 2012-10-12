
const int _MESSAGE_HEADER = 0;
const int _MESSAGE_BODY = 1;
const int _MESSAGE_WITHIN_BODY = 2;

class _MessageReader {
  
  _MessageReader(int readSize, int initialBlocks) 
      : _readSize = readSize,
        _buffer = new _CircularBlockBuffer(readSize, initialBlocks),
        _state = _MESSAGE_HEADER;
//          _buffer = new _SimpleBuffer(65535);
  
  final int _readSize;
  final _CircularBlockBuffer _buffer;
  //final _SimpleBuffer _buffer;
  
  int _state;
  int _msgType;
  int _msgLength; // Note the +1 issue.
  int _msgStart; // The position where startMessage was last called.
  
  int get state() {
    if (_state == _MESSAGE_BODY && index > _msgStart + 5)
      return _MESSAGE_WITHIN_BODY;
    else
      return _state;
  } 
  
  int get index => _buffer.index;
  int get bytesAvailable => _buffer.bytesAvailable;
  int get contiguousBytesAvailable => _buffer.contiguousBytesAvailable;

  int get messageType => _msgType;
  int get messageLength => _msgLength; // The message length reported in the header, note this doesn't include the message type byte.
  int get messageStart => _msgStart; // The index in the buffer where the message started, i.e. the index of the message type byte.
  int get messageEnd => _msgStart + _msgLength + 1;
  int get messageBytesRead => index - _msgStart; // Note includes message type byte. So after reading a message messageBytesRead == messageLength + 1.
  int get messageBytesRemaining => messageEnd - index;
  
  bool get isMessageFragment => messageBytesRemaining > bytesAvailable;
  
  void _log(String msg) => print('MessageReader: $msg');
  
  void _logState() {
    _buffer._logState();
  }
  
  //TODO remove this, let connection append directly to the buffer.
  int appendFromSocket(Socket _socket) {
    return _buffer.appendFromSocket(_socket, _readSize);
    
// Print debugging info
//    int i = _buffer.block.start;
//    while (i < _buffer.block.end - 5) {
//      int mtype = _buffer.block.list[i];
//      int a = _buffer.block.list[i + 1];
//      int b = _buffer.block.list[i + 2];
//      int c = _buffer.block.list[i + 3];
//      int d = _buffer.block.list[i + 4];
//      int len = _decodeInt32(a, b, c, d);
//      _log('Message: ${_itoa(mtype)} ${_messageName(mtype)} length: $len.');
//      i += len;
//    }
  }
  
  int peekByteFast() {
    assert(_buffer.block.start < _buffer.block.list.length);
    return _buffer.block.list[_buffer.block.start];
  }
  
  int peekByte() {
    _buffer.checkBytesAvailable();
    return _buffer.block.list[_buffer.block.start];
  }
  
  //TODO careful testing.
  // Caller must check contiguousBytesAvailable.
  int readByteFast() {
    assert(_buffer.block.start < _buffer.block.list.length);
    int b = _buffer.block.list[_buffer.block.start];
    _buffer.block.start++;
    return b;
  }
    
  int readByte() {
    _buffer.checkBytesAvailable();
    int b = _buffer.block.list[_buffer.block.start];
    _buffer.block.start++;
    return b;
  }
  
  int readInt16() {
    _buffer.checkBytesAvailable();
    
    int a, b;
    //TODO remove comments and debug. It will be faster.
    //if (contiguousBytesAvailable < 2) {
      a = readByte();
      b = readByte();
    //} else {
    //  a = _buffer.block.list[_buffer.block.start];
    //  b = _buffer.block.list[_buffer.block.start + 1];
    // _buffer.block.start += 2;
    //}
    
    return _decodeInt16(a, b);
  }
  
  int readInt32() {
    _buffer.checkBytesAvailable();
    int a, b, c, d;
    //TODO remove comments and debug. It will be faster.
    //if (contiguousBytesAvailable < 4) {
      a = readByte();
      b = readByte();
      c = readByte();
      d = readByte();
    //} else {
    //  var l = _buffer.block.list;
    //  var s = _buffer.block.start;
    //  a = l[s];
    //  b = l[s + 1];
    //  c = l[s + 2];
    //  d = l[s + 3];
    // _buffer.block.start += 4;
    //}
    
    return _decodeInt32(a, b, c, d);
  }

  // Big endian two's complement.
  // Please, somebody show me the cool way to do it with bitwise operators ;)
  int _decodeInt16(int a, int b) {
    assert(a < 256 && b < 256 && a >= 0 && b >= 0);
    int i = (a << 8) | (b << 0); 
    
    if (i >= 0x8000)
      i = -0x10000 + i;
    
    return i;
  }

  // Big endian two's complement.
  int _decodeInt32(int a, int b, int c, int d) {
    assert(a < 256 && b < 256 && c < 256 && d < 256 && a >= 0 && b >= 0 && c >= 0 && d >= 0);
    int i = (a << 24) | (b << 16) | (c << 8) | (d << 0);
    
    if (i >= 0x80000000)
      i = -0x100000000 + i;
    
    return i;
  }
  
  // Slow simple version.
  //TODO Fast version that just searches for null char and copies accross one
  //buffer at a time. See readString_v2()
  String readString() {
    
    var sb = new StringBuffer();
    
    int c;
    while ((c = readByte()) != 0) {
      sb.add(new String.fromCharCodes(<int> [c]));
    }
    
    return sb.toString();
  }

  //FIXME - slow, makes a copy. Perhaps return a view instead.
  List<int> readBytes(int n) {
    var l = new Uint8List(n);
    for (int i = 0; i < n; i++) {
      l[i] = readByte();
    }
    return l;
  }
  
  void startMessage() {
    assert(_state == _MESSAGE_HEADER);
    _state = _MESSAGE_BODY;
    _msgStart = index;
    _msgType = readByte();
    _msgLength = readInt32();
  }
  
  void endMessage() {
    _state = _MESSAGE_HEADER;
    assert(messageBytesRemaining == 0);
  }
  
  // Can only skip messages which have been entirely read into the buffer.
  void skipMessage() {
    _state = _MESSAGE_HEADER;
    assert(_msgType > 0);
    assert(_msgLength > 0);
    int endIndex = _msgStart + _msgLength + 1;
    if (endIndex == index)
      return;
    _buffer.skip(endIndex - index);
    assert(index == endIndex);
  }

  //TODO implement me.
//  String readString_v2() {
//    
//    String readStringFromCurrentBlock() {
//      int c;
//      int start = _buffer.block.start;
//      while (_buffer.block.start < _buffer.block.end) {
//        c = _buffer.block.list[_buffer.block.start];
//        if (c == 0)
//          break;
//        _buffer.block.start++;
//      }
//      
//      if (_buffer.block.end - start == 0)
//        return '';
//      
//      //FIXME Yuck there must be a better way.
//      // Also need to check what sort of character encoding that postgresql uses.
//      var list = new Uint8List.view(_buffer.block.list.asByteArray(start, _buffer.block.start - start));
//      return new String.fromCharCodes(list);
//    }
//    
//    var s = readStringFromCurrentBlock();
//    
//    if (peekByte() == 0) {
//      readByte();
//      return s;
//    }
//    
//    // If the string spans at least two blocks then continue reading.
//    var sb = new StringBuffer(s);
//    
//    for(;;) {
//      sb.add(readStringFromCurrentBlock());
//      if (peekByte() == 0) {
//        readByte();
//        return sb.toString();
//      }
//    }
//    
//    // Make sure that we don't wind up in an infinite loop.
//    //TODO perhaps add an optional max string size configuration setting.
//    //if (index > _msgEnd)
//      //throw new Exception('Read string didn\'t find a null terminator in the message.');
//  }
}

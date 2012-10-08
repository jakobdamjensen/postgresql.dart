
class _MessageReader {
  
  _MessageReader(int readSize, int initialBlocks) 
      : _readSize = readSize,
        _buffer = new _CircularBlockBuffer(readSize, initialBlocks);
//          _buffer = new _SimpleBuffer(65535);
  
  final int _readSize;
  final _CircularBlockBuffer _buffer;
  //final _SimpleBuffer _buffer;
  
  int _msgType;
  int _msgLength; // Note the +1 issue.
  int _msgStart; // The position where startMessage was last called.
  int _msgEnd;
  
  int get index => _buffer.index;
  int get bytesAvailable => _buffer.bytesAvailable;
  int get contiguousBytesAvailable => _buffer.contiguousBytesAvailable;

  int get messageType => _msgType;
  int get messageLength => _msgLength;
  int get messageStart => _msgStart;
  
  int get messageBytesRead => index - _msgStart;
  
  void _log(String msg) => print('MessageReader: $msg');
  
  void _logState() {
    _buffer._logState();
  }
  
  void appendFromSocket(Socket _socket) {
    _buffer.appendFromSocket(_socket, _readSize);
    
// Print debugging info
//    int i = _buffer.block.start;
//    while (i < _buffer.block.end - 5) {
//      int mtype = _buffer.block.list[i];
//      int a = _buffer.block.list[i + 1];
//      int b = _buffer.block.list[i + 2];
//      int c = _buffer.block.list[i + 3];
//      int d = _buffer.block.list[i + 4];
//      int len = (a << (8*3)) + (b << (8*2)) + (c << 8) + d;
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
    
    //TODO handle negative integers
    // Also ByteArray might learn how to handle big endian numbers at some
    // stage. If so then use it because it will likely be faster.
    return (a << 8) + b;
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
    
    //TODO handle negative integers
    // Also ByteArray might learn how to handle big endian numbers at some
    // stage. If so then use it because it will likely be faster.
    return (a << (8*3)) + (b << (8*2)) + (c << 8) + d;
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
    _msgStart = index;
    _msgType = readByte();
    _msgLength = readInt32();
  }
  
  void skipMessage() {
    assert(_msgType > 0);
    assert(_msgLength > 0);
    int endIndex = _msgStart + _msgLength + 1;
    if (endIndex == index)
      return;
    _buffer.skip(endIndex - index);
    assert(index == endIndex);
  }

  //TODO implement me.
  String readString_v2() {
    
    String readStringFromCurrentBlock() {
      int c;
      int start = _buffer.block.start;
      while (_buffer.block.start < _buffer.block.end) {
        c = _buffer.block.list[_buffer.block.start];
        if (c == 0)
          break;
        _buffer.block.start++;
      }
      
      if (_buffer.block.end - start == 0)
        return '';
      
      //FIXME Yuck there must be a better way.
      // Also need to check what sort of character encoding that postgresql uses.
      var list = new Uint8List.view(_buffer.block.list.asByteArray(start, _buffer.block.start - start));
      return new String.fromCharCodes(list);
    }
    
    var s = readStringFromCurrentBlock();
    
    if (peekByte() == 0) {
      readByte();
      return s;
    }
    
    // If the string spans at least two blocks then continue reading.
    var sb = new StringBuffer(s);
    
    for(;;) {
      sb.add(readStringFromCurrentBlock());
      if (peekByte() == 0) {
        readByte();
        return sb.toString();
      }
    }
    
    // Make sure that we don't wind up in an infinite loop.
    //TODO perhaps add an optional max string size configuration setting.
    if (index > _msgEnd)
      throw new Exception('Read string didn\'t find a null terminator in the message.');
  }
}

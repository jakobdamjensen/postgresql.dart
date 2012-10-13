
class _OutputBuffer {
  
  final List<Uint8List> _buffers;
  int _writeSize;
  int _msgCount;
  int _pos;
  int _start;
  int _msgType;
  
  int get _spaceAvailable => _buffers.length * _writeSize - _pos;
  
  _OutputBuffer(this._writeSize, int initialBufferCount)
    : _buffers = new List<Uint8List>()
    {
      clear();
      for (int i = 0; i < initialBufferCount; i++) {
        _buffers.add(new Uint8List(_writeSize));
      }
    }
  
  int get bytesWritten => _pos;
  

  void writeToSocket(Socket socket, [void callback(PgError error)]) {
    _writeToSocket(socket, 0, callback);
  }
  
  void _log(msg) => print(msg);
  
  void _writeToSocket(Socket socket, int startOffset, [void callback(PgError error)]) {
    try {
      int i = startOffset;
      while (i < _pos) {
        var buf = _buffers[i ~/ _writeSize];
        int len = min(_pos - i, _writeSize);
        int start = i % _writeSize;
        int bytesWritten = socket.writeList(buf, start, len);
        _log('Sent $bytesWritten bytes.');
        i += bytesWritten;
        
        if (bytesWritten < len) {
          socket.onWrite = () {
            _log('Delayed socket write.');
            _writeToSocket(socket, i, callback);
          };
          return;
        }
      }
    } catch (ex) {
      callback(new PgError.client('Socket write error: $ex'));
    }
    if (callback != null)
      callback(null);
  }
  
  void startMessage(int msgType) {
    _msgCount++;
    _msgType = msgType;
    _start = _pos;
    
    // Startup message has no message type byte, so the header is only 4 bytes.
    _pos += (msgType == _MSG_STARTUP) ? 4 : 5;
  }
  
  // Write header.
  void endMessage() {    
    if (_msgType == _MSG_STARTUP) {
      // Startup packets don't have a msg type header.
      _setInt32(_start, _pos - _start);
    } else {
      // Set message length. As per postgres protocol this length does not
      // including the msgType byte, hence _pos - 1.
      _setByte(_start, _msgType);
      _setInt32(_start + 1, _pos - _start - 1);
    }
  }
  
  void clear() {
    _pos = 0;
    _start = 0;
    _msgCount = 0;
  }
  
  void _setByte(int offset, int b) {
    assert(b >= 0 && b <= 255);
   
    int spaceAvailable = _buffers.length * _writeSize - offset;
    
    // Allocate a new buffer if required.
    if (spaceAvailable < 1) {
      _buffers.add(new Uint8List(_writeSize));
    }
    
    var buf = _buffers[offset ~/ _writeSize];
    buf[offset % _writeSize] = b;
  }
  
  void _setInt32(int offset, int i) {
    assert(i >= -2147483648 && i <= 2147483647);
    
    if (i < 0)
      i = 0x100000000 + i;
    
    int a = (i >> 24) & 0x000000FF;
    int b = (i >> 16) & 0x000000FF;
    int c = (i >> 8) & 0x000000FF;
    int d = i & 0x000000FF;
    
    _setByte(offset, a);
    _setByte(offset + 1, b);
    _setByte(offset + 2, c);
    _setByte(offset + 3, d);
  }
  
  void writeByte(int b) {
    _setByte(_pos, b);
    _pos++;
  }
  
  void writeInt16(int i) {
    assert(i >= -32768 && i <= 32767);
    
    if (i < 0)
      i = 0x10000 + i;
    
    int a = (i >> 8) & 0x00FF;
    int b = i & 0x00FF;
    
    //if (spaceAvailable >= 2) {
    // fastImpl
    //} else {
    
    writeByte(a);
    writeByte(b);
  }
  
  // Encode as a big endian two's complement 32 bit integer
  void writeInt32(int i) {
    assert(i >= -2147483648 && i <= 2147483647);
    
    if (i < 0)
      i = 0x100000000 + i;
    
    int a = (i >> 24) & 0x000000FF;
    int b = (i >> 16) & 0x000000FF;
    int c = (i >> 8) & 0x000000FF;
    int d = i & 0x000000FF;
    
    //if (spaceAvailable >= 4) {
    // fastImpl
    //} else {
    
    writeByte(a);
    writeByte(b);
    writeByte(c);
    writeByte(d);
  }
  
  //FIXME Unicode support. Check Postgresql protocol supports it. Otherwise
  // characters may need to be encoded as \uXXXX or something similar.
  //FIXME how to escape the null character as per spec.
  //TODO write a fast version of this.
  void writeString(String s) {
    for (int c in s.charCodes()) {
      if (c > 127)
        c = '?'.charCodeAt(0);
      if (c == 0)
        throw new Exception('Null character not supported.');
      writeByte(c);
    }
    
    writeByte(0);
  }
 
  // Usefull for testing.
  Uint8List dump() {
    var list = new Uint8List(bytesWritten);
    
    for (int i = 0; i < bytesWritten; i += _writeSize) {
      for (int j = 0; j < min(_writeSize, bytesWritten - i); j++) {
        var b = _buffers[i ~/ _writeSize];
        list[i + j] = b[j];
      }
    }
    return list;
  }
}


class _MessageWriter {
  
  final Uint8List _buffer;
  int _pos;
  int _msgType;
  
  _MessageWriter(this._buffer);
  
  int get bytesWritten => _pos;
  
  void startMessage(int msgType) {
    _msgType = msgType;
    // Startup message has no message type byte.
    _pos = (msgType == _MSG_STARTUP) ? 4 : 5;
  }
  
  /// Write header.
  void endMessage() {
    if (_msgType == _MSG_STARTUP) {
      _setInt32(0, _pos); // Startup packets don't have a msg type header.
    } else {
      _buffer[0] = _msgType;
      _setInt32(1, _pos - 1); // Message length not including the msgType byte.
    }
  }
  
  void writeByte(int b) {
    _buffer[_pos] = b;
    _pos++;
  }
  
  // Big endian - network byte order.
  void _setInt32(int offset, int i) {
    _buffer[offset]     = (i >> (8*3)) & 0x000000FF;
    _buffer[offset + 1] = (i >> (8*2)) & 0x000000FF;
    _buffer[offset + 2] = (i >> 8)     & 0x000000FF;
    _buffer[offset + 3] =  i           & 0x000000FF;
  }
  
  void writeInt16(int i) {
    _buffer[_pos] =     (i >> 8) & 0x000000FF;
    _buffer[_pos + 1] =  i       & 0x000000FF;
    _pos += 2;
  }
  
  void writeInt32(int i) {
    _setInt32(_pos, i);
    _pos += 4;
  }
  
  //FIXME Unicode support. Check Postgresql protocol supports it. Otherwise
  // characters may need to be encoded as \uXXXX or something similar.
  //FIXME how do you escape the null character.
  void writeString(String s) {
    for (int c in s.charCodes()) {
      if (c > 255)
        c = '?'.charCodeAt(0);
      if (c == 0)
        throw new Exception('Null character not supported.');
      _buffer[_pos] = c;
      _pos++;
    }
    _buffer[_pos] = 0;
    _pos++;
  }
 
}

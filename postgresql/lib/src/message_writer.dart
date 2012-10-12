
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
      // Startup packets don't have a msg type header.
      _encodeInt32(_pos, _buffer, 0);
    } else {
      // Set message length. As per postgres protocol this length does not
      // including the msgType byte, hence _pos - 1.
      _buffer[0] = _msgType;
      _encodeInt32(_pos - 1, _buffer, 1);
    }
  }
  
  void writeByte(int b) {
    assert(b >= 0 && b <= 255);
    _buffer[_pos] = b;
    _pos++;
  }
  

  
  void writeInt16(int i) {
    _encodeInt16(i, _buffer, _pos);
    _pos += 2;
  }
  
  // Signed int
  void writeInt32(int i) {
    _encodeInt32(i, _buffer, _pos);
    _pos += 4;
  }
  
  //FIXME Unicode support. Check Postgresql protocol supports it. Otherwise
  // characters may need to be encoded as \uXXXX or something similar.
  //FIXME how do you escape the null character.
  void writeString(String s) {
    for (int c in s.charCodes()) {
      if (c > 127)
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

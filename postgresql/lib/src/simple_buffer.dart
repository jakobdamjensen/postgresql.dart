part of postgresql;


// A fixed size buffer that will fire an error if it runs out of space.
class _SimpleBuffer {

  _SimpleBuffer(int size) {
      _block = new _Block(size);
  }

  _Block _block;

  void _logState() => _log('block start: ${_block.start}, block end: ${_block.end}, index: ${index}, bytesAvailable: $bytesAvailable.');
  void _log(String msg) => print('_SimpleBuffer: $msg');

  int get index => block.offset + block.start;
  _Block get block => _block;
  int get contiguousBytesAvailable => block.length;
  int get bytesAvailable => block.length;

  void appendFromSocket(Socket _socket, int readSize) {
    if (_block.length == 0) {
      _block.start = 0;
      _block.end = 0;
    }

    int bytesRead = _socket.readList(_block.list, _block.start, readSize);
    _block.end += bytesRead;
    _log('Read $bytesRead bytes.');
  }

  //TODO rename - it's name doesn't make sense anymore.
  /// Check if we've advanced out of the current block.
  /// After a call to check, contiguousBytesAvailable will always be > 0, unless
  /// bytesAvailable is zero because there's no more data in the buffer.
  /// Otherwise an exception is thrown.
  void checkBytesAvailable() {
    assert(block.start < block.list.length);
  }

  void skip(int nBytes) {
    _log('Skip $nBytes.');
    checkBytesAvailable();
    block.start += nBytes;
    checkBytesAvailable();
  }

}


class _Block {
  _Block(int size) : list = new Uint8List(size);
  final Uint8List list;
  int offset = 0;
  int start = 0;
  int end = 0;
  int get length => end - start;
}

class _CircularBlockBuffer {
  
  _CircularBlockBuffer(this.blockSize, int initialBlocks) {
    if (initialBlocks < 1)
      throw new Exception('initialBlocks: $initialBlocks must be >= 1.'); //TODO ArgumentError ??
    
    _blocks = new List<_Block>(initialBlocks);
    for (int i = 0; i < initialBlocks; i++)
      _blocks[i] = new _Block(blockSize);
  }
  
  List<_Block> _blocks;
  int _headIndex = 0;
  int get _tailIndex => (_headIndex + 1) % _blocks.length;
  int _currentIndex = 0;
  _Block get _head => _blocks[_headIndex];
  _Block get _tail => _blocks[_tailIndex];
  
  void _log(String msg) => print('_CircularBlockBuffer: $msg');
  
  void _logState() {
    _log('index: $index, head: $_headIndex, tail: $_tailIndex, current: $_currentIndex, blocks: ${_blocks.length}');
    int i = _headIndex;
    for (;;) {
      var block = _blocks[i];
      _log('block offset: ${block.offset}, block start: ${block.start}, block end: ${block.end}.');
      if (i == _tailIndex)
        break;
      i = (i + 1) % _blocks.length;
    }
  }
  
  final int blockSize;  
  
  int get index => block.offset + block.start;
  
  _Block get block => _blocks[_currentIndex];
  
  int get contiguousBytesAvailable => block.length;

  int get bytesAvailable {
    if (_head == _tail) {
      assert(_head == block);
      return block.length;
    }
    
    int count = 0;
    int i = _headIndex;
    for (;;) {
      count += _blocks[i].length;
      if (i == _tailIndex)
        break;
      i = (i + 1) % _blocks.length;
    }
    
    return count;
  }

  void appendFromSocket(Socket _socket, int readSize) {
    var b = _allocateBlock();
    int bytesRead = _socket.readList(b.list, 0, readSize);     
    b.start = 0;
    b.end = bytesRead;
    _log('Read $bytesRead bytes.');
  }
  
  /// After a call to allocateBlock() there will be a free block available, 
  /// ready to be written into.
  // TODO add a maximum buffer size, error if too big.
  _Block _allocateBlock() {
    if (_head.length == 0) {
      // Reuse an existing block at the start of the queue.
      // Rotate the current head block to the tail position.
      _headIndex = (_headIndex + 1) % _blocks.length;
      _log('Allocate - reused an existing head block, blocks: ${_blocks.length}, head: ${_headIndex}, tail: ${_tailIndex}, current: ${_currentIndex}.');
    } else {
      // Allocate a new block
      
      // First copy references to existing blocks into a new fixed size list 
      // which is 1 block longer.
      var list = new List<_Block>(_blocks.length + 1);
      if (_headIndex == _tailIndex) {
        list[0] = _blocks[0];
      } else {
        int j = 0;
        int i = _headIndex;
        for (;;) {
          list[j] = _blocks[i];
          j++;
          if (i == _tailIndex)
            break;
          i = (i + 1) % _blocks.length;
        }
      }
      
      // Allocate the new block.
      var b = new _Block(blockSize); 
      b.offset = _tail.offset + _tail.end;
      list[_blocks.length] = b;
      
      // Reset head and tail.
      _headIndex = 0;  
      _blocks = list;
      
      _log('Allocated a new block, blocks: ${_blocks.length}, head: ${_headIndex}, tail: ${_tailIndex}, current: ${_currentIndex}, offset: ${b.offset}.');
    }
    
    // A reused or newly allocated block is now in the tail position.
    _tail.offset = block.offset + block.end;
    _tail.start = 0;
    _tail.end = 0;
    
    return _tail;
  }
  
  //TODO rename - it's name doesn't make sense anymore.
  /// Check if we've advanced out of the current block.
  /// After a call to check, contiguousBytesAvailable will always be > 0, unless
  /// bytesAvailable is zero because there's no more data in the buffer.
  /// Otherwise an exception is thrown.
  void checkBytesAvailable() {
     
    if (block.start >= block.end) {
      _log('Index outside of the current block, index: $index, block offset: ${block.offset}, block start: ${block.start}, block end: ${block.end}.');
      
      int n = block.start - block.end;
      
      assert(n >= 0);
      
      if (_currentIndex == _tailIndex && block.start > block.end)
        throw new Exception('Buffer: stepped out of block, index: $index.');
      
      _log('Finished reading from block, block end: ${block.end}, index: $index.');
      block.start = block.end;
      
      // Move to the next block
      _currentIndex = (_currentIndex + 1) % _blocks.length;
      
      assert(block.start < block.list.length || (block.start == block.list.length && bytesAvailable == 0));
      assert(contiguousBytesAvailable > 0 || bytesAvailable == 0);
      
      // Skip remaining bytes - notice this will work recursively if we skip out of
      // the subsequent block too.
      if (n > 0)
        skip(n);
      
      assert(block.start < block.list.length || (block.start == block.list.length && bytesAvailable == 0));
      assert(contiguousBytesAvailable > 0 || bytesAvailable == 0);
    }
  }
    
  void skip(int nBytes) {
    _log('Skip $nBytes.');
    checkBytesAvailable();
    block.start += nBytes;
    checkBytesAvailable();
  }
}

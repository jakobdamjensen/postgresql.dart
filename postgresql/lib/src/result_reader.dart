
class _ResultReader implements ResultReader, ZeroCopyResultReader {
  
  _ResultReader(this._msgReader);
  
  int _state = _STATE_MSG_HEADER;
  final _MessageReader _msgReader;
  
  ResultReaderEventType _event;
  int _command;
  int _row = -1;
  int _column = -1;
  int _colSize;
  
  Dynamic _error;
  
  int _msgType;
  int _msgLength;
  int _colCount;
  
  List<ColumnDesc> columnDescs;
  
  int get command => _command;
  int get row => _row;
  int get column => _column;
  int get columnSizeInBytes => _colSize;
  ColumnDesc get columnDesc => columnDescs[column];
  int get columnCount => columnDescs.length;
  ResultReaderEventType get event => _event;
  Dynamic get error => _error;
  
  //TODO emit START/END_COMMAND events.
  // Read another value if there is one in the buffer.
  bool hasNext() {
    switch (_state) {
      case _STATE_MSG_HEADER: return _hasNextMsgHeader();
      case _STATE_COL_HEADER: return _hasNextColHeader();
      case _STATE_COL_DATA: return _hasNextColData();
      default:
        assert(false);
    }
  }
  
  bool _hasNextMsgHeader() {
    assert(_state == _STATE_MSG_HEADER);
    
    var r = _msgReader;
    
    _msgType = r.peekByte();
    
    if (_msgType != _MSG_DATA_ROW) // TODO && msgType != _MSG_COMMAND_COMPLETE)
      return false;
    
    // If there's not enough data to read the data row header then bail out and 
    // wait for more data to arrive.
    if (r.bytesAvailable < 7)
      return false;
    
    r.startMessage();
    assert(r.messageType == _MSG_DATA_ROW);
    assert(r.messageLength >= 6);
    
    _colCount = r.readInt16();
    assert(_colCount >= 0);
    
    //TODO check colCount matches the RowDescription message.    
    
    _row++;
    _column = -1;
    _event = START_ROW;
    _state = _STATE_COL_HEADER;
    
    return true;
  }
  
  bool _hasNextColHeader() {
    assert(_state == _STATE_COL_HEADER);
    
    if (_column + 1 >= _colCount) {      
      _column = -1;
      _event = END_ROW;
      _state = _STATE_MSG_HEADER;
      return true;
    }
    
    var r = _msgReader;
    
    // Check there's enough data to read the header, otherwise bail out and read
    // more data.
    if (r.bytesAvailable < 5)
      return false;
    
    _colSize = r.readInt32();
    assert(_colSize > 0);
    
    _column++;
    
    //TODO check if colSize is allowed to be big.
    // I.e. check against data type oids.
    
    // Handle column fragments.
    if (_colSize > r.contiguousBytesAvailable) {
      print('Data fragment, column size: $_colSize, bytes available in buffer: ${r.contiguousBytesAvailable}.');      
      _event = COLUMN_DATA_FRAGMENT;
      _state = _STATE_COL_DATA;
      return true;
    }
    
    _event = COLUMN_DATA;
    _state = _STATE_COL_HEADER; 
    return true;
  }
  
  bool _hasNextColData() {
    throw new Exception('Column data fragments not implemented.');
    
    assert(_state == _STATE_COL_DATA);
    
    // Continue reading data in the buffer.
  }
  
  
  // These can only be called when event == COLUMN_DATA
  
  //TODO Check this, and also make sure the value is only read once, so that we 
  // don't end up losing sync with the data stream.
  
  //TODO Encode based on default mapping using type oid in row description.
  Dynamic get value => asString();
  
  List<int> asBytes() { 
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.asBytes() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeBytes(columnDesc, colData, 0, colData.length);
  }
  
  String asString() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.asString() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeString(columnDesc, colData, 0, colData.length);
  }
  
  int asInt() { 
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.asInt() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeInt(columnDesc, colData, 0, colData.length);    
  }
  
  bool asBool() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.asBool() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeBool(columnDesc, colData, 0, colData.length);    
  }

  Decimal asDecimal() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.asDecimal() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeDecimal(columnDesc, colData, 0, colData.length);    
  }
  
  // This can only be called when event == END_COMMAND
  //TODO parse this and return meaning result.
  String get commandTag() { throw new Exception('Not implemented.'); }

  //TODO implment ZeroCopyResultReader interface.
  // These can only be called when event == COLUMN_DATA_FRAGMENT
  //Uint8List get fragment;
  //int get fragmentOffset;
  //int get fragmentLength;
  
  //Uint8List get fragment2;
  //int get fragment2Offset;
  //int get fragment2Length;
  
  //void readString(void callback(String s));
  
  //InputStream asInputStream();

}
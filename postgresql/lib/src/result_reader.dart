
class _ResultReader implements ResultReader {
  
  _ResultReader(this._msgReader) {
    _command = -1;
    _state = _STATE_MSG_HEADER;
  }
  
  int _state;
  final _MessageReader _msgReader;
  
  ResultReaderEventType _event;
  int _command = -1; // Command index.
  int _row; // Row index.
  int _column; // Column index.
  int _colSize;
  int _colCount; // Number of columns in this row.
  String _commandTag; // Information returned after completing a command.
  List<ColumnDesc> _columnDescs;
  
  ResultReaderEventType get event => _event;
  int get command => _command;
  int get row => _row;
  int get column => _column;
  int get columnSizeInBytes => _colSize;
  ColumnDesc get columnDesc => _columnDescs[column];
  List<ColumnDesc> get columnDescs => _columnDescs;
  int get columnCount => columnDescs.length;
  
  // Pull next event.
  // i.e. Read another piece of data from the buffer. If there's no more data
  // to read, i.e. no more events, then return false.
  bool hasNext() {
    
    switch (_state) {
        
      case _STATE_MSG_HEADER:
        var mtype = _msgReader.peekByte();
        if (mtype == _MSG_COMMAND_COMPLETE) {
          return _parseCommandComplete();
          
        } else if (mtype == _MSG_ROW_DESCRIPTION) {
          return _parseRowDescription();
          
        } else if (mtype == _MSG_DATA_ROW) {
          return _parseDataRow();
        }
        return false; // Bail out to the main loop and handle the message there.
        
      case _STATE_COL_HEADER:
        return _parseColHeader();
        
      case _STATE_COL_FRAGMENT:
        return _parseColFragment();
        
      default:
        assert(false);
    }
  }
  
  bool _parseDataRow() {
    assert(_state == _STATE_MSG_HEADER);
    
    var r = _msgReader;
    
    // If there's not enough data to read the data row header then bail out and 
    // wait for more data to arrive.
    if (r.bytesAvailable < 7)
      return false;
    
    r.startMessage();
    assert(r.messageType == _MSG_DATA_ROW);
    assert(r.messageLength >= 6);
    
    _colCount = r.readInt16();
    
    // Check the column count in the DataRow message matches the count in
    // the RowDescription message.
    //TODO figure out how to fire an error here. Need to call Connection._fatalError().
    assert(_colCount == columnDescs.length);
    
    _row++;
    _column = -1;
    _event = START_ROW;
    _state = _STATE_COL_HEADER;
    
    return true;
  }
  
  bool _parseColHeader() {
    assert(_state == _STATE_COL_HEADER);
    
    if (_column + 1 >= _colCount) {      
      _column = -1;
      _event = END_ROW;
      _state = _STATE_MSG_HEADER;
      //TODO check message length and bytes read match.
      //TODO figure out how to do error handling at this level.
      // Note the length reported in the message header excludes the message
      // type byte, hence +1.
      //if (r.messageBytesRead != r.messageLength + 1) {
      //  _error(new _PgError.client('Message contents do not agree with length in message header. Message type: ${_itoa(r.messageType)}, bytes read: ${r.messageBytesRead}, message length: ${r.messageLength}.'));
      //  r.skipMessage();
      //}
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
  
  bool _parseColFragment() {
    throw new Exception('Column data fragments not implemented.');
    
    assert(_state == _STATE_COL_FRAGMENT);
    
    // Continue reading data in the buffer.
  }
  
  //TODO consider writing a parser to handle long row description messages.
  // As these may be longer than 30k.
  bool _parseRowDescription() {
    
    var r = _msgReader;    
    
    //FIXME check there is enough data to continue. otherwise read more.
    r.startMessage();
    
    if (r.bytesAvailable < r.messageLength + 1 - 5) { //TODO add r.messageBytesAvailable
      return false;
    }
    
    int cols = r.readInt16();
    
    //TODO report error, rather than assert.
    assert(cols >= 0);
    
    var list = new List<ColumnDesc>(cols);
    
    for (int i = 0; i < cols; i++) {      
      var name = r.readString();
      int fieldId = r.readInt32();
      int tableColNo = r.readInt16();
      int fieldType = r.readInt32();
      int dataSize = r.readInt16();
      int typeModifier = r.readInt32();
      int formatCode = r.readInt16();
      
      list[i] = new _ColumnDesc(i, name, fieldId, tableColNo, fieldType, dataSize, typeModifier, formatCode);
    }
    
    _columnDescs = list;
    
    _event = START_COMMAND;
    _state = _STATE_MSG_HEADER;
    _command++;
    _row = -1;
    _column = -1;
    _commandTag = null;
    return true;
  }
  
  bool _parseCommandComplete() {
    var r = _msgReader;
    
    assert(r.peekByte() == _MSG_COMMAND_COMPLETE);
    
    _event = END_COMMAND;
    r.startMessage();
    
    //FIXME handle message fragment - wait for more data. Does this work?
    //TODO put this logic into a getter, so I don't have to think
    // i.e. !r.completeMessageInBuffer
    if (r.bytesAvailable < r.messageLength - 4)
      return false; //FIXME Read more data. need to tell the connection that there is a message fragment.
    
    _commandTag = r.readString();
    
    //TODO check message length and bytes read match.
    //TODO figure out how to do error handling at this level.
    // Note the length reported in the message header excludes the message
    // type byte, hence +1.
    //if (r.messageBytesRead != r.messageLength + 1) {
    //  _error(new _PgError.client('Message contents do not agree with length in message header. Message type: ${_itoa(r.messageType)}, bytes read: ${r.messageBytesRead}, message length: ${r.messageLength}.'));
    //  r.skipMessage();
    //}
    return true;
  }
  
  
  // These can only be called when event == COLUMN_DATA
  
  //TODO Check this, and also make sure the value is only read once, so that we 
  // don't end up losing sync with the data stream.
  
  //TODO Encode based on default mapping using type oid in row description.
  Dynamic readDynamic() => readString();
  
  List<int> readBytes() { 
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readBytes() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeBytes(columnDesc, colData, 0, colData.length);
  }
  
  void readBytesInto(Uint8List buffer, int start) {
    throw new Exception('Not implemented');
  }
  
  String readString() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readString() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeString(columnDesc, colData, 0, colData.length);
  }
  
  int readInt() { 
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readInt() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeInt(columnDesc, colData, 0, colData.length);    
  }
  
  bool readBool() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readBool() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeBool(columnDesc, colData, 0, colData.length);    
  }

  Decimal readDecimal() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readDecimal() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return decodeDecimal(columnDesc, colData, 0, colData.length);    
  }
  
  //TODO This can only be accessed when event == END_COMMAND
  String get commandTag => _commandTag;

  int get fragmentSizeInBytes { throw new Exception('Not implemented'); }
  
  bool readStringFragment(StringBuffer buffer) {
    throw new Exception('Not implemented.');
  }
  
  bool readBytesFragment(Uint8List buffer, int start) {
    throw new Exception('Not implemented.');
  }
  
}
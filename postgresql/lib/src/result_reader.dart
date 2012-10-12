
// Reads a result sequence of a query. A result sequence is:
// (RowDescription, DataRow*, CommandComplete)* ReadyForQuery

// ReadyForQuery is handled in the connection main loop. This class handles
// the rest of the sequence.

// An ErrorResponse could happen at any time, this stops any more results from
// being processed, and will be followed by a ReadyForQuery.

// NoticeResponses message can happen at anytime and will be handled by the
// main loop.

//TODO CopyInResponse and CopyOutResponse are not yet implemented.

// See Postgresql docs 4.6.2.2 Simple Query 
// http://www.postgresql.org/docs/9.2/static/protocol-flow.html

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
  
  int _colStart; // The byte index of the column.
  int _fragmentStart; // The byte index of the start of the current fragment.
  int _fragmentSize; // The length of the current fragment.
  
  ResultReaderEventType get event => _event;
  int get command => _command;
  int get row => _row;
  int get column => _column;
  int get columnSizeInBytes => _colSize;
  ColumnDesc get columnDesc => _columnDescs[column];
  List<ColumnDesc> get columnDescs => _columnDescs;
  int get columnCount => columnDescs.length;
  
  // TODO only allow this to be called when in fragment state.
  
  //FIXME wrong - column is not the whole message.
  int get fragmentSizeInBytes => _fragmentSize;

  // TODO only allow this to be called when in fragment state.
  bool get lastFragment => _fragmentStart + _fragmentSize == _colStart + _colSize;
  
  // Pull next event.
  // i.e. Read another piece of data from the buffer. If there's no more data
  // to read, i.e. no more events, then return false.
  bool hasNext() {
    
    if (_msgReader.state == _MESSAGE_HEADER) {
      if (_msgReader.bytesAvailable < 5)
        return false;
      
      _msgReader.startMessage();
    }
    
    switch (_state) {
        
      case _STATE_MSG_HEADER:
        if (_msgReader.messageType == _MSG_COMMAND_COMPLETE) {
          return _parseCommandComplete();
          
        } else if (_msgReader.messageType == _MSG_ROW_DESCRIPTION) {
          return _parseRowDescription();
          
        } else if (_msgReader.messageType == _MSG_DATA_ROW) {
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
    
    assert(r.messageType == _MSG_DATA_ROW);
    assert(r.messageLength >= 6);
    
    // If there's not enough data to read the data row header then bail out and 
    // wait for more data to arrive.
    if (r.bytesAvailable < 2)
      return false;
    
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
    
    var r = _msgReader;
    
    if (_column + 1 >= _colCount) {
      
      //TODO check message length and bytes read match.
      //TODO figure out how to do error handling at this level.
      // Note the length reported in the message header excludes the message
      // type byte, hence +1.
      //if (r.messageBytesRead != r.messageLength + 1) {
      //  _error(new _PgError.client('Message contents do not agree with length in message header. Message type: ${_itoa(r.messageType)}, bytes read: ${r.messageBytesRead}, message length: ${r.messageLength}.'));
      //  r.skipMessage();
      //}
      
      if (r.messageBytesRemaining != 0)
        throw new Exception('Lost sync.');
      
      r.endMessage();
      
      _column = -1;
      _event = END_ROW;
      _state = _STATE_MSG_HEADER;
      
      return true;
    }
    
    // Check there's enough data to read the header, otherwise bail out and read
    // more data.
    if (r.bytesAvailable < 4)
      return false;
    
    _colSize = r.readInt32();
    assert(_colSize > 0);
    
    _column++;
    
    //TODO check if colSize is allowed to be big.
    // I.e. check against data type oids.
    
    _colStart = r.index;
    
    // Handle column fragments.
    if (_colSize > r.bytesAvailable) {
      _state = _STATE_COL_FRAGMENT;
      return _parseColFragment();
    }
    
    _event = COLUMN_DATA;
    _state = _STATE_COL_HEADER; 
    return true;
  }
  
  bool _parseColFragment() {
    
    if (_msgReader.bytesAvailable < 1)
      return false;
    
    assert(_state == _STATE_COL_FRAGMENT);
    
    // Continue reading data in the buffer.
    _fragmentStart = _msgReader.index;
    int columnBytesRemaining = _colStart + _colSize - _fragmentStart;
    _fragmentSize = min(_msgReader.bytesAvailable, columnBytesRemaining);
    _event = COLUMN_DATA_FRAGMENT;
    
    if (lastFragment)
      _state = _STATE_COL_HEADER;
    else
      _state = _STATE_COL_FRAGMENT;
    
    print('############################');
    print('index: ${_msgReader.index}');
    print('messageStart: ${_msgReader.messageStart}');
    print('messageEnd: ${_msgReader.messageEnd}');
    print('messageBytesRemaining: ${_msgReader.messageBytesRemaining}');
    print('bytesAvailable: ${_msgReader.bytesAvailable}');
    print('contiguousBytesAvailable: ${_msgReader.contiguousBytesAvailable}');
    
    print('fragmentSizeInBytes: $fragmentSizeInBytes');
    print('lastFragment: $lastFragment');
    print('############################');
    
    return true;
  }
  
  //TODO consider writing a parser to handle long row description messages.
  // As these may be longer than 30k.
  bool _parseRowDescription() {
    
    var r = _msgReader;    
    
    assert(r.messageType == _MSG_ROW_DESCRIPTION);
    
    if (r.isMessageFragment)
      return false; //FIXME Read more data. need to tell the connection that there is a message fragment.
    
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

    //TODO check message length and bytes read match.
    //TODO figure out how to do error handling at this level.
    //if (r.messageBytesAvailable != 0) {
    //  _error(new _PgError.client('Message contents do not agree with length in message header. Message type: ${_itoa(r.messageType)}, bytes read: ${r.messageBytesRead}, message length: ${r.messageLength}.'));
    //  r.skipMessage();
    //}
    if (r.messageBytesRemaining != 0)
      throw new Exception('Lost sync.');
    
    r.endMessage();
    
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
    assert(r.messageType == _MSG_COMMAND_COMPLETE);
    
    _event = END_COMMAND;
    
    if (r.isMessageFragment)
      return false; //FIXME Read more data. need to tell the connection that there is a message fragment.
    
    _commandTag = r.readString();
    
    //TODO check message length and bytes read match.
    //TODO figure out how to do error handling at this level.
    //if (r.messageBytesAvailable != 0) {
    //  _error(new _PgError.client('Message contents do not agree with length in message header. Message type: ${_itoa(r.messageType)}, bytes read: ${r.messageBytesRead}, message length: ${r.messageLength}.'));
    //  r.skipMessage();
    //}
    if (r.messageBytesRemaining != 0)
      throw new Exception('Lost sync.');
    
    r.endMessage();
    
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
    return _decodeBytes(columnDesc, colData, 0, colData.length);
  }
  
  void readBytesInto(Uint8List buffer, int start) {
    throw new Exception('Not implemented');
  }
  
  String readString() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readString() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return _decodeString(columnDesc, colData, 0, colData.length);
  }
  
  int readInt() { 
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readInt() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return _decodeInt(columnDesc, colData, 0, colData.length);    
  }
  
  bool readBool() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readBool() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return _decodeBool(columnDesc, colData, 0, colData.length);    
  }

  Decimal readDecimal() {
    if (event != COLUMN_DATA)
      throw new Exception('ResultReader.readDecimal() called in invalid state, event: $event.');
    
    //TODO don't copy data here, just pass a reference to the buffer.
    var colData = _msgReader.readBytes(_colSize);
    return _decodeDecimal(columnDesc, colData, 0, colData.length);    
  }
  
  //TODO This can only be accessed when event == END_COMMAND
  String get commandTag => _commandTag;
  
  bool readStringFragment(StringBuffer buffer) {
    //TODO make a zero copy impl, rather than using readBytes().
    //TODO Perhaps standard lib should have a StringBuffer.addFromCharCodes(List<int> chars);
    // Note that bytes may be split accross two blocks, so may need to call
    // buffer.add() twice.
    if (_msgReader.bytesAvailable > 0) {
      var bytes = _msgReader.readBytes(min(_fragmentSize, _msgReader.bytesAvailable));
      print ('Got ${bytes.length} bytes from fragment');
      buffer.add(new String.fromCharCodes(bytes));
    }
  }
  
  bool readBytesFragment(Uint8List buffer, int start) {
    //TODO make a zero copy impl, rather than using readBytes().
    if (_msgReader.bytesAvailable > 0) {
      var bytes = _msgReader.readBytes(min(_fragmentSize, _msgReader.bytesAvailable));
      print ('Got ${bytes.length} bytes from fragment');
      for (int i = 0; i < bytes.length; i++) {
        buffer[start + i] = bytes[i];
      }
    }
  }
  
}
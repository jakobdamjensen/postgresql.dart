
Future<Connection> _connect([Settings settings = null]) {
  var c = new _Connection(settings);
  return c._connect();
}

class _Connection implements Connection {
    
  // Don't set this directly use _changeState().
  _ConnectionState _state = _NOT_CONNECTED;
  
  bool get _ok => _state == _IDLE || _state == _BUSY || _state == _READY;
  
  final Queue<_Query> _sendQueryQueue = new Queue<_Query>();
  _Query _query; // The query currently being sent, or having it's results processed.  
  Socket _socket;
  Uint8List _output;
  _MessageReader _reader;
  _MessageWriter _writer;
  final Completer<Connection> _connectCompleter = new Completer<Connection>();
  final Settings settings;
  
  _Connection(Settings settings)
      : this.settings = (settings == null) ? defaultSettings : settings {
        
    //TODO throw error via completer instead.
    // Or check settings value in the _connect() function.
    if (this.settings == null)
      throw new Exception('No connection settings specified.');
          
    _output = new Uint8List(OUTPUT_BUFFER_SIZE); //TODO make buffer sizes configurable.
    _reader = new _MessageReader(INPUT_BUFFER_SIZE, 2);
    _writer = new _MessageWriter(_output);
  }
  
  void _changeState(_ConnectionState state) {
    if (_state != state)
      _log('Connection state change: ${_state} => ${state}');
    _state = state;
  }
  
  void _log(String msg) => print(msg);
  
  // Something bad happened, but keep connection alive.
  void _error(_PgError err) {
    _log(err.toString());

    if ((_state == _NOT_CONNECTED 
        || _state == _SOCKET_CONNECTING
        || _state == _SOCKET_CONNECTED
        || _state == _AUTHENTICATING
        || _state == _AUTHENTICATED)
          && !_connectCompleter.future.isComplete
          && err.type != SERVER_NOTICE) {

      _connectCompleter.completeException(err);

    } else if ((_state == _BUSY || _state == _READY) 
        && err.type != SERVER_NOTICE) {
      
      assert(_query != null);
      _query.changeState(_COMPLETE);
      _query._streamer.completeException(err);
      
    } else {
      if (settings.onUnhandledErrorOrNotice != null)
        settings.onUnhandledErrorOrNotice(err);
    }
  }
  
  // Something really bad happened, close connection and report error.
  void _fatalError(PgError err) {
    _log('Fatal error: $err');
    
    _changeState(_CLOSED);
    _error(err);

    // Close the connection without sending a close message.
    _socket.close();
  }
  
  void close() {
    _changeState(_CLOSED);

    // Send terminate message.
    // Don't worry if there's not enough room in the send buffer to send the
    // message, as we will close the socket anyway.
    _writer.startMessage(_MSG_TERMINATE);
    _writer.endMessage();
    _sendMessage();
    
    _socket.close();
  }
  
  Future<Connection> _connect() {
    assert(_state == _NOT_CONNECTED);
    
    try {
      _socket = new Socket(settings.host, settings.port);
    } catch (ex) {
      _fatalError(new _PgError.client('Failed to open socket. $ex'));
      return _connectCompleter.future;
    }
    
    _socket
    ..onConnect = () {
      _changeState(_SOCKET_CONNECTED);
      _sendStartupMessage();
    }
    ..onError = (err) { _fatalError(new _PgError.client('Socket error. $err')); }
    ..onClosed = () { _fatalError(new _PgError.client('Socket closed.')); }
    ..onData = _readData;
    
    _changeState(_SOCKET_CONNECTING);
    
    return _connectCompleter.future;
  }
  
  void _sendStartupMessage() {
    assert(_state == _SOCKET_CONNECTED);
    
    _writer.startMessage(_MSG_STARTUP);
    _writer.writeInt32(_PROTOCOL_VERSION);
    _writer.writeString('user');
    _writer.writeString(settings.username);
    _writer.writeString('database');
    _writer.writeString(settings.database);
    
    //TODO write other params.
    
    _writer.writeByte(0); // null byte to end param list.
    
    _writer.endMessage();
    _sendMessage();
    
    _changeState(_AUTHENTICATING);
  }
  
  Query query(String sql, {int timeoutMillis, ResultMapper resultMapper}) {

    if (sql == null || sql == '')
      throw new Exception('Sql is null or empty.');
    
    //TODO
    if (timeoutMillis != null)
      throw new Exception('Query timeout not implemented.');
    
    if (resultMapper == null)
      resultMapper = new _DynamicRowResultMapper(); 
    
    if (!_ok)
      throw new Exception('Attempted a query on a closed connection.');
    
    var q = new _Query(sql, resultMapper, new _ResultReader(_reader));            
    _sendQueryQueue.addLast(q);
    q.changeState(_QUEUED);
    _processSendQueryQueue();
    return q;
  }
  
  //TODO
  Future<int> exec(String sql) {
    throw new Exception('Not implemented.');
  }
  
  void _processSendQueryQueue() {
    
    if (!_ok)
      return;
    
    if (_sendQueryQueue.length == 0)
      return; // No more queries to process.
    
    if (_query != null)
      return; // Another query already being processed.
    
    var q = _sendQueryQueue.removeFirst();
    
    assert(q.state == _QUEUED);
    q.changeState(_SENDING);
    _query = q;
   
    //TODO streamed writes for long strings.
    // At the moment _writer will just bomb if it doesn't fit in the output
    // buffer. 
    _writer.startMessage(_MSG_QUERY);
    _writer.writeString(_query.sql);
    _writer.endMessage();
    
    _changeState(_BUSY);

    _sendMessage();

    //FIXME, This needs to be occasionally delayed, as the socket write
    // sometimes needs to be completed asynchronously.
    // Perhaps send message should return a bool, and take a callback.
    // Or just return a future.
    _query.changeState(_SENT);
  }  
  
  void _handleAuthenticationRequest(_MessageReader r) {
    
    if (_state != _AUTHENTICATING) {
      _fatalError(new _PgError.client('Received authentication request from server while in invalid state: $_state.'));
      return;
    }
    
    int authType = r.readInt32();
    
    if (authType == _AUTH_TYPE_OK) {
      _changeState(_AUTHENTICATED);      
      return;
    }
    
    // Only MD5 authentication is supported.
    if (authType != _AUTH_TYPE_MD5 && authType != _AUTH_TYPE_OK) {
      _fatalError(new _PgError.client('Unsupported or unknown authentication type: ${_authTypeAsString(authType)}, only MD5 authentication is supported.'));
      return;
    }
    
    var bytes = <int> [r.readByte(), r.readByte(), r.readByte(), r.readByte()];
    var salt = new String.fromCharCodes(bytes);
    var md5 = 'md5'.concat(_md5s(settings.passwordHash.concat(salt)));
        
    _writer.startMessage(_MSG_PASSWORD);
    _writer.writeString(md5);
    _writer.endMessage();
    
    _sendMessage();
  }
  
  void _handleReadyForQuery(_MessageReader r) {
    
    //TODO Check these states. Perhaps should allow more. 
    if (_state != _READY && _state != _AUTHENTICATED) {
      _fatalError(new _PgError.client('Received ReadyForQuery message from server while in invalid state: $_state.'));
      return;
    } 
    
    int c = r.readByte();
    
    if (c == 'I'.charCodeAt(0)) {
      // Do nothing.
    } else if (c == 'T'.charCodeAt(0)) {
      _error(new _PgError.client('Transaction handling not implemented.'));
    } else if (c == 'E'.charCodeAt(0)) {
      _error(new _PgError.client('Transaction handling not implemented.'));
    } else {
      _fatalError(new _PgError.client('Unknown ReadyForQuery transaction status: ${_itoa(c)}.'));
      return;
    }
    
    var s = _state;
    var q = _query;
    
    _query = null;
    _changeState(_IDLE);
    
    if (s == _AUTHENTICATED) {
      if (!_connectCompleter.future.isComplete)
        _connectCompleter.complete(this);
    } else {
      q.onQueryComplete();
      _processSendQueryQueue();
    }
  }
  
  // TODO handle long error messages, that don't fit into the buffer.
  void _handleErrorOrNoticeResponse(_MessageReader r, {bool isError}) {
    
    // Parse error message.
    
    int code = r.readByte();
    var fields = new Map<String,String>();
    while(code != 0) {
      var char = new String.fromCharCodes([code]);
      var msg = r.readString();
      fields[char] = msg;
      code = r.readByte();
    }
    
    var err = isError ? new _PgError.error(fields)
                        : new _PgError.notice(fields); 
    
    if (_state == _AUTHENTICATING) {
      // Authentication failed.
      _fatalError(err);
      return;
    } 
          
    //TODO Check what libpq sets the state too after an error.
    // After issuing a query if there is an error, it should also send a ready
    // for query, which will cause the state to go back to idle.
    // if (_state == _BUSY) {
    //   _changeState(_READY);
    //   return;
    // }
    
    // Errors aren't expected in non-busy state - but worth logging them anyways
    // if they happen.
    
    _error(err);
  }
  
  //TODO consider writing a parser to handle long row description messages.
  // As these may be longer than 30k.
  void _handleRowDescription(_MessageReader r) {
    
    if (_state != _BUSY) {
      _error(new _PgError.client('Received RowDescription message while not in busy state, state: $_state.'));
      r.skipMessage();
      return;
    }
    
    assert(_query != null);
    
    int cols = r.readInt16();
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
    
    _query.onRowDescription(list);
    
    _changeState(_READY);
  }
  
  void _handleCommandComplete(_MessageReader r) {
    if (_state != _READY) {
      _error(new _PgError.client('Received CommandComplete message while not in ready state, state: $_state.'));
      r.skipMessage();
      return;
    }
    
    var commandTag = r.readString();
    _query.onCommandComplete(commandTag);
  }
  
  // Sends a message stored in the output buffer.
  // Note if the socket buffers are full, the message will not be sent
  // immediately (i.e. asynchronously, instead of synchronously as per usual).
  void _sendMessage() {
    try {
      
      //This is non-blocking, returns a count of how many bytes were written to the buffer.
      // If a full message is not written, set Socket.onWrite to queue the next write.
      int bytesWritten = _socket.writeList(_output, 0, _writer.bytesWritten);
      
      if (bytesWritten == _writer.bytesWritten) {
        _log('Sent $bytesWritten bytes.');
      } else {
        //FIXME
        throw new Exception('writeList() failed - writeList queueing not yet implemented.');
        // Set io state writing. ??
        //_socket.onWrite = _continueSending;
      }
      
    } catch (ex) {
      _fatalError(new _PgError.client('Socket write error.'));
    }
  }
    
  void _readData() {

    var r = _reader;
    
    // Check to see if the connection is in a valid reading state.
    if (!_ok && _state != _AUTHENTICATING && _state != _AUTHENTICATED)
      return;

    // Allow socket onData handler to finish after every 100 reads when reading
    // a large amount of data.
    // TODO make this configurable.
    // Also prevents errors from causing infinite loops.
    SOCKET_READ: for(int i = 0; i < 100; i++) {
      
      if (r.bytesAvailable > 0)
        _log('Message fragment left in buffer - bytesAvailable: ${r.bytesAvailable}.');
      
      r.appendFromSocket(_socket);
      
      // Debugging
      r._buffer._logState();
      
      if (r.bytesAvailable < 5)
        return; // Wait for more data.
      
      NEXT_MSG: for(;;) {

        // Check to see if the connection is in a valid reading state.
        if (!_ok && _state != _AUTHENTICATING && _state != _AUTHENTICATED)
          return;
        
        // Make sure there is enough data available to read the message header.
        if (r.bytesAvailable < 5)
          continue SOCKET_READ;
        
        // Peek at the message type.
        int mtype = r.peekByte();
        
        // In authenticating state only handle a subset of the message types.
        if (_state == _AUTHENTICATING
            && mtype != _MSG_AUTH_REQUEST
            && mtype != _MSG_ERROR_RESPONSE) {
          
          _fatalError(new _PgError.client('Unexpected message type. Are you sure you connect to a postgresql database? MsgType: \'${_itoa(mtype)}\'.'));
          return;
        }
          
        // In authenticated state only handle a subset of the message types.
        if (_state == _AUTHENTICATED
            && mtype != _MSG_BACKEND_KEY_DATA 
            && mtype != _MSG_PARAMETER_STATUS
            && mtype != _MSG_READY_FOR_QUERY
            && mtype != _MSG_ERROR_RESPONSE
            && mtype != _MSG_NOTICE_RESPONSE) {
            
          _fatalError(new _PgError.client('Unexpected message type while in authenticated state: ${_itoa(mtype)}.'));
          return;
        }

        // If it's a large message type, then we'll need to hand over to a
        // separate routine to handle it, as these can handle message fragments
        // and don't require the entire message to be read into the buffer.
        
        if (mtype == _MSG_DATA_ROW) { // or a partial message and state is query_reading.
          if (_state != _READY) {
            _error(new _PgError.client('Received DataRow message while not in ready state, state: $_state.'));
            r.skipMessage();
            continue NEXT_MSG;
          }
          
          if (r.bytesAvailable < 7)
            continue SOCKET_READ; // Read more data.

          _query.onDataRow();
          continue NEXT_MSG;
        }
        
        // Currently handled with standard messages.
        //TODO implement me. Read data without buffering it all.
        //if (mtype == _MSG_ERROR_RESPONSE || mtype == _MSG_NOTICE_RESPONSE) {
          // Hand over to fragment reader.
          //continue NEXT_MSG;
        //}  
        
        // Currently handled with standard messages.
        // TODO need to handle large row description messages. Currently this
        // could lead to the buffer growing to a huge size.
        //if (mtype == _MSG_ROW_DESCRIPTION) {
        // continue NEXT_MSG;
        //}
        
        //if (mtype == _MSG_FUNCTION_CALL_RESPONSE
        //         || mtype == _MSG_NOTIFICATION_RESPONSE
        //         || mtype == _MSG_COPY_DATA) {
          // Not implemented.
          //TODO skip data without buffering.
          //continue NEXT_MSG;
        //}

        
        // Standard size message handlers. These messages are always less than 
        // 30k, so we can buffer them safely.

        // Parse message header - advances 5 bytes.
        r.startMessage();
        
        //_log('Read message header, type: ${_itoa(r.messageType)}, code: ${r.messageType}, length: ${r.messageLength}, offset: ${r.messageStart}.');

        // Check for a sane message size - need to prevent accidently reading a
        // massive number of bytes into the buffer.
        if (!_checkMessageLength(r.messageType, r.messageLength)) {
          _fatalError(new _PgError.client('Bad message length.')); //FIXME message.
          return;
        }
        
        // If only part of a message is left in buffer then read more data.
        // -5 as the header has already been read.
        if (r.bytesAvailable < r.messageLength + 1 - 5) //TODO add r.messageBytesAvailable
          continue SOCKET_READ;

        // Dispatch to message handler method.
        _handleMessage(r);
        
        if (_state == _CLOSED)
          return;
                
        // Note the length reported in the message header excludes the message
        // type byte, hence +1.
        if (r.messageBytesRead != r.messageLength + 1)
            _error(new _PgError.client('Message contents do not agree with length in message header. Message type: ${_itoa(r.messageType)}, bytes read: ${r.messageBytesRead}, message length: ${r.messageLength}.'));
            
        // Get ready to handle the next message in the buffer.
        // Use the message length information from the header.
        r.skipMessage();
      }
    }
  }

  bool _handleMessage(_MessageReader r) {
    
    var t = r.messageType;
    
    _log('Handle message: ${_itoa(t)} ${_messageName(t)}.');
    
    switch (t) {
      case _MSG_AUTH_REQUEST: _handleAuthenticationRequest(r); break;
      case _MSG_COMMAND_COMPLETE: _handleCommandComplete(r); break;
      case _MSG_READY_FOR_QUERY: _handleReadyForQuery(r); break;
      case _MSG_ROW_DESCRIPTION: _handleRowDescription(r); break;
      
      case _MSG_ERROR_RESPONSE: _handleErrorOrNoticeResponse(r, isError: true); break;
      case _MSG_NOTICE_RESPONSE: _handleErrorOrNoticeResponse(r, isError: false); break;
      
      case _MSG_BACKEND_KEY_DATA:
      case _MSG_PARAMETER_STATUS:
        //_log('Ignoring unimplemented message type: ${_itoa(t)} ${_messageName(t)}.');
        r.skipMessage();
        break;

      case _MSG_NOTIFICATION_RESPONSE:
      case _MSG_BIND:
      case _MSG_BIND_COMPLETE:
      case _MSG_CLOSE_COMPLETE: 
      case _MSG_COMMAND_COMPLETE: 
      case _MSG_COPY_DATA:
      case _MSG_COPY_DONE:
      case _MSG_COPY_IN_RESPONSE:
      case _MSG_COPY_OUT_RESPONSE:
      case _MSG_COPY_BOTH_RESPONSE:
      case _MSG_DATA_ROW:
      case _MSG_EMPTY_QUERY_REPONSE:
      case _MSG_FUNCTION_CALL_RESPONSE:
      case _MSG_NO_DATA:
      case _MSG_PARAMETER_DESCRIPTION:
      case _MSG_PARSE_COMPLETE:
      case _MSG_PORTAL_SUSPENDED:
        _error(new _PgError.client('Unimplemented message type: ${_itoa(t)} ${_messageName(t)}.'));
        r.skipMessage();
        break;
      default:
        _error(new _PgError.client('Unknown message type received: ${_itoa(t)} ${_messageName(t)}.'));
        r.skipMessage();
        break;
    }
  }
  
  bool _checkMessageLength(int msgType, int msgLength) {
    
    if (_state == _AUTHENTICATING) {
      if (msgLength < 8) return false;
      if (msgType == _MSG_AUTH_REQUEST && msgLength > 2000) return false;
      if (msgType == _MSG_ERROR_RESPONSE && msgLength > 30000) return false;
    } else {
      if (msgLength < 4) return false;
      
      // These are the only messages from the server which may exceed 30,000
      // bytes.
      if (msgLength > 30000 && (msgType != _MSG_NOTICE_RESPONSE
          && msgType != _MSG_ERROR_RESPONSE
          && msgType != _MSG_COPY_DATA
          && msgType != _MSG_ROW_DESCRIPTION
          && msgType != _MSG_DATA_ROW
          && msgType != _MSG_FUNCTION_CALL_RESPONSE
          && msgType != _MSG_NOTIFICATION_RESPONSE)) {
        return false;
      }
    }
    return true;
  }

}

String _md5s(String s) {
  var hash = new MD5();
  hash.update(s.charCodes());
  return CryptoUtils.bytesToHex(hash.digest());
}

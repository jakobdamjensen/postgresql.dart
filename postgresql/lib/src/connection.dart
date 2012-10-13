
Future<Connection> _connect([Settings settings = null]) {
  
  settings = (settings == null) ? defaultSettings : settings;    
  if (settings == null)
    throw new Exception('Settings not specified.');
  
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
  _MessageReader _reader;
  _MessageWriter _writer;
  final Completer<Connection> _connectCompleter = new Completer<Connection>();
  final Settings settings;
  
  _Connection(this.settings) {          
    //TODO make buffer/read sizes configurable.
    _reader = new _MessageReader(_SOCKET_READ_SIZE, 2);
    _writer = new _MessageWriter(_SOCKET_WRITE_SIZE, 1);
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
      _changeState(_READY);
      _query.completeException(err);
      
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

    // The state change needs to be occasionally delayed, as the socket write
    // needs to be completed asynchronously if the buffers are full.
    _sendMessage(() => _query.changeState(_SENT));
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
      q.complete();
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
    
  // Sends messages stored in the output buffer. Note if the socket buffers are
  // full, the message will not be sent immediately. In this case the caller
  // will need to wait for the callback to called to know the message has been
  // sent. If the message send fails a fatal error will be triggered causing
  // the connection to be closed.
  void _sendMessage([void callback()]) {
    try {
      _writer.writeToSocket(_socket, (err) {
        if (err == null && callback != null)
          callback();
        if (err != null)
          _fatalError(err);
      });
    } catch (ex) {
      _fatalError(new _PgError.client('Socket write error.'));
    }
  }    

  void _readData() {

    // Allow socket onData handler to finish after every 100 reads when reading
    // a large amount of data. This make sure an isolate doesn't get to greedy
    // and hog a thread. Returning back will allow the scheduler to reschedule.
    // TODO make this configurable.
    SOCKET_READ: for(int i = 0; i < 100; i++) {
    
      // The reader could be one of the following states:
      //  -- buffer is empty
      //  -- buffer has data and the:
      //      -- reader is at a message header position
      //      -- reader is at a message body start position
      //      -- reader is within a message body, at some offset
    
      // Check to see if the connection is in a valid reading state.
      if (!_ok && _state != _AUTHENTICATING && _state != _AUTHENTICATED)
        return;
      
      //TODO make readSize configurable.
      int bytesRead = _reader.appendFromSocket(_socket);
    
      if (bytesRead == 0)
        return;
      
      // Print debugging info
//    int i = _input.block.start;
//    while (i < _input.block.end - 5) {
//      int mtype = _input.block.list[i];
//      int a = _input.block.list[i + 1];
//      int b = _input.block.list[i + 2];
//      int c = _input.block.list[i + 3];
//      int d = _input.block.list[i + 4];
//      int len = _decodeInt32(a, b, c, d);
//      print('########### Message: ${_itoa(mtype)} $mtype ${_messageName(mtype)} length: $len.');
//      i += len;
//    }
      
      if (_reader.state == _MESSAGE_WITHIN_BODY) {
        // This can only happen for types which can be processed in fragments
        // without requiring the entire message being read into the buffer.
        //TODO At the moment this can only happen for DataRow messages.
        
        if ((_state != _BUSY && _state != _READY)
            || _reader.messageType != _MSG_DATA_ROW) {
          _fatalError(new _PgError.client('Lost sync #1.')); //TODO message
        }
        
        assert(_query != null);
        _query.readResult();
        
        _reader._logState();
        
        //TODO This is likely a bit broken.
        // Consider allowing the reader to return a value of what to do next.
        // i.e. NEXT_MSG, SOCKET_READ, or error, or fatal error.
        if (_reader.bytesAvailable > 0 && 
            (_reader.state == _MESSAGE_HEADER
              || _reader.state == _MESSAGE_BODY)) {
          // Fallthrough - i.e. continue NEXT_MSG;
        } else if (_reader.state == _MESSAGE_WITHIN_BODY) {
          continue SOCKET_READ;
        } else if (_reader.bytesAvailable == 0) {
          continue SOCKET_READ;
        } else {
          assert(false);
        }
      }
      
      NEXT_MSG: for(;;) {

        // Check to see if the connection is in a valid reading state.
        if (!_ok && _state != _AUTHENTICATING && _state != _AUTHENTICATED)
          return;
        
        if (_reader.state == _MESSAGE_HEADER) {
          if (_reader.bytesAvailable < 5)
            continue SOCKET_READ;
      
          _reader.startMessage();
        }
        
        assert(_reader.state == _MESSAGE_BODY);

        _log('Received message ${_messageName(_reader.messageType)} length: ${_reader.messageLength}');
        
        if (!_checkMessageLength(_reader.messageType, _reader.messageLength)) {
          _fatalError(new _PgError.client('Lost sync #2.'));
        }

        // In authenticating state only handle a subset of the message types.
        if (_state == _AUTHENTICATING
            && _reader.messageType != _MSG_AUTH_REQUEST
            && _reader.messageType != _MSG_ERROR_RESPONSE) {
          
          _fatalError(new _PgError.client('Unexpected message type. Are you sure you connect to a postgresql database? MsgType: \'${_itoa(mtype)}\'.'));
          return;
        }
          
        // In authenticated state only handle a subset of the message types.
        if (_state == _AUTHENTICATED
            && _reader.messageType != _MSG_BACKEND_KEY_DATA 
            && _reader.messageType != _MSG_PARAMETER_STATUS
            && _reader.messageType != _MSG_READY_FOR_QUERY
            && _reader.messageType != _MSG_ERROR_RESPONSE
            && _reader.messageType != _MSG_NOTICE_RESPONSE) {
            
          _fatalError(new _PgError.client('Unexpected message type while in authenticated state: ${_itoa(mtype)}.'));
          return;
        }

        // Large message types - these may be more than 30k. Ideally they 
        // should be read by a streaming parser, so that the buffer doesn't
        // need to grow to accomodate them. Currently only DataRow messages are
        // read with a streaming parser.
        
        // Large message types:
        //   _MSG_NOTICE_RESPONSE
        //   _MSG_ERROR_RESPONSE
        //   _MSG_FUNCTION_CALL_RESPONSE
        //   _MSG_NOTIFICATION_RESPONSE
        //   _MSG_COPY_DATA
        //   _MSG_ROW_DESCRIPTION
        //   _MSG_DATA_ROW.
        
        // These mesage types are handled by the ResultReader class.
        //TODO handle empty query response.
        if (_reader.messageType == _MSG_ROW_DESCRIPTION
            || _reader.messageType == _MSG_DATA_ROW
            || _reader.messageType == _MSG_COMMAND_COMPLETE) {
          
          if (_state == _BUSY)
            _changeState(_READY);
          
          if (_state != _READY)
            _fatalError(new _PgError.client('Lost sync #3.')); //TODO message
          
          assert(_query != null);
          
          _query.readResult();

          //TODO This is likely a bit broken.
          // Consider allowing the reader to return a value of what to do next.
          // i.e. NEXT_MSG, SOCKET_READ, or error, or fatal error.
          if (_reader.bytesAvailable > 0 && 
              (_reader.state == _MESSAGE_HEADER
                || _reader.state == _MESSAGE_BODY)) {
            continue NEXT_MSG;
          } else if (_reader.state == _MESSAGE_WITHIN_BODY) {
            continue SOCKET_READ;
          } else if (_reader.bytesAvailable == 0) {
            continue SOCKET_READ;
          } else {
              assert(false);
          }
        }
        
        // Standard sized messages are handled after here. These messages are 
        // always less than 30k, so we can buffer them safely.
        // The entire message must be read into the buffer before continuing.
        if (_reader.isMessageFragment)
          continue SOCKET_READ;
      
        _handleMessage(_reader);
      
        if (_reader.messageBytesRemaining > 0) {
          _error(new _PgError.client('Bad message length.')); //TODO this should just be a warning.
      
          // Get ready to handle the next message in the buffer.
          // Trust the message length information from the header.
          _reader.skipMessage();
        }
      
        _reader.endMessage();
        
      } // end message loop.      
    } // end socket read loop.
  }
  
  bool _handleMessage(_MessageReader r) {
    
    var t = r.messageType;
    
    _log('Handle message: ${_itoa(t)} ${_messageName(t)}.');
    
    switch (t) {
      case _MSG_AUTH_REQUEST: _handleAuthenticationRequest(r); break;
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
  
      // These are handled by the ResultReader class.
      case _MSG_DATA_ROW:
      case _MSG_COMMAND_COMPLETE:
        _error(new _PgError.client('Unexpected message type: ${_itoa(t)} ${_messageName(t)}.'));
        r.skipMessage();
        break;
        
      default:
        _error(new _PgError.client('Unknown message type received: ${_itoa(t)} ${_messageName(t)}.'));
        r.skipMessage(); //FIXME this will probably just blow up, as this probably means the connection has lost sync.
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

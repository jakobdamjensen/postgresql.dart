
Future<Connection> _connect([Settings settings = null]) {
  var c = new _Connection(settings);
  return c._connect();
}

typedef void _EventHandler(_Event);

//TODO make me a const class.
class _Event {
  _Event(this.type);
  final int type;
  String toString() {
    if (type == _EVENT_CONNECTED)
      return '_EVENT_CONNECTED';
    else if (type == _EVENT_QUERY_SENT)
      return '_EVENT_QUERY_SENT';
    else if (type == _EVENT_QUERY_COMPLETE)
      return '_EVENT_QUERY_COMPLETE';
    else if (type == _EVENT_ERROR)
      return '_EVENT_ERROR';
    else if (type == _EVENT_FATAL_ERROR)
      return '_EVENT_FATAL_ERROR';
    else if (type == _EVENT_CLOSED)
      return '_EVENT_CLOSED';
    else
      return 'Unknown event: $type.';
  }
}

const int _EVENT_CONNECTED = 1;
const int _EVENT_QUERY_SENT = 2;
const int _EVENT_QUERY_COMPLETE = 3;
const int _EVENT_ERROR = 4;
const int _EVENT_FATAL_ERROR = 5;
const int _EVENT_CLOSED = 6;

class _Settings implements Settings {
  _Settings(this.host,
      this.port,
      String username,
      this.database,
      //this._params,
      String password)
    : username = username,
      passwordHash = _md5s(password.concat(username));
  
  final String host;
  final int port;
  final String username;
  final String database;
  //final Map<String,String> _params;
  final String passwordHash;
}

String _md5s(String s) {
  var hash = new MD5();
  hash.update(s.charCodes());
  return CryptoUtils.bytesToHex(hash.digest());
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
  List<PgError> _errors;
  final Completer<Connection> _connectCompleter;
  
  final Settings settings;
  final Queue<_Event> _events = new Queue<_Event>();
  
  _Connection(Settings settings)
      : this.settings = (settings == null) ? defaultSettings : settings,
        _connectCompleter = new Completer<Connection>() {
        
    if (this.settings == null)
      throw new Exception('No connection settings specified.');
          
    _output = new Uint8List(OUTPUT_BUFFER_SIZE);
    _reader = new _MessageReader(INPUT_BUFFER_SIZE, 2);
    _writer = new _MessageWriter(_output);
    
    _errors = new List<PgError>();
  }
  
  void _changeState(_ConnectionState state) {
    _log('Connection state change: ${_state} => ${state}');
    _state = state;
  }
  
  void _log(String msg) {
    print(msg);
  }
  
  // Something bad happened, but keep connection alive.
  // Todo distinguish between client and server generated errors.
  //TODO error codes. Maybe use a preprocessor to make these.
  void _error(int code, String msg) {
    _log('Error: $msg');
    var err = new PgError(code, msg);
    _errors.addLast(err);
    if (!_connectCompleter.future.isComplete)
      _connectCompleter.completeException(err);
    
    //FIXME fire event.
  }
  
  // Something really bad happened, close connection and report error.
  void _fatalError(int code, String msg) {
    _log('Fatal error: $msg');
    var err = new PgError(code, msg);
    _errors.addLast(err);
    
    if (!_connectCompleter.future.isComplete)
      _connectCompleter.completeException(err);
    
    //FIXME fire event.
    
    //TODO don't send close message.
    close();
  }
  
  List<PgError> popErrors() {
    var errors = _errors;
    _errors = new List<PgError>();
    return errors;
  }
  
  Future<Connection> _connect() {
    if (_state != _NOT_CONNECTED) {
      _fatalError(0, 'connect() called while in invalid state: $_state.');
      return _connectCompleter.future;      
    }
    
    try {
      _socket = new Socket(settings.host, settings.port);
    } catch (ex) {
      _fatalError(0, 'Socket error: $ex.');
      return _connectCompleter.future;
    }
    
    _socket
    ..onConnect = () {
      _changeState(_SOCKET_CONNECTED);
      _sendStartupMessage();
    }
    ..onError = (err) {
      _fatalError(0, 'Socket error: $err.');
      if (!_connectCompleter.future.isComplete)
        _connectCompleter.completeException(err);
    }
    ..onClosed = () {
      String err = 'Socket closed.';
      _fatalError(0, err);
      if (!_connectCompleter.future.isComplete)
        _connectCompleter.completeException(err);
    }
    ..onData = () {
      _readData();
    };
    
    _changeState(_SOCKET_CONNECTING);
    
    return _connectCompleter.future;
  }
  
  void _sendStartupMessage() {
    if (_state != _SOCKET_CONNECTED) {
      _fatalError(0, '_sendStartupMessage() called while in invalid state: $_state.');
      return;
    }
    
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
  
  void close() {
    
    //TODO Send close message.
    
    if (_state == _CLOSED)
      return;
    
    _state = _CLOSED;
    _socket.close();
  }
  
  Query query(String sql, {int timeoutMillis, Object resultType, ResultMapper resultMapper}) {
    
    //TODO
    if (timeoutMillis != null)
      throw new Exception('Query timeout not implemented.');
    
    //TODO
    if (resultType != null)
      throw new Exception('Result type mapping not implemented.');
    
    if (resultMapper != null && resultType != null)
      throw new Exception('Query() can take a resultReader, or a resultType, but not both.');
    
    if (resultMapper == null)
      resultMapper = new _DefaultResultMapper(); 
    
    var q = new _Query(sql, resultMapper, new _ResultReader(_reader));
    
    if (sql == null) {
      q.changeState(_FAILED);
      _error(0, 'Null sql string.');
      return q;
    }
    
    if (!_ok) {
      q.changeState(_FAILED);
      //TODO set error on query instead.
      _error(0, 'Attempted to send a query on a connection which is not in a ok state.');
      return q;
    }
            
    _sendQueryQueue.addLast(q);
    q.changeState(_QUEUED);
    _processSendQueryQueue();
    
    return q;
  }
  
  //TODO
  Future<int> exec(String sql) {
    throw new Exception('Not implemented.');
  }
  
  //TODO call this on query complete event.
  void _processSendQueryQueue() {
    
    if (!_ok)
      return;
    
    if (_sendQueryQueue.length == 0)
      return; // No more queries to process.
    
    if (_query != null)
      return; // Another query already being processed.
    
    var q = _sendQueryQueue.removeFirst();
    
    if (q.state != _QUEUED) {
      _error(0, 'Send query failed. Invalid query state: ${q.state}.');
      return;
    }
    
    q.changeState(_SENDING);
    
    _query = q;
   
    //TODO streamed writes for long strings.
    // At the moment _writer should just bomb if it doesn't fit in the output
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
      _fatalError(0, 'Received authentication request from server while in invalid state: $_state.');
      r.skipMessage();
      return;
    }
    
    int authType = r.readInt32();
    
    if (authType == _AUTH_TYPE_OK) {
      _changeState(_AUTHENTICATED);      
      return;
    }
    
    // Only MD5 authentication is supported.
    if (authType != _AUTH_TYPE_MD5 && authType != _AUTH_TYPE_OK) {
      _fatalError(0, 'Unsupported or unknown authentication type: ${_authTypeAsString(authType)}, only MD5 authentication is supported.');
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
    
    //TODO Check these states. 
    if (_state != _READY && _state != _AUTHENTICATED) {
      r.skipMessage();
      _fatalError(0, 'Received ReadyForQuery message from server while in invalid state: $_state.');
      return;
    } 
    
    int c = r.readByte();
    
    if (c == 'I'.charCodeAt(0)) {
      // Do nothing.
    } else if (c == 'T'.charCodeAt(0)) {
      _error(0, 'Transaction handling not implemented.');
    } else if (c == 'E'.charCodeAt(0)) {
      _error(0, 'Transaction handling not implemented.');
    } else {
      _fatalError(0, 'Unknown ReadyForQuery transaction status: ${_itoa(c)}');
    }
    
    var s = _state;
    var q = _query;
    
    _query = null;
    _changeState(_IDLE);
    
    if (s == _AUTHENTICATED) {
      if (!_connectCompleter.future.isComplete) {
        _connectCompleter.complete(this);
      }
    } else {
      q.onQueryComplete();
      _processSendQueryQueue();
    }
  }
  
  void _handleErrorResponse(_MessageReader r) {
    
    // Parse error message.
    // TODO handle long error messages, that don't fit into the buffer.
    int code = r.readByte();
    var list = new List<PgError>();
    while(code != 0) {
      var msg = r.readString();
      list.add(new PgError(code, msg));
      code = r.readByte();
      _log('Error ${_itoa(code)} $msg');
    }
    
    if (_state == _AUTHENTICATING) {
      _errors.addAll(list);
      _fatalError(0, 'Authentication failed.');
      return;
    }
    
    if (_state == _BUSY) {
      if (_query != null 
          && (_query.state == _SENT || _query.state == _RESULTS_READY)) {
        _query.changeState(_COMPLETE);
        _changeState(_READY);
        _query.onQueryError(list);
      } else {
        _errors.addAll(list);
        _changeState(_READY);
      }
      return;
    }
    
    // Errors aren't expected in this state - but worth logging them anyways if
    // they happen.
    //FIXME Perhaps these should be added as notices in this case??
    
    //TODO fire error event.
    _errors.addAll(list);
    
  }
  
  void _handleRowDescription(_MessageReader r) {
    
    if (_state != _BUSY) {
      _error(0, 'Received RowDescription message while not in busy state, state: $_state.');
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
      _error(0, 'Received CommandComplete message while not in ready state, state: $_state.');
      r.skipMessage();
      return;
    }
    
    //TODO store this information in _query.
    var commandTag = r.readString();    
    
    //FIXME
    //_query._rowProcessor.commandComplete(commandTag);
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
      _fatalError(0, 'Socket error where sending message: $ex');
    }
  }
    
  void _readData() {

    var r = _reader;
    
    // Check to see if the connection is in a valid reading state.
    if (!_ok && _state != _AUTHENTICATING && _state != _AUTHENTICATED)
      return;

    // Allow socket onData handler to finish after every 100 reads when reading a large amount of data.
    // also prevents state errors for causing infinite loops.
    SOCKET_READ: for(int i = 0; i < 100; i++) {
      
      if (r.bytesAvailable > 0)
        _log('Message fragment left in buffer - bytesAvailable: ${r.bytesAvailable}.');
      
      r.appendFromSocket(_socket);
      
      r._buffer._logState();
      
      // Wait for more data.
      if (r.bytesAvailable < 5)
        return;
      
      // Debugging
      //TODO
      //r._logBufferLayout();
      
      NEXT_MSG: for(;;) {

        // Check to see if the connection is in a valid reading state.
        if (!_ok && _state != _AUTHENTICATING && _state != _AUTHENTICATED)
          return;
        
        // Make sure there is enough data available to read the message header.
        if (r.bytesAvailable < 5)
          continue SOCKET_READ;
        
          
        //TODO
        //In authenticating and authenticated state only handle a subset of the message types.
        if (_state == _AUTHENTICATING) {
          //const authenticatingTypes = [sdfsdf, sdfsdfsdf, sdfsdfsdf];
          //if (!authenticatingTypes.contains(msgType)) {
          //  _fatalError(0, 'Unexpected message type while in authenticating state, type: ${_atoi(msgType)}.');
          //  return;
          //}
        } else if (_state == _AUTHENTICATED) {
          //const authenticatedTypes = [sdfsdf, sdfsdfsdf, sdfsdfsdf];
          //if (!authenticatedTypes.contains(msgType)) {
          //  _fatalError(0, 'Unexpected message type while in authenticated state, type: ${_atoi(msgType)}.');
          //  return;
          //}
        }

        // If it's a large message type, then we'll need to hand over to a
        // separate routine to handle it, as these can handle message fragments
        // and don't require the entire message to be read into the buffer.
        int mtype = r.peekByte();
        
        if (mtype == _MSG_DATA_ROW) { // or a partial message and state is query_reading.
          if (_state != _READY) {
            _error(0, 'Received DataRow message while not in ready state, state: $_state.');
            r.skipMessage();
            return;
          }
          
          if (r.bytesAvailable < 7)
            continue SOCKET_READ; // Read more data.

          _query.onDataRow();
          
          continue NEXT_MSG;
        }
        
        if (mtype == _MSG_ERROR_RESPONSE) {
          //TODO implement me. Read data without buffering it all.
          //continue NEXT_MSG;
        } else if (mtype == _MSG_NOTICE_RESPONSE) {
          //TODO implement me. Read data without buffering it all.
          //continue NEXT_MSG;
        }  
        
        // Currently handled with standard messages.
        // TODO need to allow large row description messages. Currently this
        // could lead to the buffer growing to a huge size.
        //if (mtype == _MSG_ROW_DESCRIPTION) {
        // continue NEXT_MSG;
        //} else
        
        else if (mtype == _MSG_FUNCTION_CALL_RESPONSE
                 || mtype == _MSG_NOTIFICATION_RESPONSE
                 || mtype == _MSG_COPY_DATA) {
          // Not implemented.
          //TODO skip data without buffering.
          //continue NEXT_MSG;
        }

        // Standard size message handlers. These messages must be less than 30k,
        // so we can buffer them safely.

        // Parse message header - advances 5 bytes.
        r.startMessage();
        
        //_log('Read message header, type: ${_itoa(r.messageType)}, code: ${r.messageType}, length: ${r.messageLength}, offset: ${r.messageStart}.');

        // Check for sane message size - need to prevent accidently reading a
        // massive number of bytes into our buffer.
        if (!_checkMessageLength(r.messageType, r.messageLength)) {
          _fatalError(0, 'Bad message length.'); //FIXME message.
          return;
        }
        
        // If only part of a message is left in buffer then read more data.
        // -5 as the header has already been read.
        if (r.bytesAvailable < r.messageLength + 1 - 5)
          continue SOCKET_READ;

        // Dispatch to message handler method.
        _handleMessage(r);
        
        if (_state == _CLOSED)
          return;
                
        // Note the length reported in the message header excludes the message
        // type byte, hence +1.
        if (r.messageBytesRead != r.messageLength + 1)
            _error(0, 'Message contents do not agree with length in message header. Message type: ${_itoa(r.messageType)}, bytes read: ${r.messageBytesRead}, message length: ${r.messageLength}.');
            
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
      case _MSG_ERROR_RESPONSE: _handleErrorResponse(r); break;
      case _MSG_COMMAND_COMPLETE: _handleCommandComplete(r); break;
      case _MSG_READY_FOR_QUERY: _handleReadyForQuery(r); break;
      case _MSG_ROW_DESCRIPTION: _handleRowDescription(r); break;
      
      case _MSG_BACKEND_KEY_DATA:
      case _MSG_PARAMETER_STATUS:
        //_log('Ignoring unimplemented message type: ${_itoa(t)} ${_messageName(t)}.');
        r.skipMessage();
        break;
        
      case _MSG_NOTICE_RESPONSE:
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
        _error(0, 'Unimplemented message type: ${_itoa(t)} ${_messageName(t)}.');
        r.skipMessage();
        break;
      default:
        _error(0, 'Unknown message type received: ${_itoa(t)} ${_messageName(t)}.');
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


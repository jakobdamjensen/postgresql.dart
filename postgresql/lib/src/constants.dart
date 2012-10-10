
const int SOCKET_READ_SIZE = 8192;
const int INPUT_BUFFER_SIZE = 8192 * 2;

const int SOCKET_WRITE_SIZE = 8192;
const int OUTPUT_BUFFER_SIZE = SOCKET_WRITE_SIZE;

const int _STATE_MSG_HEADER = 0;
const int _STATE_COL_HEADER = 1;
const int _STATE_COL_DATA = 2;


const _FAILED = const _QueryState('Failed'); // This state isn't really used.
const _CREATED = const _QueryState('Created');
const _QUEUED = const _QueryState('Queued');
const _SENDING = const _QueryState('Sending');
const _SENT = const _QueryState('Sent');
const _RESULTS_READY = const _QueryState('Results ready');
const _COMPLETE = const _QueryState('Complete'); // When an error happen a query is also complete.

class _QueryState {
  final String name;
  const _QueryState(this.name);
  String toString() => name;
}


// Connecting states
const _NOT_CONNECTED = const _ConnectionState('Not connected'); // CONNECTION_NEEDED
const _SOCKET_CONNECTING = const _ConnectionState('Socket connecting'); // CONNECTION_STARTING
const _SOCKET_CONNECTED = const _ConnectionState('Socket connected'); // CONNECTION_STARTED
const _AUTHENTICATING = const _ConnectionState('Authenticating'); // CONNECTION_AWAITING_RESPONSE
const _AUTHENTICATED = const _ConnectionState('Authenticated'); // CONNECTION_AUTH_OK

//TODO Do I need this too? static const _CLOSING = const _ConnectionState('Closing');

const _CLOSED = const _ConnectionState('Closed'); // CONNECTION_BAD

// Connected states
const _IDLE = const _ConnectionState('Idle'); // PGASYNC_IDLE

//FIXME Perhaps these are really just a duplication of the query states?
const _BUSY = const _ConnectionState('Busy'); // PGASYNC_BUSY //TODO rename to 'waiting for query'?
const _READY = const _ConnectionState('Ready'); // PGASYNC_READY //TODO rename 'has query result'?

//static const _STREAMING_ERROR_MSG = const _ConnectionState('Streaming error message');
//static const _STREAMING_ROW_DATA = const _ConnectionState('Streaming row data');



class _ConnectionState {
  final String name;
  const _ConnectionState(this.name);
  String toString() => name;
}

const int _PROTOCOL_VERSION = 196608;

const int _AUTH_TYPE_MD5 = 5;
const int _AUTH_TYPE_OK = 0;

// Messages sent by client (Frontend).
const int _MSG_STARTUP = -1; // Fake message type as StartupMessage has no type in the header.
const int _MSG_PASSWORD = 112; // 'p'
const int _MSG_QUERY = 81; // 'Q'
const int _MSG_TERMINATE = 88; // 'X'

// Message types sent by the server.
const int _MSG_AUTH_REQUEST = 82; //'R'.charCodeAt(0);
const int _MSG_ERROR_RESPONSE = 69; //'E'.charCodeAt(0);
const int _MSG_BACKEND_KEY_DATA = 75; //'K'.charCodeAt(0);
const int _MSG_PARAMETER_STATUS = 83; //'S'.charCodeAt(0);
const int _MSG_NOTICE_RESPONSE = 78; //'N'.charCodeAt(0);
const int _MSG_NOTIFICATION_RESPONSE = 65; //'A'.charCodeAt(0);
const int _MSG_BIND = 66; //'B'.charCodeAt(0);
const int _MSG_BIND_COMPLETE = 50; //'2'.charCodeAt(0);
const int _MSG_CLOSE_COMPLETE = 51; //'3'.charCodeAt(0);
const int _MSG_COMMAND_COMPLETE = 67; //'C'.charCodeAt(0);
const int _MSG_COPY_DATA = 100; //'d'.charCodeAt(0);
const int _MSG_COPY_DONE = 99; //'c'.charCodeAt(0);
const int _MSG_COPY_IN_RESPONSE = 71; //'G'.charCodeAt(0);
const int _MSG_COPY_OUT_RESPONSE = 72; //'H'.charCodeAt(0);
const int _MSG_COPY_BOTH_RESPONSE = 87; //'W'.charCodeAt(0);
const int _MSG_DATA_ROW = 68; //'D'.charCodeAt(0);
const int _MSG_EMPTY_QUERY_REPONSE = 73; //'I'.charCodeAt(0);
const int _MSG_FUNCTION_CALL_RESPONSE = 86; //'V'.charCodeAt(0);
const int _MSG_NO_DATA = 110; //'n'.charCodeAt(0);
const int _MSG_PARAMETER_DESCRIPTION = 116; //'t'.charCodeAt(0);
const int _MSG_PARSE_COMPLETE = 49; //'1'.charCodeAt(0);
const int _MSG_PORTAL_SUSPENDED = 115; //'s'.charCodeAt(0);
const int _MSG_READY_FOR_QUERY = 90; //'Z'.charCodeAt(0);
const int _MSG_ROW_DESCRIPTION = 84; //'T'.charCodeAt(0);


//TODO Yuck - enums please. Must be a better way of doing consts.
String _messageName(int msg) {
  switch (msg) {
    case _MSG_AUTH_REQUEST: return 'AuthenticationRequest';
    case _MSG_ERROR_RESPONSE: return 'ErrorResponse';
    case _MSG_BACKEND_KEY_DATA: return 'BackendKeyData';
    case _MSG_PARAMETER_STATUS: return 'ParameterStatus';
    case _MSG_NOTICE_RESPONSE: return 'NoticeResponse';
    case _MSG_NOTIFICATION_RESPONSE: return 'NotificationResponse';
    case _MSG_BIND: return 'Bind';
    case _MSG_BIND_COMPLETE: return 'BindComplete';
    case _MSG_CLOSE_COMPLETE: return 'CloseComplete'; 
    case _MSG_COMMAND_COMPLETE: return 'CommandComplete'; 
    case _MSG_COPY_DATA: return 'CopyData';
    case _MSG_COPY_DONE: return 'CopyDone';
    case _MSG_COPY_IN_RESPONSE: return 'CopyInResponse';
    case _MSG_COPY_OUT_RESPONSE: return 'CopyOutResponse';
    case _MSG_COPY_BOTH_RESPONSE: return 'CopyBothResponse';
    case _MSG_DATA_ROW: return 'DataRow';
    case _MSG_EMPTY_QUERY_REPONSE: return 'EmptyQueryResponse';
    case _MSG_FUNCTION_CALL_RESPONSE: return 'FunctionCallResponse';
    case _MSG_NO_DATA: return 'NoData';
    case _MSG_PARAMETER_DESCRIPTION: return 'ParameterDescription';
    case _MSG_PARSE_COMPLETE: return 'ParseComplete';
    case _MSG_PORTAL_SUSPENDED: return 'PortalSuspended';
    case _MSG_READY_FOR_QUERY: return 'ReadyForQuery';
    case _MSG_ROW_DESCRIPTION: return 'RowDescription';
    default:
      return 'Unknown message type: ${_itoa}.';
  }
}

String _itoa(int c) {
  try {
    return new String.fromCharCodes([c]);
  } catch (ex) {
    return 'Invalid';
  }
}

String _authTypeAsString(int authType) {
  const unknown = 'Unknown';
  const names = const <String> ['Authentication OK',
                                unknown,
                                'Kerberos v5',
                                'cleartext password',
                                unknown,
                                'MD5 password',
                                'SCM credentials',
                                'GSSAPI',
                                'GSSAPI or SSPI authentication data',
                                'SSPI'];
  var type = unknown;
  if (authType > 0 && authType < names.length)
    type = names[authType];
  return type;
}


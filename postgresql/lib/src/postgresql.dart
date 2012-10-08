
Settings defaultSettings = null;

abstract class Settings {
  
  factory Settings({String host: 'localhost',
                   int port: 5432,
                   String username,
                   String database,
                   String password}) =>
                       
                       new _Settings(
                           host,
                           port,
                           username,
                           database,
                           password);
  String get host;
  int get port;
  String get username;
  String get database;
  //Map<String,String> params;
  String get passwordHash;
}

Future<Connection> connect([Settings settings = null]) {
  return _connect(settings);
}

abstract class Connection {
  //TODO Future<int> exec(String sql);
  Query query(String sql);
  void close();
}

typedef void ReadResultCallback(ResultReader r, Streamer streamer);
typedef void ZeroCopyReadResultCallback(ZeroCopyResultReader r, Streamer streamer);

abstract class Query extends Stream<Dynamic> {
  
  //TODO void cancel();
  
  // Note this callback will be called each time the buffer is filled, so
  // don't store state in local variables.
  void readResult(ReadResultCallback callback);
  void readResultZeroCopy(ZeroCopyReadResultCallback callback);
}

//TODO remove impl.
class PgError {
  final int code;
  final String message;
  PgError(this.code, this.message);
  String toString() => 'Error $code: $message';
}

class ResultReaderEventType {
  final String name;
  const ResultReaderEventType(this.name);
  String toString() => name;
}

const START_ROW = const ResultReaderEventType('start-row');
const END_ROW = const ResultReaderEventType('end-row');
const START_COMMAND = const ResultReaderEventType('start-command');
const END_COMMAND = const ResultReaderEventType('end-command');
const COLUMN_DATA = const ResultReaderEventType('column-data');
const ERROR = const ResultReaderEventType('error');

// Usefull?
//const BUFFER_FULL = const ResultReaderEventType('buffer-full');
//const BUFFER_EMPTY = const ResultReaderEventType('buffer-empty');

// This is only returned when using zero copy.
const COLUMN_DATA_FRAGMENT = const ResultReaderEventType('column-data-fragment');

abstract class ResultReader {
  
  bool hasNext();  
  
  int get command;
  int get row;
  int get column;
  int get columnSizeInBytes;
  ColumnDesc get columnDesc;  
  ResultReaderEventType get event;
  Dynamic get error;
  
  // These can only be called when event == COLUMN_DATA
  Dynamic get value; // Encode based on default mapping.
  
  String asString();
  int asInt();
  bool asBool();
  List<int> asDecimal();
  List<int> asBytes();
  
  // This can only be called when event == END_COMMAND
  //TODO parse this and return meaning result.
  String get commandTag;
}

abstract class ZeroCopyResultReader implements ResultReader {
  // These can only be called when event == COLUMN_DATA_FRAGMENT
  Uint8List get fragment;
  int get fragmentOffset;
  int get fragmentLength;
  
  Uint8List get fragment2;
  int get fragment2Offset;
  int get fragment2Length;
  
  void readString(void callback(String s));
  
  InputStream asInputStream();
}

abstract class Stream<T> implements Future<Dynamic> {  
  void onReceive(void receiver(T value));
  Future<T> one();
  Future<List<T>> all();
  //TODO
  //Future<T> first();
  //Future<T> last();    
  //Stream<T> take(int n);
  //Stream<T> skip(int n);  
  //Stream<T> range(int start, int count);
  //Future<int> count();
  //Future<T> nth(int n);
}

abstract class Streamer<T> implements Completer<T> {
  factory Streamer() => new _Streamer();
  Stream<T> get stream;
  void send(T value);
  void sendAll(Collection<T> values);
}

//FIXME Something like this will probably appear in the standard library. When
// it does use it.
class Decimal {
  Decimal(this.unscaled, this.precision, this.scale);
  final int unscaled;
  final int precision;
  final int scale;
}

abstract class ColumnDesc {
  int get index;
  String get name;
  bool get binary;
  
  //TODO figure out what to name these.
  // Perhaps just use libpq names as they will be documented in existing code 
  // examples. It may not be neccesary to store all of this info.
  int get fieldId;
  int get tableColNo;
  int get fieldType;
  int get dataSize;
  int get typeModifier;
  int get formatCode;
}

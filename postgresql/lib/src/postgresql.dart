// Public interface.

Settings defaultSettings = null;

typedef void ErrorHandler(PgError error);

abstract class Settings {
  
  factory Settings({String host: 'localhost',
                   int port: 5432,
                   String username,
                   String database,
                   String password,
                   ErrorHandler onUnhandledErrorOrNotice}) =>
                       
                       new _Settings(
                           host,
                           port,
                           username,
                           database,
                           password,
                           onUnhandledErrorOrNotice);
  String get host;
  int get port;
  String get username;
  String get database;
  //TODO Map<String,String> params; 
  String get passwordHash;
  ErrorHandler get onUnhandledErrorOrNotice;
}

Future<Connection> connect([Settings settings = null]) {
  return _connect(settings);
}

abstract class Connection {
  //TODO Future<int> exec(String sql);
  //TODO ConnectionState get state;
  Query query(String sql, {int timeoutMillis, ResultMapper resultMapper});
  void close();
}

abstract class Query extends Stream<Dynamic> {
  //TODO QueryState get state;
  //TODO void cancel();  
}

abstract class ResultMapper {
  void onData(ResultReader r, Streamer streamer);
}

class PgErrorType {
  const PgErrorType(this.name);
  final String name;
  String toString() => name;
}

const CLIENT_ERROR = const PgErrorType('CLIENT_ERROR');
const SERVER_ERROR = const PgErrorType('SERVER_ERROR');
const SERVER_NOTICE = const PgErrorType('SERVER_NOTICE');

abstract class PgError {
  PgErrorType get type; //FATAL_CLIENT_ERROR, FATAL_SERVER_ERROR, SERVER_ERROR, SERVER_NOTICE
  String get severity; //FIXME Client errors, report 'ERROR' or 'FATAL??'. Note that notice severities, get translated to the local language.
  String get code; //SQLSTATE http://www.postgresql.org/docs/8.4/static/errcodes-appendix.html  (Client is supposed to issue these too.)
  String get message; // One line description.
  Map<String,String> get fields; // Fields contain all message fields, as sent by the server.
  String toDetailedMessage();
  String toString();
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
const COLUMN_DATA_FRAGMENT = const ResultReaderEventType('column-data-fragment');

abstract class ResultReader {

  bool hasNext();  
  
  ResultReaderEventType get event;
  
  int get command;
  int get row;
  int get column;
  int get columnSizeInBytes;
  ColumnDesc get columnDesc;
  List<ColumnDesc> get columnDescs;
  int get columnCount; // Number of columns in the row.
  
  // These can only be called when event == COLUMN_DATA
  Dynamic readDynamic(); // Encode based on default mapping.
  String readString(); //TODO use default string encoding.
  int readInt();
  bool readBool();
  Decimal readDecimal();
  List<int> readBytes();
  void readBytesInto(Uint8List buffer, int start);
  //TODO Date readDate(); dates and time.
  
  // This can only be called when event == END_COMMAND
  //TODO parse this and return meaning result.
  String get commandTag;
  
  // These can only be called when event == COLUMN_DATA_FRAGMENT
  // Or COLUMN_DATA too??
  int get fragmentSizeInBytes;
  
  // If the final fragment after a sequence of fragments have been received.
  bool get lastFragment;
  
  // Returns true if there is still more data fragments to read.
  bool readStringFragment(StringBuffer buffer);
  
  // Returns true if there are still more data fragments to read.
  bool readBytesFragment(Uint8List buffer, int start);
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
  int get fieldId; // If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
  int get tableColNo; // If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
  int get fieldType; // The object ID of the field's data type.
  int get dataSize; // The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
  int get typeModifier; // The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
  int get formatCode; // The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
}



class _Query implements Query {  
  
  _Query(this.sql, _MessageReader _msgReader)
    : _state = _CREATED,
      _resultReader = new _ResultReader(_msgReader);
  
  _QueryState _state;
  ReadResultCallback _resultCallback;
  final _ResultReader _resultReader;
  final List<PgError> _errors = new List<PgError>();
  final List<List<ColumnDesc>> _columnDescs = new List<List<ColumnDesc>>();
  
  final String sql;  
  
  _QueryState get state => _state;
  
  final Streamer<Dynamic> _streamer = new Streamer<Dynamic>();
  
  void changeState(_QueryState state) {
    //TODO use _log
    print('Query state change: $_state => $state.');
    _state = state;
  }
    
  void readResult(ReadResultCallback callback) {
    //TODO check for invalid query state. I.e. already submitted.
    _resultCallback = callback;
  }
  
  //void readResultZeroCopy(ReadResultZeroCopyCallback callback);
  
  void addColumnDescs(List<ColumnDesc> list) {
    //FIXME this needs a bit of a refactor.
    _columnDescs.add(list);
    _resultReader.columnDescs = list;
  }
  
  // Called from connection main processing loop to kick of the result
  // processing process. 
  void processDataRows() {
    if (_resultCallback == null)
      _resultCallback = _rowResultsReader;
    _resultCallback(_resultReader, _streamer);
  }
  
  void _rowResultsReader(_ResultReader r, Streamer streamer) {
    List<Dynamic> row;
    while (r.hasNext()) {
      if (r.event == ERROR) {
        print('Query error: ${r.error}');
        _errors.add(new PgError(0, r.error));
      
      } else if (r.event == START_ROW) {
        row = new List<Dynamic>();
      } else if (r.event == END_ROW) {
        streamer.send(row);
      } else if (r.event == COLUMN_DATA) {
        row.add(r.value);
      }
    }
  }
  
  // Delegate to stream impl.
  void onReceive(void receiver(Dynamic value)) => 
      _streamer.stream.onReceive(receiver);
  Future<Dynamic> one() => _streamer.stream.one(); 
  Future<List<Dynamic>> all() => _streamer.stream.all();
  Dynamic get value => _streamer.stream.value;
  Object get exception => _streamer.stream.exception;
  Object get stackTrace => _streamer.stream.stackTrace;
  bool get isComplete => _streamer.stream.isComplete;
  bool get hasValue => _streamer.stream.hasValue;
  void onComplete(void complete(Future<Dynamic> future)) => 
      _streamer.stream.onComplete(complete);
  void then(void onSuccess(Dynamic value)) => 
      _streamer.stream.then(onSuccess);
  void handleException(bool onException(Object exception)) => 
      _streamer.stream.handleException(onException);
  Future transform(transformation(Dynamic value)) => 
      _streamer.stream.transform(transformation);
  Future chain(Future transformation(Dynamic value)) => 
      _streamer.stream.chain(transformation);
  Future transformException(transformation(Object exception)) => 
      _streamer.stream.transformException(transformation);
}



class _Query implements Query {  
  
  _Query(this.sql, this._resultMapper, this._resultReader)
    : _state = _CREATED;
  
  _QueryState _state;
  
  final String sql;
  _QueryState get state => _state;
  final ResultMapper _resultMapper;
  final _ResultReader _resultReader;
  final Streamer<Dynamic> _streamer = new Streamer<Dynamic>();
  
  void _log(String msg) => print(msg);
  
  void changeState(_QueryState state) {
    if (state == _state)
      return;
    _log('Query state change: $_state => $state.');
    _state = state;
  }
  
  void readResult() {
    _resultMapper.onData(_resultReader, _streamer);
  }
  
  void complete() {
    changeState(_COMPLETE);
    if (!_streamer.future.isComplete)
      _streamer.complete(this);
  }
  
  void completeException(ex) {
    changeState(_COMPLETE);
    if (!_streamer.future.isComplete)
      _streamer.completeException(ex);
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


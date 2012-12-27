part of postgresql;



typedef void _ValueReceiver(value);

//TODO current caller can only can one of these methods. Perhaps throw
// an exception if the user, for example, attempts to call one() after all().
class _Stream<T> implements Stream<T> {

  Future _future; // Initialised by Streamer constructor.
  _ValueReceiver _receiver;

  void onReceive(void receiver(T value)) {
    _receiver = receiver;
  }

  Future<T> one() {
    bool hasResult = false;
    T value;
    onReceive((val) {
      if (hasResult) {
        throw new Exception('Expected only one result.');
      }
      hasResult = true;
      value = val;
    });
    return _future.transform((_) => value);
  }

  Future<List<T>> all() {
    var list = new List<T>();
    onReceive((val) {
      list.add(val);
    });
    return _future.transform((_) => list);
  }

  // Delegate to future implementation.
  //factory Future.immediate(T value) => new FutureImpl<T>.immediate(value);
  T get value => _future.value;
  Object get exception => _future.exception;
  Object get stackTrace => _future.stackTrace;
  bool get isComplete => _future.isComplete;
  bool get hasValue => _future.hasValue;
  void onComplete(void complete(Future<T> future)) => _future.onComplete(complete);
  void then(void onSuccess(T value)) => _future.then(onSuccess);
  void handleException(bool onException(Object exception)) => _future.handleException(onException);
  Future transform(transformation(T value)) => _future.transform(transformation);
  Future chain(Future transformation(T value)) => _future.chain(transformation);
  Future transformException(transformation(Object exception)) => _future.transformException(transformation);

}

class _Streamer<T> implements Streamer<T> {

  _Streamer()
    : _completer = new Completer<dynamic>(),
      stream = new _Stream<T>() {

    stream._future = _completer.future;
  }

  final Completer<dynamic> _completer;
  final _Stream<T> stream;

  void send(T value) {
    if (stream._receiver != null) {
      stream._receiver(value);
    }
  }

  void sendAll(Collection<T> values) {
    for (var v in values) {
      send(v);
    }
  }

  // Delegate to Completer implementation.
  Future get future => _completer.future;
  void complete(T value) => _completer.complete(value);
  void completeException(Object exception, [Object stackTrace]) =>
      _completer.completeException(exception, stackTrace);
}


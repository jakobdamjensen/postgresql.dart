
class _Row {
  _Row(this._columnNames, this._values) {
    //assert(this._columnNames.length == this._values.length);
  }
  
  final List<String> _columnNames;
  final List<Dynamic> _values;
  
  operator[] (int i) => _values[i];
  
  noSuchMethod(String name, _) {
    
    //FIXME only allowed this for debugging.
    if (_columnNames == null)
      return;
    
    if (name.startsWith('get:')) {
      var colName = name.substring(4, name.length);
      var i = _columnNames.indexOf(colName);
      if (i != -1)
        return _values[i];
      else
        //FIXME throw NoSuchMethodError
        throw new Exception('Unknown column name: $colName.');
    } else {
      //FIXME throw NoSuchMethodError 
    }
  }
  
  String toString() => _values.toString();
}

class _DefaultResultMapper implements ResultMapper {
  
  _Row _row;
  List<String> _columnNames;
  List<Dynamic> _values;
  
  void onData(ResultReader r, Streamer streamer) {
    
    while (r.hasNext()) {
      if (r.event == ERROR) {
        if (!streamer.future.isComplete)
          streamer.completeException(r.error); //TODO Only first error is sent. Check that this matches protocol description.
      
      } else if (r.event == START_ROW) {
        //FIXME temporary until I implement start command.
        if (_columnNames == null)
          _columnNames = r.columnDescs.map((c) => c.name);
        
        _values = new List<Dynamic>(r.columnCount);
      } else if (r.event == END_ROW) {
        var row = new _Row(_columnNames, _values);
        streamer.send(row);
      } else if (r.event == COLUMN_DATA) {
        _values[r.column] = r.value;
      } else if (r.event == START_COMMAND) {
        //FIXME start command is not called yet.
        _columnNames = r.columnDescs.map((c) => c.name);
      } else if (r.event == END_COMMAND) {
        //Do nothing
      } else {
        assert(false);
      }
    }
  }
}

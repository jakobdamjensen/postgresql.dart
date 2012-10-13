
class _DynamicRow {
  _DynamicRow(this._columnNames, this._values) {
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

class _DynamicRowMapper implements Mapper {
  
  _DynamicRow _row;
  List<String> _columnNames;
  List<Dynamic> _values;
  
  StringBuffer _stringBuffer;
  Uint8List _colBuffer;
  int _fragmentStart;
  
  void onData(ResultReader r, Streamer streamer) {
    
    while (r.hasNext()) {
      if (r.event == START_COMMAND) {
        _columnNames = r.columnDescs.map((c) => c.name);
        
        // Debugging
        // r.columnDescs.forEach((cd) => print(cd));
        
      } else if (r.event == END_COMMAND) {
        //Do nothing
        
      } else if (r.event == START_ROW) {
        _values = new List<Dynamic>(r.columnCount);
        
      } else if (r.event == END_ROW) {
        var row = new _DynamicRow(_columnNames, _values);
        streamer.send(row);
        
      } else if (r.event == COLUMN_DATA) {
        _values[r.column] = r.readDynamic();
        
      } else if (r.event == COLUMN_DATA_FRAGMENT) {
        
        // Handle values which are too large to fit in the buffer.
        // Only String and binary types are handled. Everything else should
        // always fit in the buffer.
        //if (r.columnDesc.fieldType == string???) {
//          if (_stringBuffer == null)
//            _stringBuffer = new StringBuffer();
//          
//          r.readStringFragment(_stringBuffer);
//          
//          if (r.lastFragment) {
//            _values[r.column] = _stringBuffer.toString();
//            _stringBuffer = null;
//          }
        //} else {
          if (_colBuffer == null) {
            _colBuffer = new Uint8List(r.columnSizeInBytes);
            _fragmentStart = 0;
          }
        
          r.readBytesFragment(_colBuffer, _fragmentStart);
          _fragmentStart += r.fragmentSizeInBytes;
          
          if (r.lastFragment) {
            _values[r.column] = _colBuffer;
            _colBuffer = null;
          }
        //}
        
      } else {
        assert(false);
      }
    }
  }
}

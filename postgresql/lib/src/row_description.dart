
// Parses a RowDescription message split accross multiple reads

//TODO remove this it's not neccesary - or tested...
//class _RowDescriptionStateMachine {
 
//  _RowDescriptionStateMachine(this._reader);
//  
//  static const int _HEADER = 0;
//  static const int _NAME = 1;
//  static const int _REST = 2;
//  static const int _DONE = 3;
//  static const int _ERROR = 4;
//  
//  int _state = _HEADER;
//  int _columns;
//  int _i;
//  String _name;
//  
//  final _InputBuffer _reader;
//  final List<_ColumnDesc> _list = new List<_ColumnDesc>();
//  
//  // Keeps on returning null until it returns a list of _ColumnDesc.
//  List<_ColumnDesc> read() {
//    var r = _reader;
//    
//    for (;;) {
//      switch (_state) {
//        case _HEADER:
//          if (_reader.bytesAvailable < 7)
//            return null;
//          
//          _reader.skipHeader();
//          int cols = _reader.readInt16();
//          assert(cols >= 0);
//          
//          _i = 0;
//          _columns = cols;
//          _name = null;
//          
//          _state = _NAME;
//          break;
//          
//        case _NAME:
//          // Upper limit on name size is the length of the buffer.
//          _name = _reader.readString();
//          _state = _REST;
//          break;
//          
//        case _REST:
//          
//          if (r.bytesAvailable < 18)
//            return null;
//          
//          int fieldId = r.readInt32();
//          int tableColNo = r.readInt16();
//          int fieldType = r.readInt32();
//          int dataSize = r.readInt16();
//          int typeModifier = r.readInt32();
//          int formatCode = r.readInt16();
//          
//          _list.add(new _ColumnDesc(
//              _i,
//              _name,
//              fieldId,
//              tableColNo,
//              fieldType,
//              dataSize,
//              typeModifier,
//              formatCode));
//          
//          _i++;
//          if (_i >= _columns)
//            _state = _DONE;
//          else
//            _state = _NAME;
//          break;
//          
//        case _DONE:
//          return _list;
//          
//        default:
//          assert(false);
//      }
//    }
//  }
//}
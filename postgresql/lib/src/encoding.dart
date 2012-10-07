

//TODO
Dynamic decodeValue(_ColumnDesc col, List<int> buffer, int start, int length) {
  return decodeString(col, buffer, start, length);
}

//TODO
String decodeString(_ColumnDesc col, List<int> buffer, int start, int length) {
  if (start == 0 && buffer.length == length)
    return new String.fromCharCodes(buffer);
  else
    throw new Exception('Not implemented');
}

//TODO
int decodeInt(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}

//TODO
bool decodeBool(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}

//TODO
Decimal decodeDecimal(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}

//TODO
List<int> decodeBytes(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}
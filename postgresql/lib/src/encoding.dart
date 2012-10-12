
// Encode as a big endian two's complement 32 bit integer
void _encodeInt32(int i, Uint8List buffer, int offset) {
  assert(i >= -2147483648 && i <= 2147483647);
  
  if (i < 0)
    i = 0x100000000 + i;
  
  buffer[offset] = (i >> 24) & 0x000000FF;
  buffer[offset + 1] = (i >> 16) & 0x000000FF;
  buffer[offset + 2] = (i >> 8) & 0x000000FF;
  buffer[offset + 3] = i & 0x000000FF;
}

// Encode as a big endian two's complement 16 bit integer
void _encodeInt16(int i, Uint8List buffer, int offset) {
  assert(i >= -32768 && i <= 32767);
  
  if (i < 0)
    i = 0x10000 + i;
  
  buffer[offset] = (i >> 8) & 0x00FF;
  buffer[offset + 1] = i & 0x00FF;
}

// Big endian two's complement 16 bit integer.
int _decodeInt16(int a, int b) {
  assert(a < 256 && b < 256 && a >= 0 && b >= 0);
  int i = (a << 8) | (b << 0); 
  
  if (i >= 0x8000)
    i = -0x10000 + i;
  
  return i;
}

// Big endian two's complement 32 bit integer.
int _decodeInt32(int a, int b, int c, int d) {
  assert(a < 256 && b < 256 && c < 256 && d < 256 && a >= 0 && b >= 0 && c >= 0 && d >= 0);
  int i = (a << 24) | (b << 16) | (c << 8) | (d << 0);
  
  if (i >= 0x80000000)
    i = -0x100000000 + i;
  
  return i;
}


//TODO
Dynamic _decodeValue(_ColumnDesc col, List<int> buffer, int start, int length) {
  return _decodeString(col, buffer, start, length);
}

//TODO
String _decodeString(_ColumnDesc col, List<int> buffer, int start, int length) {
  if (start == 0 && buffer.length == length)
    return new String.fromCharCodes(buffer);
  else
    throw new Exception('Not implemented');
}

//TODO
int _decodeInt(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}

//TODO
bool _decodeBool(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}

//TODO
Decimal _decodeDecimal(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}

//TODO
List<int> _decodeBytes(_ColumnDesc col, List<int> buffer, int start, int length) {
  throw new Exception('Not implemented.');
}

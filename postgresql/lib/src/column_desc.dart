part of postgresql;

class _ColumnDesc implements ColumnDesc {
  final int index;
  final String name;

  //TODO figure out what to name these.
  // Perhaps just use libpq names as they will be documented in existing code
  // examples. It may not be neccesary to store all of this info.
  final int fieldId;
  final int tableColNo;
  final int fieldType;
  final int dataSize;
  final int typeModifier;
  final int formatCode;

  bool get binary => formatCode == 1;

  _ColumnDesc(this.index, this.name, this.fieldId, this.tableColNo, this.fieldType, this.dataSize, this.typeModifier, this.formatCode);

  String toString() => 'ColumnDesc: index: $index, name: $name, fieldId: $fieldId, tableColNo: $tableColNo, fieldType: $fieldType, dataSize: $dataSize, typeModifier: $typeModifier, formatCode: $formatCode.';
}

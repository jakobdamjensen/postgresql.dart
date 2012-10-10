#library('postgresql');

#import('dart:crypto');
#import('dart:io');
#import('dart:isolate');
#import('dart:math');
#import('dart:scalarlist');

#source('src/postgresql.dart');

#source('src/circular_block_buffer.dart');
#source('src/column_desc.dart');
#source('src/connection.dart');
#source('src/constants.dart');
#source('src/encoding.dart');
#source('src/message_reader.dart');
#source('src/message_writer.dart');
#source('src/pg_error.dart');
#source('src/query.dart');
#source('src/row_description.dart');
#source('src/result_mapper.dart');
#source('src/result_reader.dart');
#source('src/settings.dart');
#source('src/simple_buffer.dart');
#source('src/stream.dart');

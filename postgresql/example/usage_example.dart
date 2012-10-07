#import('dart:io');
#import('package:postgresql/postgresql.dart');

void mainOld() {
  testBuffer();
}

void main() {
  var s = new Settings(host: 'localhost', port: 5432, username: 'testdb', database: 'testdb', password: 'password');
  connect(s)
  ..then((c) {
    print('connected...');
    runExampleQueries(c);
  })
  ..handleException((e) {
    print(e);
    return true;
  });
}


void runExampleQueries(Connection c) {
  var sql = 'select 1 as one, \'2\' as two, 3.1 as three;';
  
  c.query(sql).one()
    ..then((result) {
      print(result);
    })
    ..handleException((err) {
      print(err);
    });
}  

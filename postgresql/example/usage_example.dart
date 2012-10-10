#import('dart:io');
//TODO #import('package:postgresql/postgresql.dart');
#import('../lib/postgresql.dart', prefix: 'pg');

void main() {
  var s = new pg.Settings(host: 'localhost', port: 5432, username: 'testdb', database: 'testdb', password: 'password');
  pg.connect(s)
  ..then((c) {
    print('connected...');
    runExampleQuery(c);
  })
  ..handleException((e) {
    print('Exception: $e');
    return true;
  });
}

void runExampleQuery(pg.Connection c) {
  var sql = 'select 1 as one, \'2\' as two, 3.1 as three;';
  
  c.query(sql).one()
    ..then((result) {
      print(result.one);
      print(result.two);
      print(result.three);
    })
    ..handleException((err) {
      print(err);
      return true;
    });
}  

void runExampleQueryBad(pg.Connection c) {
  //var sql = 'select 1 as one, \'2\' as two, 3.1 as three;';
  var sql = 'dsfsdfdsf';
  
  c.query(sql).one()
    ..then((result) {
      print(result);
    })
    ..handleException((err) {
      print(err);
      return true;
    });
}

void runExampleQueries(pg.Connection c) {
  
  var q1 = c.query('select 1 as one, \'2\' as two, 3.1 as three').one();
  var q2 = c.query('select 1 as one, \'2\' as two, 3.1 as three').one();
  var q3 = c.query('select 1 as one, \'2\' as two, 3.1 as three').one();
  
  Futures.wait([q1, q2, q3])
  ..then((result) {
    print(result);
    c.close();
  })
  ..handleException((err) {
    print(err);
    return true;
  });
  
}
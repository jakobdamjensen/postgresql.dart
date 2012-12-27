part of postgresql;


class _PgError implements PgError {

    final PgErrorType type;
    final String severity; // Client errors, report 'ERROR' or 'FATAL??'. Note that notice severities, get translated to the local language.
    final String code; //SQLSTATE http://www.postgresql.org/docs/8.4/static/errcodes-appendix.html  (Client is supposed to issue these too.)
    final String message; // One line description.
    final Map<String,String> fields; // Fields contain all message fields, as sent by the server.

    //_PgError.client(String message, [Dynamic exception])
    _PgError.client(String message)
      : type = CLIENT_ERROR,
        severity = 'ERROR', //FIXME what should this be?
        code = '', //FIXME figure out codes for client errors.
        message = '$message', // $exception',
        fields = null;

    _PgError.error(Map<String,String> fields)
      : type = SERVER_ERROR,
        severity = fields.containsKey('S') ? fields['S'] : '',
        code = fields.containsKey('C') ? fields['C'] : '',
        message = fields.containsKey('M') ? fields['M'] : '',
        fields = fields;

    _PgError.notice(Map<String,String> fields)
        : type = SERVER_NOTICE,
        severity = fields.containsKey('S') ? fields['S'] : '',
        code = fields.containsKey('C') ? fields['C'] : '',
        message = fields.containsKey('M') ? fields['M'] : '',
        fields = fields;

    String toString() => '$severity $code $message';

    String toDetailedMessage() {
      var sb = new StringBuffer();
      sb.add(toString());
      if (fields != null) {
        for(var val in fields.values) {
          sb.add(val);
      }
        }
    }
}


class _Settings implements Settings {
  _Settings(this.host,
      this.port,
      String username,
      this.database,
      //TODO this._params,
      String password,
      this.onUnhandledErrorOrNotice)
    : username = username,
      passwordHash = _md5s(password.concat(username));
  
  final String host;
  final int port;
  final String username;
  final String database;
  //TODO final Map<String,String> _params;
  final String passwordHash;
  final ErrorHandler onUnhandledErrorOrNotice;
}
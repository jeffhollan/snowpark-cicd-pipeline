class SnowflakeConnection(object):

    _connection = None

    @property
    def connection(self):
        return type(self)._connection

    @connection.setter
    def connection(self, val):
        type(self)._connection = val

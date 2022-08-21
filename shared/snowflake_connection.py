# Class to store a singleton connection option
# Used to pass the connection from a stored procedure to a notebook script

class SnowflakeConnection(object):

    _connection = None

    @property
    def connection(self):
        return type(self)._connection

    @connection.setter
    def connection(self, val):
        type(self)._connection = val

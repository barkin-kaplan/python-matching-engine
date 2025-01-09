from typing import Dict

from helper.db_connections.mongo_db_connector import MongoDbConnector


class MongoDbFactory:
    def __init__(self):
        # connection_string + db_name -> sql_alchemy_models connector
        self._dbs: Dict[str, MongoDbConnector] = {}

    def get_db_connector(self, connection_string: str, db_name: str):
        key = connection_string + db_name
        if key not in self._dbs:
            self._dbs[key] = MongoDbConnector(connection_string, db_name)

        return self._dbs[key]

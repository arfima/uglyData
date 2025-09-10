""" These classes contain methods for executing data cleaning and validation queries 
in the database. The idea is to have them as attributes of the DB and AsyncDB classes 
and access them like: db.cleaning.remove_na() or db.checker.check_na()
"""


class SQLCleaning:
    def __init__(self, db):
        self.db = db

    def remove_na(self):
        ...


class SQLChecker:
    def __init__(self, db):
        self.db = db

    def check_na(self):
        ...

#!/usr/bin/env python

import os
import re
import sqlite3
import sys
import unittest

from squeak import Squeak

class TestSqueak(unittest.TestCase):
    
    db = 'testdb'

    def setUp(self):
        
        # clear previous test run
        try:
            os.remove('testdb')
        except OSError: pass
        
        connection = sqlite3.connect(self.db)
        cursor = connection.cursor()
        
        create_my_table = """
CREATE TABLE "my_table" (
    "id" integer NOT NULL PRIMARY KEY,
    "name" varchar (20) NOT NULL DEFAULT ""
)
;"""
    
        create_my_seat = """
CREATE TABLE "my_seat" (
    "id" integer NOT NULL PRIMARY KEY,
    "table" integer NOT NULL REFERENCES "my_table" ("id"),
    "type" varchar(10) NOT NULL DEFAULT ""
)
;"""
    
        populate_my_table_1 = """
INSERT INTO my_table (id, name) VALUES (1, 'abc')
;"""
    
        populate_my_table_2 = """
INSERT INTO my_table (id, name) VALUES (2, 'def')
;"""

        populate_my_seat_1 = """
INSERT INTO my_seat VALUES (1, 1, 'tall')
;"""

        populate_my_seat_2 = """
INSERT INTO my_seat VALUES (2, 1, 'short')
;"""

        cursor.execute(create_my_table)
        cursor.execute(create_my_seat)
        cursor.execute(populate_my_table_1)
        cursor.execute(populate_my_table_2)
        cursor.execute(populate_my_seat_1)
        cursor.execute(populate_my_seat_2)
        
        connection.commit()
        cursor.close()
    
    def tearDown(self):
        try:
            os.remove('testdb')
        except OSError: pass
    
    # -- Drop tests --
    
    def test_drop_regular_column(self):
        return self._drop_column('my_table', 'name')

    def test_drop_foreign_key_column(self):
        return self._drop_column('my_seat', 'table')
    
    def _drop_column(self, table, column):
        cursor = sqlite3.connect(self.db).cursor()
        start = len(cursor.execute("SELECT * from %s" % table).fetchone())
        cursor.close()

        squeak = Squeak(self.db, table)
        squeak.drop_column(column)

        cursor = sqlite3.connect(self.db).cursor()
        end = len(cursor.execute("SELECT * from %s" % table).fetchone())
        cursor.close()
        
        return self.assert_(start == end + 1)    
    
    # -- Rename tests --
    
    def test_rename_regular_column(self):
        return self._rename_column('my_table', 'name')
        
    def test_rename_foreign_key_column(self):
        return self._rename_column('my_seat', 'table')
    
    def _rename_column(self, table, column):
        cursor = sqlite3.connect(self.db).cursor()
        start = len(cursor.execute("SELECT * from %s" % table).fetchone())
        cursor.close()

        squeak = Squeak(self.db, table)
        squeak.rename_column(column, '%s_renamed' % column)

        was_renamed = False
        for field in squeak.fields:
            if re.match('["\']%s_renamed' % column, field):
                was_renamed = True
                break

        cursor = sqlite3.connect(self.db).cursor()
        end = len(cursor.execute("SELECT * from %s" % table).fetchone())
        cursor.close()
        
        return self.assert_(start == end and was_renamed)
    
    # -- Replace definition tests --
    
    def test_replace_regular_column_definition(self):
        return self._replace_definition('my_table',
                                        'name',
                                        'varchar (20)')
    
    def test_replace_foreign_key_column_definition(self):
        return self._replace_definition('my_seat',
                                        'table',
                                        'integer')
    
    
    def _replace_definition(self, table, column, new_definition):
        cursor = sqlite3.connect(self.db).cursor()
        start = len(cursor.execute("SELECT * from %s" % table).fetchone())
        cursor.close()

        squeak = Squeak(self.db, table)
        squeak.replace_definition(column, new_definition)

        was_replaced = False
        for field in squeak.fields:
            m = re.match(r'^["\']?(%s)["\']?\s(.*)' % column, field)
            if m and m.group(1) == column and m.group(2) == new_definition:
                was_replaced = True
                break

        cursor = sqlite3.connect(self.db).cursor()
        end = len(cursor.execute("SELECT * from %s" % table).fetchone())
        cursor.close()
        
        return self.assert_(start == end and was_replaced)
        
        
if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSqueak)
    unittest.TextTestRunner(verbosity=2).run(suite)

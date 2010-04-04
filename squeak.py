#!/usr/bin/env python

"""
Copyright (c) 2010 Thibaud Morel l'Horset <teebes@teebes.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

----

Python helper for SQLite3. Allows to drop & alter columns.

Sample usage for a SQLite3 database 'db' with the following table:

CREATE TABLE "my_table" (
    "id" integer NOT NULL PRIMARY KEY,
    "name" varchar (20) NOT NULL DEFAULT ""
);

Examples from the command line

To drop the 'name' column:
$ squeak.py db my_table drop_column name

To rename the 'name' column to 'first_name':
$ squeak.py db my_table rename_column name first_name

To allow the 'name' column to be null:
$ squeak.py db my_table replace_definition name varchar \(20\)

Same examples from the python shell

To drop the 'name' column:
>>> from squeak import Squeak
>>> s = Squeak('db', 'my_table')
>>> s.drop_column('name')

To rename the 'name' column to 'first_name':
>>> from squeak import Squeak
>>> s = Squeak('db', 'my_table')
>>> s.drop_column('name')

To allow the 'name' column to be null:
>>> from squeak import Squeak
>>> s = Squeak('db', 'my_table')
>>> s.replace_definition('name', 'varchar (20)')
"""

import re
import sqlite3
import sys

class SqueakError(Exception): pass

def get_fields_from_sql(creation_sql):
    """
    Takes a sqlite3 creation sql and returns a list containing each field
    as an element.
    """
    
    inline_creation_sql = ' '.join(creation_sql.split('\n'))

    # get the part of the query string that deals with fields
    fields = re.findall(
        r'create\s+table\s+["\']?\w+["\']?\s+\((?P<fields>.*)\)[\s\n;]*$',
        inline_creation_sql, re.IGNORECASE
    )[0]
    
    # Fields in Sqlite3 are seperated by commas, but commas can also exist
    # within parenthesis as per http://www.sqlite.org/lang_createtable.html
    # So we temporarily replace all contents in parenthesis with a string that
    # doesn't contain a comma and therefore won't be split, and then we
    # re-insert the original parenthesis contents
    
    # identify expressions that are in parenthesis
    matches = re.findall('\([^\)]+\)', fields)

    # replace the parenthesis expressions in the fields string
    for match in matches:
        # escape the parenthesis in each match so that the fields re sub
        # correctly replaces
        match = re.sub('\(', '\(', match)
        match = re.sub('\)', '\)', match)
        
        # replace the parenthesis
        fields = re.sub(match, '#__#', fields)

    # now split on ','
    fields = fields.split(',')
    
    # cleanup beginning and trailing spaces
    fields = [re.sub('(^\s+|\s+$)', '', field) for field in fields]

    # restore the parenthesis expressions
    processed_fields = []
    for field in fields:
        if re.search('#__#', field):
            field = re.sub('#__#', matches.pop(0), field)
        
        processed_fields.append(field)
    
    return processed_fields

class Squeak(object):

    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        self.connection = sqlite3.connect(self.db)
        
        cursor = self.connection.cursor()
        try:
            self.creation_sql = cursor.execute(
                "select sql from sqlite_master where tbl_name = ?;",
                (table_name,)
            ).fetchone()[0]
        except (IndexError, TypeError):
            raise SqueakError("No such table: '%s'" % table_name)
        
        
        self.fields = get_fields_from_sql(self.creation_sql)
        cursor.close()

    def _create_table(self, new_table_name, fields):
        cursor = self.connection.cursor()
        creation_sql = ('CREATE TABLE "%s" (\n    %s\n);'
                        % (new_table_name, ',\n    '.join(fields)))
        cursor.execute(creation_sql)
        self.connection.commit()
        cursor.close()
        
    def _cleanup_tables(self, safe):
        cursor = self.connection.cursor()
        if safe:
            # rename the initial table but keep it around
            cursor.execute("ALTER TABLE %s RENAME TO %s_initial;" % (
                                                            self.table_name,
                                                            self.table_name))
        else:
            # drop the initial table
            cursor.execute("DROP TABLE %s;" % self.table_name)

        # rename the temp table to the initial table
        cursor.execute("ALTER TABLE %s_tmp RENAME TO  %s;" % (self.table_name,
                                                              self.table_name))
        self.connection.commit()
        cursor.close()

    def drop_column(self, column_name, safe=False):
        # filter out the column that is no longer wanted and save the names
        # of the columns that will be copied
        columns = []
        new_fields = []
        col_re = r'["\']?(?P<column>\w+)["\']?'
        for field in self.fields:
            column = re.match(col_re, field, re.IGNORECASE).group('column')
            if column == column_name:
                continue
            else:
                columns.append(column)
                new_fields.append(field)
        
        # provided column doesn't exist
        if len(new_fields) == len(self.fields):
            return False, u"No such column: '%s'" % column_name
        
        self._create_table(self.table_name + '_tmp', new_fields)

        cursor = self.connection.cursor()

        # copy the filtered data to the temp table
        columns = map(lambda x: '"%s"' % x, columns)
        columns_arg = (', ').join(columns)
        cursor.execute("INSERT INTO %s_tmp (%s) "
                       "SELECT %s FROM %s;" % (self.table_name,
                                              columns_arg,
                                              columns_arg,
                                              self.table_name))
        self.connection.commit()
        cursor.close()

        self._cleanup_tables(safe)
        self.fields = new_fields

        return True, u"Column '%s' dropped." % column_name

    def rename_column(self, old_column, new_column, safe=False):
        fields = []
        found = False
        for field in self.fields:
            m = re.match(r'^["\']?(?P<col_name>\w+)["\']?', field)
            if old_column == m.group('col_name'):
                found = True
                field = re.sub(r'^["\']?%s["\']?' % old_column,
                               '"%s"' % new_column,
                               field)
            fields.append(field)
        
        if not found:
            return False, u"No such column: '%s'" % old_column

        # create the temporary table
        self._create_table("%s_tmp" % self.table_name, fields)
        
        # populate the temporary table
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO %s_tmp SELECT * FROM %s"
                       % (self.table_name, self.table_name))
        self.connection.commit()
        cursor.close()
        
        self._cleanup_tables(safe)
        
        self.fields = fields
        
        return True, u"Column '%s' renamed to '%s'." % (old_column, new_column)
    
    def replace_definition(self, column_name, new_definition, safe=False):
        """
        allows to replace the definition of a column so that a user can
        do things like add/drop constraints
        """
        # get a formatted list of fields with the new definition
        # replacing the designated column
        column_parsing = re.compile(r'["\']?(\w+)["\']?\s(.*)')
        found_column = False
        fields = []
        for field in self.fields:
            column, definition = column_parsing.findall(field)[0]
            if column == column_name:
                found_column = True
                definition = new_definition
            if definition[-1] == ',':
                definition = definition[:-1]
            fields.append('"%s" %s' % (column, definition))

        if not found_column:
            return False, u"No such column: %s" % column_name 

        # create the temp table
        self._create_table("%s_tmp" % self.table_name, fields)

        try:        
            # migrate the data over
            cursor = self.connection.cursor()
            cursor.execute("INSERT into %s_tmp SELECT * FROM %s"
                           % (self.table_name, self.table_name))
            cursor.close()

        except sqlite3.IntegrityError:
            msg = ("The column definition you have supplied is raising "
                   "an integrity error when Sqlite3 is copying the data "
                   "from the original table to the "
                   "new table "
                   "(for example if you're adding a NOT NULL constraint "
                   "on a column that already has null values). "
                   "Please check your definition and/or the existing data "
                   "in the original table.")
            cursor = self.connection.cursor()
            cursor.execute("DROP TABLE %s_tmp" % self.table_name)
            cursor.close()
            return False, msg

        self.connection.commit()
        
        self._cleanup_tables(safe)
        
        self.fields = fields
        
        return True, "Changed the definition for column %s to: %s" \
                        % (column_name, new_definition)

def main():
    def print_usage():
        print ("Usage: squeak.py <db> <table_name> subcommand\n\n"
               "Available subcommands:\n"
               "  drop_column <column_name> [safe]\n"
               "  rename_column <old_column_name> <new_column_name> [safe]\n"
               "  replace_definition <column_name> <new_definition> [safe]\n")
               #"  change_constraints <column_name> <new_constraints> [safe]")

    if len(sys.argv) <= 4:
        print_usage()
        return
    
    try:
        squeak = Squeak(sys.argv[1], sys.argv[2])
    except SqueakError, e:
        print "Error: %s" % e
        return
    
    subcommand = sys.argv[3]
    
    if subcommand == 'drop_column':
        if sys.argv[-1] == 'safe':
            safe = True
        else:
            safe = False
        print squeak.drop_column(sys.argv[4], safe=safe)[1]
        return

    elif subcommand == 'rename_column':
        if len(sys.argv) < 6:
            print_usage()
            return
        if sys.argv[-1] == 'safe':
            safe = True
        else:
            safe = False
        print squeak.rename_column(sys.argv[4], sys.argv[5], safe)[1]
        return

    elif subcommand == 'replace_definition':
        if len(sys.argv) < 6:
            print_usage()
            return
        if sys.argv[-1] == 'safe':
            safe = True
            definition = ' '.join(sys.argv[5:-1])
        else:
            safe = False
            definition = ' '.join(sys.argv[5:])
        print squeak.replace_definition(sys.argv[4], definition, safe)[1]
        return

    else:
        print "Invalid subcommand: %s\n" % subcommand
        print_usage()
        return

if __name__ == '__main__':
    main()

#!/bin/python3.6
#
# DummySQL (ds)
#
# Idea of the program is to ease the pain of complex and heavy queries done to
# files with a lot of content (and why not small ones as well), or just to dump
# stuff to database.
#
# This can be achieved by making a dummy sqlite instance, and input the data
# there and run queries there.
#
# By default, sqlite database will be named with the given filename.
#
# Execution of the program itself is simple:
#   ds <delimiter_character> <filename>
#       This will separate file content to columns with the given delimiter,
#       objects will be separated by newline by default. So this means
#       one row => one object. This will create object's colums named as:
#       col1, col2, col3, etc. by default.
#
#  ds -H <delimiter> <filename>
#       if -H is defined, it means that the file has header-row, which will
#       be used to name given columns. Header rows use the same delimiters
#       as the data as well. Header-row will be readed as the first "data" row.
#           Example of header row (where delimiter is "'"):
#                   "name1,name2,name3,name4"
#
# ds -S <delimiter> <filename>
#       if -S is defined, this will mean, that the there is a schema-row for
#       columns in the first row. With this you can define datatypes for
#       columns, for more effecient queries.
#           Example of schema row (where delimiter is ","):
#                   "name1(bigint),name2(varchar),name3(BLOB),name4(boolean)"
#
#  ds -O <character/characterset> <delimiter> <filename>
#       if -O is defined, it means that program will use given character or
#       characterset as separator for objects (which by default is '\n' or
#       newline).
#
# ds -D <database_name> <delimiter> <filename>
#       if -D is defined, it means that the program will use given database-
#       name instead of filename. This is a good way to create multiple
#       tables in one database.
#
#
#
#   Glossary for variables:
#       p = header          (boolean)
#       f = file            (file)
#       d = database name   (String)
#       o = object delimiter(String)
#       r = row             (String)
#       dl = column delimiter(String)
#       sqldb = database connection
#
#   Version 1.0
#   Author: Joel Lappalainen    30.4.2018


from sys import argv
from sqlite3 import connect
from sqlite3 import Error as SQLiteError


def getArgvValue(p):
    return argv[argv.index(p)+1]


def createColumns(r, dl):
    """ Creates dummy columns and return them as a string """
    colRet = ""
    for i in range(len(r.split(dl))):
        colRet = "column"+i+dl+colRet
    return colRet


def createTable(r, dl, sqldb, f):
    columns = ""
    tableCreate = "CREATE TABLE IF NOT EXISTS " + f
    for col in r.split(dl):
        columns = columns + col + " text \n"
    columns = r
    tableCreate = tableCreate + "(" + columns + ")"

    sqldb.execute(tableCreate)
    return 0


def openFile(f):
    """ Opens the file and returns file-object """
    with open(f, 'r') as file:
        return file


def createDatabase(d):
    """ Creates database, and connection to SQLite database """
    try:
        conn = connect(d)
        return conn
    except SQLiteError as e:
        print(e)
    finally:
        conn.close()


def parseFile(f, p, o, dl):
    """ Parses through the file, while checking if the 'p' is True,
        we will skip the first row."""
    return 0


def insertData():
    return 0


def main():
    if "-D" in argv:
        d = getArgvValue("-D")
    else:
        d = argv[-1]
    sqldb = createDatabase(d)

    dl = argv[-2]
    if ["-S", "-H"] in argv:
        p = True
    else:
        p = False
    if "-O" in argv:
        o = getArgvValue("-O")
    else:
        o = '\n'
    f = argv[-1]
    file = openFile(f)

    # Create or fetch columns, and create a table.
    if p:
        createTable(file.readline(), dl, sqldb, f)
    else:
        createTable(createColumns(file.readline(), dl), dl, sqldb)

    # Create

    parseFile(f, p, o, dl)

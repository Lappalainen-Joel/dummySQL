#!/bin/python3.8
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
# Requirement to use this script is to have data file, which has a separate
# header row, which will declare the table schema.
#
# Example of working data file is as following:
#   column1,column2,column3,column4
#   data1,data2,data3,data4
#   data12,data22,data32,data42
# In this case all of the fields will be treated as TEXT datatypes.
#
# It is possible to declare datatypes within the header as following:
#   column1 (TEXT),column2 (BLOB),column3 (INTEGER),column4 (TEXT)
#   test1,0500,12,yay
#
# Execution of the program itself is simple:
#   dummySql.py <file_name>
#       This will create sqlite database and write data rows to it
#       using default parameters which are:
#           tablename = "table1"
#           delimiter = ","
#           databasename = <filename>.db
#
# It is also possible to change the default attributes by:
#   dummySql.py -T <table_name>
#   dummySql.py -S <delimiter>
#   dummySql.py -D <database_name>
#
#
#   Version 1.0
#   Author: Joel Lappalainen    24.5.2020


from sys import argv
from re import search
from sqlite3 import connect
from sqlite3 import Error as SQLiteError
from collections import defaultdict


def getArgvValue(parameter):
    return argv[argv.index(parameter) + 1]


def readHeaderRow(file):
    """ Read header row of file, strip it out from newline and return it.
    Retuns String """
    with open(file, 'r') as f:
        return next(f).strip()


def generateColList(header, delimiter):
    """ Generate attribute list from header row. Which states the position,
    column_name and value_type. Returns a 2-level Dict. """

    # Initialize variables.
    id = 0
    splitheader = header.split(delimiter)
    coldict = defaultdict(dict)

    for i in range(len(splitheader)):
        # Check if column have datatype defined.
        matcher = search(r'(^[\w]+)\s\(([\w]+)\)$', splitheader[i])
        if matcher:
            coldict[id]["column_name"] = matcher.group(1)
            coldict[id]["value_type"] = matcher.group(2)
            id += 1
        # If not defined. Just state all attributes as TEXT.
        else:
            coldict[id]["column_name"] = splitheader[i]
            coldict[id]["value_type"] = "TEXT"
            id += 1

    return coldict


def initDb(name):
    """ Initializes database as param name and returns the connection. """
    try:
        conn = connect(name)
        return conn
    except SQLiteError as e:
        print(e)
    return conn


def createTableWithDict(db, coldict, tablename):
    """ Creates table to database using values from dictionary. """

    # Initialize variables
    command = "CREATE TABLE IF NOT EXISTS " + tablename
    columns = ""
    for i in range(len(coldict)):
        columns = columns + coldict[i]["column_name"]
        columns = columns + " " + coldict[i]["value_type"] + ", "
    command = command + " (" + columns[:-2] + ")"
    db.execute(command)


def formatDataRowToSql(row, coldict, delimiter):
    """ Modifies values in row according to their datatypes. Returns a value
    which can be used in SQL insert statement. Returns String. """

    # Initialize variables
    returnrow = ""

    for i in range(len(row.split(delimiter))):
        if coldict[i]["value_type"] == "BLOB":
            returnrow = returnrow + 'x' + "'" + row.split(delimiter)[i].strip() + "'"
        else:
            returnrow = returnrow + "'" + row.split(delimiter)[i].strip() + "'"
        returnrow = returnrow + ", "
    return returnrow[:-2]


def writeDataToTable(db, table, coldict, file, delimiter):
    """ Writes data rows in the file to the table. """

    # Initialize variables
    basecommand = "INSERT INTO " + table + " ("
    columns = ""

    # Generate SQL INSERT command base.
    for i in range(len(coldict)):
        columns = columns + coldict[i]["column_name"] + ", "
    columns = columns[:-2] + ")"
    basecommand = basecommand + columns + " VALUES ("

    # Ignore the first row.
    with open(file, 'r') as f:
        next(f)
        for row in f:
            sqlcommand = basecommand + formatDataRowToSql(row, coldict, delimiter) + ")"
            print(sqlcommand)
            db.execute(sqlcommand)
            db.commit()


def main():
    # If separate database name declared use that. Otherwise use input file's
    # name with added .db extension
    if "-D" in argv:
        dbfilename = getArgvValue("-D")
    else:
        dbfilename = argv[-1] + ".db"

    # If separate delimiter is declared use that. Otherwise use comma.
    if "-S" in argv:
        delimiter = getArgvValue("-S")
    else:
        delimiter = ","

    # If separate table name is defined use that. Otherwise use 'table1'
    if "-T" in argv:
        tablename = getArgvValue("-T")
    else:
        tablename = "table1"

    list = generateColList(readHeaderRow(argv[-1]), delimiter)
    db = initDb(dbfilename)
    createTableWithDict(db, list, tablename)
    writeDataToTable(db, tablename, list, argv[-1], delimiter)
    db.close()


main()

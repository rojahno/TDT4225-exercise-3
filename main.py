import sys

from DbConnector import DbConnector
from task1.DatabaseSetup import DatabaseSetup


def task_1():
    # Creates a connection with the database
    connector = DbConnector()
    # Creates a database setup object
    setup = DatabaseSetup(connector)



def task_3():
    print('task 3')


def main():
    task_1()


if __name__ == "__main__":
    main()

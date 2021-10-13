import sys

from DbConnector import DbConnector
from task1.DatabaseSetup import DatabaseSetup
from task2.Queries import Queries


def task_1():
    # Creates a database setup object
    setup = DatabaseSetup()

    # Creates user, activites and track points
    # setup.drop_all_coll()
    # setup.create_all_collections()
    # Shows the collections
    # setup.show_coll()
    setup.traverse_dataset()
    # setup.get_num_trackpoints()

def task_2():
    # Creates a connection with the database
    query = Queries()

    # Get number of users
    query.get_num_user()

    # Get number of activities
    query.get_num_activities()

    # Get number of trackpoints
    query.get_num_trackpoints()



def task_3():
    print('task 3')


def main():
    task_2()


if __name__ == "__main__":
    main()

from task1.DatabaseSetup import DatabaseSetup
import time
import datetime


def task_1():
    start = time.time()
    # Creates a database setup object
    setup = DatabaseSetup()
    # Creates user, activites and track points
    setup.drop_all_coll()
    setup.create_all_collections()
    # Shows the collections
    # setup.show_coll()
    setup.traverse_dataset()
    setup.get_num_trackpoints()
    print(f'Time elapsed: {str(datetime.timedelta(seconds=(time.time()-start)))}')


def task_3():
    print('task 3')


def main():
    task_1()


if __name__ == "__main__":
    main()

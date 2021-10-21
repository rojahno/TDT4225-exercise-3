from task1.DatabaseSetup import DatabaseSetup
import time
import datetime


def task_1():
    start = time.time()  # Starts the timer to see how much time has elapsed.
    setup = DatabaseSetup()  # Creates a database setup object
    setup.drop_all_coll()  # Drops the previous created collections
    setup.create_all_collections()  # Creates user, activites and track points
    setup.show_coll()  # Shows the collections
    setup.traverse_dataset()  # Traverses the dataset and inserts data into the collections
    print(f'Time elapsed: {str(datetime.timedelta(seconds=(time.time() - start)))}')  # Prints the time elapsed


def main():
    task_1()


if __name__ == "__main__":
    main()

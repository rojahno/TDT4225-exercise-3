from task1.DatabaseSetup import DatabaseSetup
from task2.Queries import Queries
import time
import datetime


def task_1():
    start = time.time()
    # Creates a database setup object
    setup = DatabaseSetup()
    # Creates user, activites and track points
    # setup.drop_all_coll()
    # setup.create_all_collections()
    # Shows the collections
    # setup.show_coll()
    # setup.traverse_dataset()
    setup.get_num_trackpoints()
    print(f'Time elapsed: {str(datetime.timedelta(seconds=(time.time() - start)))}')


def task_2():
    start = time.time()
    # Creates a connection with the database
    query = Queries()

    # Get number of users
    #query.get_num_user()

    # Get number of activities
    #query.get_num_activities()

    # Get number of trackpoints
    #query.get_num_trackpoints()

    # get avg, min, max of activities
    #query.get_avg_min_max_act_per_user()

    # Find the top 10 users with the highest number of activities
    #query.get_10_users_highest_num_act()

    # Find the number of users who started an activity one day, and ended it the next
    #query.get_num_midnight_active_people()

    # Find activities that are registred multiple times
    query.get_activities_reg_mult_times()

    # Find possibly infected people
    #query.get_possibly_infected_people()

    # get users who've never taken a taxi
    #query.get_non_taxi_users()







    # Get the average activities per user
    #query.get_average_activities()
    timed = str(datetime.timedelta(seconds=(time.time() - start))).split(':')
    print(f'Time elapsed: {timed[0]} hours, {timed[1]} minutes, {timed[2]} seconds')


def task_3():
    print('task 3')


def main():
    task_2()


if __name__ == "__main__":
    main()

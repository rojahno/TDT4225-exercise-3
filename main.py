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
    # query.get_num_trackpoints()

    # get avg, min, max of activities
    # query.get_avg_min_max_act_per_user()

    # Find the top 10 users with the highest number of activities
    # query.get_10_users_highest_num_act()

    # Find the number of users who started an activity one day, and ended it the next
    # query.get_num_midnight_active_people()

    # Find duplicate activities
    # query.get_activities_reg_mult_times()

    # Find possibly infected people
    #query.get_possibly_infected_people()

    # get users who've never taken a taxi
    # query.get_non_taxi_users()

    # distinct users that have taken each (non-null) transportation mode
    # query.count_users_per_trasnp_mode()

    # year with most activities
    #query.year_and_month_with_most_activities()

    # user with most activities in year, month from 9a
    # query.user_most_activities_specific_year_month()

    # user with most activities december 2008
    # query.user_most_activities_specific_year_month()

    # total walked distance in 2008 by user 112

    #top 20 users who have gained most altitude meters
    #query.mile_high_club()

    # Get the average activities per user
    timed = str(datetime.timedelta(seconds=(time.time() - start))).split(':')
    print(f'Time elapsed: {timed[0]} hours, {timed[1]} minutes, {timed[2]} seconds')


def task_3():
    print('task 3')


def main():
    task_2()


if __name__ == "__main__":
    main()

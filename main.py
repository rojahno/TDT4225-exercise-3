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

    # Q1: Get number of users
    # query.get_num_user()

    # Q1: Get number of activities
    # query.get_num_activities()

    # Q1: Get number of track points
    # query.get_num_trackpoints()

    # Q2: Get avg, min, max of activities
    # query.get_avg_min_max_act_per_user()

    # Q3: Find the top 10 users with the highest number of activities
    # query.get_10_users_highest_num_act()

    # Q4: Find the number of users who started an activity one day, and ended it the next
    query.get_nr_users_with_multiple_day_activities()

    # Q5: Find duplicate activities
    # query.get_activities_reg_mult_times()

    # Q6: Find possibly infected people
    # query.get_possibly_infected_people()

    # Q7: get users who've never taken a taxi
    # query.get_non_taxi_users()

    # Q8: distinct users that have taken each (non-null) transportation mode
    # query.count_users_per_trasnp_mode()

    # Q9: year with most activities
    # query.year_and_month_with_most_activities()

    # Q9: user with most activities in year, month from 9a
    # query.user_most_activities_specific_year_month()

    # Q9: user with most activities december 2008
    # query.user_most_activities_specific_year_month()

    # Q10: total walked distance in 2008 by user 112
    # query.tot_dist_in_2008_by_user_112()

    # Q11: top 20 users who have gained most altitude meters
    # query.top_20_attitude_gain_users()

    # Q12: Get all invalid activities
    # query.get_all_users_with_invalid_activities()

    timed = str(datetime.timedelta(seconds=(time.time() - start))).split(':')
    print(f'Time elapsed: {timed[0]} hours, {timed[1]} minutes, {timed[2]} seconds')


def task_3():
    print('task 3')


def main():
    task_2()


if __name__ == "__main__":
    main()

import os
import sys
import uuid
import math

from DbConnector import DbConnector

class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.labels_dict = {}

    # Nr. 1
    def get_num_user(self):
        user_collections = self.db["user"].find().count()
        print("Nr. of users:", user_collections)

    # Nr. 1
    def get_num_activities(self):
        activities_collections = self.db["activities"].find().count()
        print("Nr. of activities:", activities_collections)

    # Nr. 1
    def get_num_trackpoints(self):
        track_point_collections = self.db["track_points"].find().count()
        print("Nr. of track_points:", track_point_collections)

    # Nr. 2
    def get_avg_min_max_act_per_user(self):
        """grouping activities by user_id, and counting number of activities in each group =>
            pick the user with most and least activities, and the avg number of activities.
            If more than one user has the min/max activities, pick the last
        """
        activities_per_user = self.db["activities"].aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "activity_count": {"$count": {}}
                }
            },
            ]
        )

        num_users = self.db["user"].find().count()

        minimum_with_id, maximum_with_id = (None, math.inf), (None, -math.inf)
        num_acs = []
        for i in activities_per_user:
            if i["activity_count"] < minimum_with_id[1]:
                minimum_with_id = (i["_id"], i["activity_count"])
            if i["activity_count"] > maximum_with_id[1]:
                maximum_with_id = (i["_id"], i["activity_count"])
            num_acs.append(i["activity_count"])

        average = sum(num_acs)/num_users

        print(f"min num activities: id: {minimum_with_id[0]}, num: {minimum_with_id[1]}\n"
              f"max num activities: id: {maximum_with_id[0]}, num: {maximum_with_id[1]}\n"
              f"avg num activities: {average}")

    # Nr. 3
    def get_10_users_highest_num_act(self):
        ten_best_users = self.db["activities"].aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "count": {"$count": {}}
                }

            },
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        )
        print(f"10 users with most activities:")
        for i in ten_best_users:
            print(f"Id: {i['_id']}, Number of activities: {i['count']}")

    # Nr. 4
    def get_num_midnight_active_people(self):
        """Calculate the difference between end and start for each activity (in milliseconds),
            and divide by number of milliseconds in a day.
            Return all activities with that difference greater than or equal
            to a day.

        """
        num_midnight_active = self.db["activities"].aggregate([

            {
                "$project":
                    {
                        "_id": "$user_id",
                        "activity_id": "$id",
                        "start_date": "$start_time",
                        "end_date": "$end_time",
                        "dateDifference": {
                            "$divide": [{"$subtract": ["$end_time", "$start_time"]}, 86400000]}
                    }
            },
            {
                "$match": {"dateDifference": {"$gte": 1}}
            }
        ])

        for i in num_midnight_active:
            print(f"id: {i['_id']}, start_date: {i['start_date'].isoformat()}, end_date: {i['end_date'].isoformat()}")

    # Nr. 5

    def get_activities_reg_mult_times(self):
        pass

    # Nr. 6
    def get_possibly_infected_people(self):
        pass

    # Nr. 7
    def get_non_taxi_users(self):
        # To metoder. Vet ikke hvilken som blir riktig.

        # (All users) - (all users who have taken a taxi) = (all users who have not taken a taxi)
        users = self.db["user"].find().distinct("id")
        taxi_users = self.db["activities"].find({"transportation_mode": "taxi"}).distinct("user_id")
        non_taxi_users = sorted(list(set(users) - set(taxi_users)))
        print(f"Nr. of users never taken a taxi ({len(non_taxi_users)} in total):\n")
        for i in range(0, len(non_taxi_users) - 4, 4):
            print(non_taxi_users[i:i + 4])

        # Find all (distinct) user_id in activities where transportation mode is not taxi
        # Denne er muligens feil: en bruker kan ha både aktiviteter uten taxi og aktiviteter med taxi =>
        # da kan det hende det hentes ut fordi han har en eller flere aktiviteter uten taxi
        non_taxi_users_2 = self.db["activities"].find({"transportation_mode": {"$ne": "taxi"}}).distinct("user_id")
        print(f"Nr. of users never taken a taxi ({len(non_taxi_users_2)} in total):\n")
        for i in range(0, len(non_taxi_users_2) - 4, 4):
            print(non_taxi_users_2[i:i+4])

    # Nr. 8

    # Nr. 9

    # Nr. 10

    # Nr. 11

    # Nr. 12

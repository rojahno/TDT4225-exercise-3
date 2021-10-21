import os
import sys
import uuid
import math
from datetime import datetime

import pandas
import pymongo
from IPython.lib.pretty import pprint
from bson.son import SON

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

        average = sum(num_acs) / num_users

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
                        "_id": "$id",
                        "activity_id": "$activity",
                        "user_id": "$user_id",
                        "start_time": "$start_time",
                        "end_time": "$end_time",
                        "start_day": {"$dayOfYear": "$start_time"},
                        "end_day": {"$dayOfYear": "$end_time"},
                    }
            },
            {
                "$match": {"$expr": {"$ne": ["$end_day", "$start_day"]}}
            },

            {
                "$group": {
                    "_id": {"users": "$user_id"},
                    "count": {"$sum": 1}
                }
            },
            {"$project": {
                "users": "$_id.users"
            }
            },
            {"$group": {"_id": "users", "count": {"$sum": 1}}
             },

            {"$sort": {"_id": 1}},
        ])
        print(list(num_midnight_active))
        # for i in num_midnight_active:
        #   print(f"id: {i['_id']}, start_date: {i['start_time'].isoformat()}, end_date: {i['end_time'].isoformat()}")

    # Nr. 5

    def get_activities_reg_mult_times(self):
        a = self.db["activities"].aggregate([
            {
                "$group": {
                    "_id": {"start": "$start_time", "end": "$end_time"},
                    "count": {"$count": {}}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {"$sort": {"_id": -1}}
        ])

        count = 0
        for i in a:
            count += 1
            print(f"start_time: {i['_id']['start']}, end_time: {i['_id']['end']} count: {i['count']}")
        print(count)

    # Nr. 6
    def get_possibly_infected_people(self):

        self.db.track_points.create_index([("location", pymongo.GEOSPHERE)])
        possibly_infected_people = self.db["track_points"].aggregate([
            {
                "$geoNear": {
                    "near": {"type": "Point", "coordinates": [39.97548, 116.33031]},
                    "key": "location",
                    "distanceField": "calculated",
                    "maxDistance": 60,
                    "spherical": True
                }
            }
        ])

        for i in possibly_infected_people:
            print(i)

    # Nr. 7
    def get_non_taxi_users(self):
        # To metoder. Vet ikke hvilken som blir riktig.

        # (All users) - (all users who have taken a taxi) = (all users who have not taken a taxi)
        users = self.db["user"].find().distinct("id")
        taxi_users = self.db["activities"].find({"transportation_mode": "taxi"}).distinct("user_id")
        print(taxi_users)
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
            print(non_taxi_users_2[i:i + 4])

    # Nr. 8
    def count_users_per_trasnp_mode(self):
        users_per_transp_mode = self.db["activities"].aggregate([
            {
                "$match": {
                    "transportation_mode": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "uniqueCount": {"$addToSet": "$user_id"}
                }
            },
            {
                "$project": {
                    "uniqueTranspModeCount": {"$size": "$uniqueCount"}
                }
            }
        ])

        for i in users_per_transp_mode:
            print(f"Trasportation mode: {i['_id']}, Unique non null users: {i['uniqueTranspModeCount']}")

    # Nr. 9a
    def year_and_month_with_most_activities(self):
        months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october"
                                                                                                         "november",
                  "december"]
        year = self.db["activities"].aggregate([
            {
                "$project": {
                    "start_year": {"$year": "$start_time"}
                }
            },
            {
                "$group": {
                    "_id": "$start_year",
                    "activity_count": {"$count": {}}
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "max_activities": {"$max": "$activity_count"},
                }
            },
            {"$sort": {"max_activities": -1}},
            {"$limit": 1}
        ])
        for i in year:
            print(f"year: {i['_id']}, num activities: {i['max_activities']}")

        month = self.db["activities"].aggregate([
            {
                "$project": {
                    "month": {"$month": "$start_time"},
                    "year": {"$year": "$start_time"}
                }
            },
            {
                "$match": {"year": 2008}
            },
            {
                "$group": {
                    "_id": "$month",
                    "activity_count": {"$count": {}}
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "max_activities": {"$max": "$activity_count"},
                }
            },
            {"$sort": {"max_activities": -1}},
            {"$limit": 1}
        ])
        for i in month:
            print(f"month: {months[i['_id'] - 1]}, num activities: {i['max_activities']}")

    # Nr. 9b
    # Is 405 hours for user 062 and 1931 hours for user 128 too much?
    # double check this
    def user_most_activities_specific_year_month(self):
        user = self.db["activities"].aggregate([
            {
                "$project": {
                    "_id": "$user_id",
                    "year_to_match": {"$year": "$start_time"},
                    "month_to_match": {"$month": "$start_time"},
                }
            },
            {
                "$match": {
                    "year_to_match": 2008,
                    "month_to_match": 11
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "activities_per_user": {"$count": {}}
                }
            },

            {"$sort": {"activities_per_user": -1}},
            {"$limit": 2}
        ])

        user_ids = []
        for i in user:
            print(i)
            user_ids.append(i['_id'])

        hours = self.db["activities"].aggregate([
            {
                "$match": {
                    "user_id": {"$in": user_ids}
                }
            },
            {
                "$project": {
                    "_id": "$user_id",
                    "act_id": "$id",
                    "start": "$start_time",
                    "end": "$end_time",
                    "hours_per_activity": {
                        "$divide": [{"$subtract": ["$end_time", "$start_time"]}, 3600000]
                    },
                }
            },
            {"$sort": {"start": -1}},
            {
                "$group": {
                    "_id": "$_id",
                    "hourSum": {"$sum": "$hours_per_activity"}
                }
            }
        ])
        for i in hours:
            print(i)

    # Nr. 10

    # Nr. 11
    def mile_high_club(self):
        # NOT FINISHED!
        users = self.db["user"].find()

        accumulated_altitudes = []

        for user in users:
            user_id = user["id"]

            tp_for_user = self.db["track_points"].aggregate([
                {
                    "$match": {
                        "user_id": user_id
                    }
                },

            ])
            current_user_altitude_sum = 0
            for i in range(1, len(list(tp_for_user))):
                if tp_for_user["altitude"] != -777:
                    current_user_altitude_sum += (tp_for_user[i]["altitude"] - tp_for_user[i - 1]["altitude"])
                accumulated_altitudes.append(current_user_altitude_sum)
            print(accumulated_altitudes)
            """
            current_user_altitude_sum = 0
            # https://stackoverflow.com/questions/9789601/pymongo-doesnt-iterate-over-collection
            for doc in tp_for_user[:-2]:
                if tp_for_user["altitude"] != -777:
                    current_user_altitude_sum += (tp_for_user[i]["altitude"] - tp_for_user[i - 1]["altitude"])
            accumulated_altitudes.append(current_user_altitude_sum)
            """

    # Nr. 12
    def get_all_users_with_invalid_activities(self):
        print('Task 12')
        track_points = self.db["track_points"].aggregate([
            {
                "$project": {
                    "_id": "$id",
                    "activity": "$activity",
                    "start_time": "$start_time",
                    "user_id": "$user_id",
                    "list_position": "$list_position"
                }
            },
            {"$sort": {"activity": 1}},

        ], allowDiskUse=True)
        user_dict = {}
        prev_point = None
        current_activity = ""
        found_invalid = False
        point: dict
        for point in track_points:
            if point['activity'] != current_activity:
                found_invalid = False
                current_activity = point['activity']
                prev_point = None

            if found_invalid is True:
                continue

            if prev_point is None:
                prev_point = point

            else:
                prev_time: datetime = prev_point['start_time']
                current_time: datetime = point['start_time']
                difference = current_time - prev_time
                if difference.seconds >= 300:
                    found_invalid = True
                    user_id: str = point['user_id']
                    if user_id in user_dict.keys():
                        prev_invalid_count = user_dict[user_id]
                        user_dict.update({user_id: prev_invalid_count + 1})
                    else:
                        user_dict[user_id] = 1
                prev_point = point
        pprint(sorted(user_dict.items()))

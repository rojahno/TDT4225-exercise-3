import math
from datetime import datetime
from haversine import haversine
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
        answer_print(1, f"Nr. of users: {user_collections}")

    # Nr. 1
    def get_num_activities(self):
        activities_collections = self.db["activities"].find().count()
        answer_print(1, f"Nr. of activities: {activities_collections}")

    # Nr. 1
    def get_num_trackpoints(self):
        track_point_collections = self.db["track_points"].find().count()
        answer_print(1, f"Nr. of track_points: {track_point_collections}")

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

        answer_print(2, f"min num activities: id: {minimum_with_id[0]}, num: {minimum_with_id[1]}\n"
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
        string_list = []
        for i in ten_best_users:
            string_list.append(f"Id: {i['_id']}, Number of activities: {i['count']}")
        answer_print(3, f"10 users with most activities:", string_list)

    # Nr. 4
    def get_nr_users_with_multiple_day_activities(self):
        """Calculate the difference between end and start for each activity (in milliseconds),
            and divide by number of milliseconds in a day.
            Return all activities with that difference greater than or equal
            to a day.
        """
        num_multiple_day_activities = self.db["activities"].aggregate([
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
        ])

        answer_print(4,
                     f"Number of activities that start one day and ends the next: {num_multiple_day_activities.next()['count']}")

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

        string_list = []
        count = 0
        for i in a:
            count += 1
            string_list.append(f"start_time: {i['_id']['start']}, end_time: {i['_id']['end']} count: {i['count']}")
        answer_print(5, "reccuring activities", string_list)

    # Nr. 6
    def get_possibly_infected_people(self):
        date_to_match = datetime.strptime('2008-08-24 15:38:00', "%Y-%m-%d %H:%M:%S")
        point_to_match = (39.97548, 116.33031)
        possibly_infected_people = []
        points_for_users_matching_date = self.db["track_points"].aggregate([
            {
                "$match": {
                    "$expr": {"$lte": [
                        {"$abs":
                            {"$dateDiff": {
                                "startDate": "$start_time",
                                "endDate": date_to_match,
                                "unit": "minute"}}},
                        60]}
                }
            },
            {
                "$group": {
                    "_id": "$user_id",
                    "time_match_trackpoint": {
                        "$push": "$location.coordinates"
                    }
                }
            },
        ], allowDiskUse=True)

        for i in points_for_users_matching_date:
            if i['_id'] in possibly_infected_people:
                continue
            for user_point in i["time_match_trackpoint"]:
                if haversine(tuple(user_point), point_to_match, unit='m') <= 100:
                    possibly_infected_people.append(i['_id'])
                    break

        string_list = []
        for user in possibly_infected_people:
            string_list.append(user)
        answer_print(6, "Possibly infected users:", string_list)

    # Nr. 7
    def get_non_taxi_users(self):

        non_taxi_users = self.db["activities"].aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "taxi_or_not_arr": {
                        "$push": {
                            "$cond": [
                                {"$eq": ["$transportation_mode", "taxi"]},
                                "taxi",
                                None
                            ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "taxi_or_not_arr": {"$setDifference": ["$taxi_or_not_arr", [None]]}
                }
            },
            {
                "$match": {
                    "$expr": {"$eq": [{"$size": "$taxi_or_not_arr"}, 0]}
                },

            },
            {
                "$group": {
                    "_id": "$_id"
                }
            },
            {"$sort": {"_id": 1}}
        ])

        string_list = []
        print_count = 1
        for i in non_taxi_users:
            if print_count % 6 == 0:
                string_list.append(f"{i['_id']},")
            else:
                string_list.append(f"{i['_id']},")
            print_count += 1
        answer_print(7, "Users who have never taken a taxi:", string_list)

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
        string_list = []
        for i in users_per_transp_mode:
            string_list.append(f"Trasportation mode: {i['_id']}, Unique non null users: {i['uniqueTranspModeCount']}")
        answer_print(8, "User count on activity", string_list)

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
        string_list = []
        for i in year:
            string_list.append(f"year: {i['_id']}, num activities: {i['max_activities']}")

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
            string_list.append(f"month: {months[i['_id'] - 1]}, num activities: {i['max_activities']}")
        answer_print(9, "Year and month with most activities", string_list)

    # Nr. 9b
    def user_most_activities_specific_year_month(self):
        users = self.db["activities"].aggregate([
            {
                "$project": {
                    "_id": "$user_id",
                    "year_to_match": {"$year": "$start_time"},
                    "month_to_match": {"$month": "$start_time"},
                    "hours_per_activity": {
                        "$divide": [{"$subtract": ["$end_time", "$start_time"]}, 3600000]
                    },
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
                    "activities_per_user": {"$count": {}},
                    "hourSum": {"$sum": "$hours_per_activity"}
                }
            },

            {"$sort": {"activities_per_user": -1}},
            {"$limit": 2}
        ])
        string_list = []
        user: dict
        for user in users:
            string_list.append(
                f'User {user["_id"]} have {user["activities_per_user"]} activities, and {round(user["hourSum"], 2)} hours ')
        answer_print(9, "User with most activities in december 2008:", string_list)

    # Nr. 10
    def tot_dist_in_2008_by_user_112(self):
        activities = self.db["activities"].aggregate([
            {
                "$match": {
                    "transportation_mode": "walk",
                    "user_id": "112"
                },
            },
            {
                "$project": {
                    "_id": "$id"
                }
            }
        ])

        act_id = [activity["_id"] for activity in activities]

        track_points = self.db["track_points"].aggregate([
            {
                "$match": {
                    "activity": {"$in": act_id}}
            },
            {
                "$project": {
                    "id": "$user_id",
                    "start_year": {"$year": "$start_time"},
                    "latlon": "$location.coordinates",
                }
            },
            {
                "$group": {
                    "_id": "$id",
                    "lat_lons": {
                        "$push": {
                            "$cond": [
                                {"$eq": ["$start_year", 2008]},
                                "$latlon",
                                None
                            ]
                        }
                    }
                }
            }
        ])
        dist = 0
        for i in track_points:
            for j in range(0, len(i["lat_lons"]) - 1):
                dist += haversine(tuple(i["lat_lons"][j - 1]), tuple(i["lat_lons"][j]))
        answer_print(10, f"User 112 walked {dist} km in 2008")

    # Nr. 11
    def top_20_attitude_gain_users(self):

        users_ids = self.db["track_points"].distinct("user_id")

        accumulated_altitudes = []

        for user_id in users_ids:

            tp_for_user = self.db["track_points"].aggregate([
                {
                    "$match": {
                        "user_id": user_id
                    }
                },
                {
                    "$project": {
                        "_id": "$user_id",
                        "activity_id": "$activity",
                        "altitude": "$altitude",
                        "start_time": "$start_time"
                    }
                },
                {"$sort": {"start_time": 1}}
            ], allowDiskUse=True)

            tp_list = list(tp_for_user)
            current_user_altitude_sum = 0
            for i in range(1, len(tp_list)):
                if (tp_list[i - 1]["altitude"] != -777) and (tp_list[i]["altitude"] != -777) \
                        and (tp_list[i - 1]["altitude"] < tp_list[i]["altitude"]):
                    if tp_list[i - 1]["activity_id"] == tp_list[i]["activity_id"]:
                        current_user_altitude_sum += (tp_list[i]["altitude"] - tp_list[i - 1]["altitude"])
            print(user_id, "done")
            accumulated_altitudes.append((user_id, current_user_altitude_sum))
            accumulated_altitudes = sorted(accumulated_altitudes, key=lambda x: x[1], reverse=True)
        string_list = []
        for i in accumulated_altitudes[:20]:
            string_list.append(f"id: {i[0]}, altitude gained: {i[1]}")
        answer_print(11, "Top 20 users with highest altitude gained:", string_list)

    # Nr. 12
    def get_all_users_with_invalid_activities(self):
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
        answer_print(12, "Users with invalid activities and their count", sorted(user_dict.items()))


def answer_print(question_nr=0, string="", string_list=None):
    print("\n---------------------------")
    print(f"QUESTION: {question_nr}")
    print(string)
    if string_list is not None:
        for string in string_list:
            print(string)
    print("---------------------------\n")

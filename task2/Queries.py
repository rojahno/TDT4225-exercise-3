import os
import sys
import uuid

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
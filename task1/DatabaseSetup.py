import os
import sys
import uuid

from DbConnector import DbConnector
import datetime
"""
Handles the database setup.
- Creates the tables
- Inserts data from the files
- Prints data from the database
"""


class DatabaseSetup:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.labels_dict = {}

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def create_all_collections(self):
        self.create_coll("user")
        self.create_coll("activities")
        self.create_coll("track_points")

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def drop_all_coll(self):
        self.drop_coll("user")
        self.drop_coll("activities")
        self.drop_coll("track_points")

    def show_coll(self):
        collections = self.client['test_db'].list_collection_names()
        print(collections)

    def is_plt_file(self, extension):
        return extension == ".plt"

    def get_extension(self, path):
        name, extension = os.path.splitext(path)
        return extension

    def get_nr_of_lines(self, path):
        """
        Returns the number of lines in the file
        @param path: The path of the file
        @type path: str
        @return: Number of lines
        @rtype: int
        """
        reoccurring_lines = 6  # The 6 lines in the top which are not trajectories
        num_lines = sum(1 for line in open(path)) - reoccurring_lines
        return num_lines

    def get_user_label(self):
        """
        Returns the user labels in a list. Loads the entire file into memory and should not be used on large files.
        @return: A list of labels
        @rtype: list
        """
        label_path = "dataset/dataset/labeled_ids.txt"
        labels = open(label_path, 'r').read().splitlines()
        return labels

    def get_user_ids(self):
        """
        Searches the dataset folder for user ids and returns a sorted list of user ids.
        @return: A sorted list of user ids
        @rtype: list
        """
        path = "dataset/dataset/Data"
        user_ids = sorted([f for f in os.listdir(path) if not f.startswith('.')])
        return user_ids

    def get_last_line(self, root: str, file: str):
        """
        @param root: The path of the file
        @type root: str
        @param file: The name of the file
        @type file:str
        @return: The last line in the file
        @rtype: str
        """
        try:
            path = os.path.join(root, file)  # The current path
            with open(path, "r") as f1:
                last_line = f1.readlines()[-1].rstrip()
                return last_line
        except Exception as e:
            print(f'An error occurred while retrieving the last line in the file:{sys.exc_info()[2]}')

    def get_first_line(self, root: str, file: str):
        """
        Returns the first relevant line
        @param root: The path of the file
        @type root: str
        @param file: The name of the file
        @type file:str
        @return: The first relevant line
        @rtype: str
        """
        try:
            with open(os.path.join(root, file)) as f:
                return_line = ""
                for read in range(7):
                    return_line = f.readline()  # removes the first lines containing descriptions.
                return return_line
        except Exception as e:
            print(f'An error occurred while retrieving the first line in the file:{sys.exc_info()[2]}')

    def format_label_line(self, label: str):
        """
        Formats the label line in a file to the values we need to insert into the database.
        @param label: The label line
        @type label: str
        @return: start_time, end_time and transportation_mode
        @rtype: str
        """
        values = label.split()
        start_time = "".join((values[0], " ", values[1]))
        end_time = "".join((values[2], " ", values[3]))
        transportation_mode = "".join(values[4])
        start_time = start_time.replace("/", "-")
        end_time = end_time.replace("/", "-")
        start_time = datetime.datetime.strptime(
            start_time,
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = datetime.datetime.strptime(
            end_time,
            "%Y-%m-%d %H:%M:%S"
        )
        return start_time, end_time, transportation_mode

    def format_trajectory_time(self, label: str):
        values = label.split(",")
        time = datetime.datetime.strptime(
            "".join((values[5], " ", values[6])),
            "%Y-%m-%d %H:%M:%S"
        )
        return time

    def format_trajectory_line(self, line: str):
        """
        Formats the trajectory line in a file
        @param line: The trajectory line
        @type line: str
        @return: latitude, longitude, altitude, days_passed and start_time
        @rtype: str
        """
        values = line.split(",")
        latitude = float(values[0])
        longitude = float(values[1])
        altitude = float(values[3])
        days_passed = float(values[4])
        start_time = datetime.datetime.strptime(
            "".join((values[5].replace('-', '/'), " ", values[6])).rstrip(),
            "%Y/%m/%d %H:%M:%S"
        )
        return latitude, longitude, altitude, days_passed, start_time

    def create_activity(self, root, file):
        """
        Creates a new activity
        @param root: The path of the file
        @type root: str
        @param file: The name of the file
        @type file:str
        @return: Activity - a new activity
        @rtype: Activity
        """
        labeled_users = self.get_user_label()
        user_id = os.path.basename(os.path.dirname(root))
        first_line = self.get_first_line(root, file).rstrip()
        last_line = self.get_last_line(root, file)
        start_time = self.format_trajectory_time(first_line)
        end_time = self.format_trajectory_time(last_line)
        activity_id = str(uuid.uuid4())
        date_key = tuple((start_time, end_time))
        if user_id in labeled_users:
            if date_key in self.labels_dict[user_id]:
                transportation_mode = self.labels_dict[user_id][date_key]
                return {'id': activity_id, 'user_id': user_id, 'start_time': start_time, 'end_time': end_time,
                        'transportation_mode': transportation_mode}
        return {'id': activity_id, 'user_id': user_id, 'start_time': start_time, 'end_time': end_time,
                'transportation_mode': None}

    def create_user(self, root):
        user_id = os.path.basename(os.path.dirname(root))
        print(user_id)
        user_label = self.get_user_label()
        has_label = False
        if user_id in user_label:
            has_label = True
        return {'id': user_id, 'has_label': has_label}

    def create_label_activities(self):
        """
        Creates a new label activity.
        @return: label_activity_list - a list with all label activities
        @rtype: list
        """
        for root, dirs, files in os.walk('dataset/dataset/Data', topdown=True):
            for file in files:
                if file == "labels.txt":
                    with open(os.path.join(root, file)) as f:
                        f.readline()  # removes the first line containing descriptions.
                        labeled_users = self.get_user_label()
                        user_id = os.path.basename(os.path.basename(root))
                        # if user-id (e.g. 082) isn't a key in label-dict, add it empty dict as value
                        if (user_id not in self.labels_dict) and (user_id in labeled_users):
                            self.labels_dict[user_id] = {}
                        for line in f:
                            start_time, end_time, transportation_mode = self.format_label_line(line)
                            # Assuming label dates (together) are unique => add label-dates-tuple as key, with
                            # transportation mode as value
                            date_key = tuple((start_time, end_time))
                            self.labels_dict[user_id][date_key] = transportation_mode

    def traverse_dataset(self):
        """
        Traverses the dataset and inserts activities and track_points
        @return: None
        @rtype: None
        """
        # populate label_dict
        self.create_label_activities()
        for root, dirs, files in os.walk('dataset/dataset/Data', topdown=True):
            if len(dirs) == 0 and len(files) > 0:
                user = self.create_user(root)
                self.insert_user(user)
                for file in files:
                    path = os.path.join(root, file)  # The current path
                    if self.is_plt_file(self.get_extension(path)) and self.get_nr_of_lines(path) <= 2500:
                        track_point_list = []  # A list to batch insert the trajectories
                        activity = self.create_activity(root, file)
                        self.insert_activity(activity)  # Inserts the activity into the database
                        with open(os.path.join(root, file)) as f:  # opens the current file
                            for read in range(6):
                                f.readline()
                            for line in f:
                                latitude, longitude, altitude, days_passed, start_time = \
                                    self.format_trajectory_line(line)
                                track_point_list.append(
                                    {
                                        'user_id': user['id'],
                                        'activity': activity['id'],
                                        'location': {
                                             'type': 'Point',
                                             'coordinates': [latitude, longitude]
                                        },
                                        'altitude': altitude,
                                        'days_passed': days_passed,
                                        'start_time': start_time})
                        self.batch_insert_track_points(track_point_list)

    def insert_user(self, user: dict):
        self.db["user"].insert_one(user)
        print("Created user", user, "with", "activities")

    def insert_activity(self, activity: dict):
        self.db["activities"].insert_one(activity)
        # print("Created activity", activity, "with", "trackpoints")

    def batch_insert_track_points(self, track_points: list):
        self.db["track_points"].insert_many(track_points)
        # print("Created trackpoints", track_points)

    def get_num_trackpoints(self):
        track_point_collections = self.db["track_points"].find().count()
        print(track_point_collections)

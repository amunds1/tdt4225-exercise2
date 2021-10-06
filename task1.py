from tabulate import tabulate
from utils.DbConnector import DbConnector
from indexer.userindexer import UserIndexer
from indexer.activityindexer import indexLabels, indexSingleActivity
from utils.utils import convertToCorrectDateFormat


class GeolifeStats:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(30) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN DEFAULT 0)
                """

        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS Activity (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                user_id VARCHAR(30),
                CONSTRAINT userFk FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE,
                transportation_mode VARCHAR(30),
                start_date_time DATETIME,
                end_date_time DATETIME)
                """

        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT,
                CONSTRAINT activityFk FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DATETIME,
                date_time DATETIME)
                """

        self.cursor.execute(query)
        self.db_connection.commit()

    def populate_user_table(self, users):
        """
        Populates the User table with id and has_label
        """
        for userId, data in users.items():
            query = "INSERT INTO User (id, has_labels) VALUES ('%s', '%s')"
            self.cursor.execute(query % (userId, data["has_labels"]))

        self.db_connection.commit()

    def insert_activities_for_users_with_labels(self, user, userID):
        activityQuery = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (" \
                        "'%s', '%s', '%s', '%s');"
        trackPointQuery = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (" \
                          "'%s', '%s', '%s', '%s', '%s', '%s') "

        if user["has_labels"]:
            labels = indexLabels(user)
            matchingFiles = []

            # Go trough labels
            for labelForComparison in labels:
                # Extract start date and start time from label entry
                labelStartTime = convertToCorrectDateFormat(labelForComparison[0]) + " " + \
                                 labelForComparison[1]

                # Extract end date and end time from label entry
                labelEndTime = convertToCorrectDateFormat(labelForComparison[2]) + " " + labelForComparison[
                    3]

                # Go trough each .plt file
                for file in user["files"]:
                    matchOnStartTime = False
                    matchOnEndTime = False

                    # Parse .plt file
                    activity = indexSingleActivity(user, file)

                    # Go trough each trackpoint
                    for trackpoint in activity:

                        # Extract start date and time from trackpoint
                        trackPointDateTime = trackpoint["date"] + " " + trackpoint["time"]

                        if trackPointDateTime == labelStartTime:
                            matchOnStartTime = True
                        elif trackPointDateTime == labelEndTime:
                            matchOnEndTime = True

                        if matchOnStartTime and matchOnEndTime:
                            # print("Match on file: ", file)
                            # print("Label start time: ", labelStartTime)
                            # print("Label end time: ", labelEndTime)
                            # print("\n")

                            if file not in matchingFiles:
                                matchingFiles.append(file)

                            # Extract transport mode from label entry
                            transportMode = labelForComparison[4]

                            self.cursor.execute(activityQuery % (userID, transportMode, labelStartTime, labelEndTime))

                            # Get id field of newly added Activity
                            activityID = self.cursor.lastrowid

                            for tp in activity:
                                self.cursor.execute(trackPointQuery % (activityID,
                                                                       tp["latitude"],
                                                                       tp["longitude"],
                                                                       tp["altitude"],
                                                                       tp["date"],
                                                                       tp["date"] + " " + tp["time"]))

                            break

                        else:
                            pass

        # For non matching files
        for file in user["files"]:
            if file not in matchingFiles:
                try:
                    activity = indexSingleActivity(user, file)

                    # Select the time and date of first activity as start time
                    startTime = "{startDate} {startTime}".format(startDate=activity[0]["date"],
                                                                 startTime=activity[0]["time"])

                    # Select the time and date of the last activity as end time
                    endTime = "{startDate} {startTime}".format(startDate=activity[len(activity) - 1]["date"],
                                                               startTime=activity[len(activity) - 1]["time"])

                    self.cursor.execute(activityQuery % (userID, 'null', startTime, endTime))

                    # Get id field of newly added Activity
                    activityID = self.cursor.lastrowid

                    for trackpoint in activity:
                        self.cursor.execute(trackPointQuery % (activityID,
                                                               trackpoint["latitude"],
                                                               trackpoint["longitude"],
                                                               trackpoint["altitude"],
                                                               trackpoint["date"],
                                                               trackpoint["date"] + " " + trackpoint["time"]))

                except Exception as e:
                    print(e)

        self.db_connection.commit()

    def insert_activities_for_users_without_labels(self, user, userID):
        activityQuery = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (" \
                        "'%s', '%s', '%s', '%s');"
        trackPointQuery = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (" \
                          "'%s', '%s', '%s', '%s', '%s', '%s') "

        if not user["has_labels"]:
            # For each activity
            for file in user["files"]:
                try:
                    activity = indexSingleActivity(user, file)

                    # Select the time and date of first activity as start time
                    startTime = "{startDate} {startTime}".format(startDate=activity[0]["date"],
                                                                 startTime=activity[0]["time"])

                    # Select the time and date of the last activity as end time
                    endTime = "{startDate} {startTime}".format(startDate=activity[len(activity) - 1]["date"],
                                                               startTime=activity[len(activity) - 1]["time"])

                    self.cursor.execute(activityQuery % (userID, 'null', startTime, endTime))

                    # Get id field of newly added Activity
                    activityID = self.cursor.lastrowid

                    for trackpoint in activity:
                        self.cursor.execute(trackPointQuery % (activityID,
                                                               trackpoint["latitude"],
                                                               trackpoint["longitude"],
                                                               trackpoint["altitude"],
                                                               trackpoint["date"],
                                                               trackpoint["date"] + " " + trackpoint["time"]))

                except Exception as e:
                    print(e)

        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)


def main():
    application = None
    userIndexer = None

    try:
        application = GeolifeStats()
        userIndexer = UserIndexer({})

        userIndexer.convertDatasetToDictionary()

        """
        Drop existing tables
        """
        application.drop_table("TrackPoint")
        application.drop_table("Activity")
        application.drop_table("User")

        """
        Create tables
        """
        application.create_user_table()
        application.create_activity_table()
        application.create_trackpoint_table()

        """
        Populate User table with users
        """
        application.populate_user_table(userIndexer.users)

        """
        Insert data
        """
        for userID in userIndexer.users:
            print("Now on user ", userID)
            if userIndexer.users[userID]["has_labels"]:
                application.insert_activities_for_users_with_labels(userIndexer.users[userID], userID)
            else:
                application.insert_activities_for_users_without_labels(userIndexer.users[userID], userID)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if application:
            application.connection.close_connection()


if __name__ == '__main__':
    main()

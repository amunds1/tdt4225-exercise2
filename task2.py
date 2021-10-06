import datetime

from haversine import haversine
from utils.DbConnector import DbConnector


class Task2:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # Complete
    def one(self):
        query = "SELECT " \
                "(SELECT COUNT(*) FROM User) as UserCount, " \
                "(SELECT COUNT(*) FROM Activity) as ActivityCount, " \
                "(SELECT COUNT(*) FROM TrackPoint) as TrackPointCount "

        self.cursor.execute(query)

        print("Task #1")
        for (UserCount, ActivityCount, TrackPointCount) in self.cursor:
            print("Number of users:", UserCount)
            print("Number of activities:", ActivityCount)
            print("Number of trackpoints:", TrackPointCount)

    # Complete
    def two(self):
        queryAvg = "SELECT AVG(AverageActivity) FROM (SELECT COUNT(*) AS AverageActivity FROM Activity GROUP BY " \
                   "user_id) AS sub "

        queryMin = "SELECT MIN(MinActivity) FROM (SELECT COUNT(*) AS MinActivity FROM Activity GROUP BY user_id) AS sub"

        queryMax = "SELECT MAX(MaxActivity) FROM (SELECT COUNT(*) AS MaxActivity FROM Activity GROUP BY user_id) AS sub"

    # Complete
    def three(self):
        query = "SELECT user_id as User, COUNT(*) AS Activities " \
                "FROM Activity GROUP BY user_id " \
                "ORDER BY Activities DESC " \
                "LIMIT 10 "

        self.cursor.execute(query)

        for (User, Activities) in self.cursor:
            print("User: {user} Activities: {activities}".format(user=User, activities=Activities))

    # Complete
    def four(self):
        users = []

        for i in range(0, 183):
            query = "SELECT start_date_time, end_date_time FROM Activity WHERE user_id={i}".format(i=i)

            self.cursor.execute(query)

            for (start_date_time, end_date_time) in self.cursor:
                # Increment start date with one day
                incrementedStartDate = start_date_time + datetime.timedelta(days=1)

                # Check if incremented start date equals end date
                if incrementedStartDate.date() == end_date_time.date():
                    if i not in users:
                        users.append(i)

        print("Users that have started an activity one day and ended it the next:", len(users))

    # Complete
    def five(self):
        query = "SELECT user_id, transportation_mode, start_date_time, end_date_time, COUNT(*) as Count " \
                "FROM Activity " \
                "GROUP BY user_id, transportation_mode, start_date_time, end_date_time " \
                "HAVING COUNT(*) > 1"

        self.cursor.execute(query)

        for (Count) in self.cursor:
            print("Duplicate rows:", Count)

    def six(self):
        # Requires that data.zip is unzipped
        pass

    # Complete
    def seven(self):
        users = ['135', '132', '104', '103', '168', '157', '150', '159', '166', '161', '102', '105', '133', '134', '160', '158',
         '167', '151', '169', '156', '024', '023', '015', '012', '079', '046', '041', '048', '077', '083', '084', '070',
         '013', '014', '022', '025', '071', '085', '049', '082', '076', '040', '078', '047', '065', '091', '096', '062',
         '054', '053', '098', '038', '007', '000', '009', '036', '031', '052', '099', '055', '063', '097', '090', '064',
         '030', '008', '037', '001', '039', '006', '174', '180', '173', '145', '142', '129', '116', '111', '118', '127',
         '120', '143', '144', '172', '181', '175', '121', '119', '126', '110', '128', '117', '153', '154', '162', '165',
         '131', '136', '109', '100', '107', '138', '164', '163', '155', '152', '106', '139', '101', '137', '108', '130',
         '089', '042', '045', '087', '073', '074', '080', '020', '027', '018', '011', '016', '029', '081', '075', '072',
         '086', '044', '088', '043', '017', '028', '010', '026', '019', '021', '003', '004', '032', '035', '095', '061',
         '066', '092', '059', '050', '057', '068', '034', '033', '005', '002', '056', '069', '051', '093', '067', '058',
         '060', '094', '112', '115', '123', '124', '170', '177', '148', '141', '146', '179', '125', '122', '114', '113',
         '147', '178', '140', '176', '149', '171']

        query = "SELECT user_id as UserID FROM Activity " \
                "WHERE transportation_mode = 'taxi'" \
                "GROUP BY UserID"

        self.cursor.execute(query)

        for (UserID) in self.cursor:
            users.remove(UserID[0])

        print(users)

    # Complete
    def eight(self):
        query = "SELECT COUNT(*) AS Count, transportation_mode AS TransportationMode FROM Activity WHERE " \
                "transportation_mode != 'null' GROUP BY transportation_mode "

        self.cursor.execute(query)

        for (Count, TransportationMode) in self.cursor:
            print("Count:", Count, " Transportation mode:", TransportationMode)

    # Complete
    def nine(self):
        # Task a)
        queryA = "SELECT YEAR(start_date_time) AS Year, MONTH(start_date_time) AS Month, COUNT(*) AS Activities FROM " \
                 "Activity GROUP BY YEAR(start_date_time), MONTH(start_date_time) ORDER BY Activities DESC LIMIT 1"

        self.cursor.execute(queryA)

        for (Year, Month, Activities) in self.cursor:
            print("Year:", Year)
            print("Month:", Month)
            print("Activities:", Activities)

        # Task b)

        # Find the users with the most activities in november of 2008
        queryB = "SELECT user_id as User, COUNT(*) AS Activities FROM Activity Where MONTH(start_date_time)='11' && " \
                 "YEAR(start_date_time)='2008' GROUP BY user_id ORDER BY Activities DESC LIMIT 2; "

        self.cursor.execute(queryB)

        # Save users to a dictionary
        users = {}
        for (User, Activities) in self.cursor:
            users[User] = 0

        print(users)

        # Update users total hours spent by comparing difference start and end time of an activity
        for (UserID, hoursSpent) in users.items():
            query = "SELECT start_date_time AS Start, end_date_time AS End FROM Activity WHERE " \
                    "user_id={}".format(UserID)

            self.cursor.execute(query)

            for (Start, End) in self.cursor:
                timeDelta = End - Start
                totalSeconds = timeDelta.total_seconds()
                minutes = totalSeconds / 60
                hours = minutes / 60

                users[UserID] += hours

        for UserID, hoursSpent in users.items():
            print("User {UserID} has spent a total of {hoursSpent} hours".format(UserID=UserID, hoursSpent=round(hoursSpent)))

    # Complete
    def ten(self):
        query = "SELECT lat, lon FROM User " \
                "INNER JOIN Activity ON User.id = Activity.user_id " \
                "INNER JOIN TrackPoint TP on Activity.id = TP.activity_id " \
                "WHERE user_id=112 AND YEAR(start_date_time)='2008' AND transportation_mode='walk'"

        self.cursor.execute(query)

        distance = 0
        oldPos = (0, 0)

        for (lat, lon) in self.cursor:
            newPos = (lat, lon)
            distance += haversine(oldPos, newPos)

            oldPos = newPos

        print(round(distance, 2), "kilometres")

    # Complete
    def eleven(self):
        result = {}

        for user in range(0, 183):
            query = "SELECT user_id as UserID, altitude from Activity " \
                    "inner join TrackPoint TP on Activity.id = TP.activity_id " \
                    "where user_id={user}".format(user=user)

            self.cursor.execute(query)

            altitudeGained = 0
            oldAlt = -999
            for (UserID, altitude) in self.cursor:
                newAlt = altitude

                # On first iteration set oldAlt to newAlt to get correct initial alt difference
                if oldAlt == -999:
                    oldAlt = newAlt

                if newAlt > oldAlt or newAlt != -777:
                    altitudeGained += newAlt - oldAlt

                    result[UserID] = altitudeGained

                oldAlt = newAlt

        sortedResult = sorted(result.items(), key=lambda x: x[1], reverse=True)

        for userWithAltitude in sortedResult:
            print("User:", userWithAltitude[0], " Altitude gained:", userWithAltitude[1])


    def twelve(self):
        pass


def main():
    question = Task2()

    #question.one()
    #question.two()
    #question.three()
    #question.four()
    #question.five()
    #question.six()
    #question.seven()
    #question.eight()
    #question.nine()
    #question.ten()
    #question.eleven()


if __name__ == '__main__':
    main()












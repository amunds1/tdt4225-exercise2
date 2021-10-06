import os


class UserIndexer:
    def __init__(self, users):
        self.users = users

    def convertDatasetToDictionary(self):
        users = {}

        for root, dirs, files in os.walk("dataset/dataset/Data", topdown=False):
            # Discard root path
            if root == 'dataset/dataset/Data':
                pass
            else:
                # Extract userId from root path (e.g. dataset/dataset/Data/114/Trajectory)
                userId = root.split("/")[3]

                # Create a new object for a user if it does not already exists in the dictionary
                if userId in users:
                    pass
                else:
                    users[userId] = {}

                # Update object with root path and .plt file names belonging to a given user
                if "/Trajectory" in root:
                    filesInfo = {
                        "path": root,
                        "files": files,
                        "has_labels": 0
                    }

                    users[userId].update(filesInfo)

                # Set has_labels to True if user has a labels.txt file
                if 'labels.txt' in files:
                    users[userId].update({"has_labels": 1})

        self.users = users



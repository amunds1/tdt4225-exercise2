import re
from utils.utils import generatePltFilePath


def indexLabels(user):
    """
    Returns a list of entries from a given label.txt file

    Args:
        user: User

    Returns:
        activities []
    """
    activities = []

    filePath = user["path"].replace("/Trajectory", "/labels.txt")

    file = open(filePath, "r")

    # Save lines to an array and discard the first one ("Start Time	End Time Transportation Mode")
    entries = file.readlines()[1:]

    for entry in entries:
        information = re.split(" |\t|\n", entry)
        # Remove last [''] element
        information = information[0:len(information) - 1]

        activities.append(information)

    return activities


def indexSingleActivity(user, activityFilename):
    """
    Returns a list of trajectories from a .plt file, where each trajectory is stored as a dictionary

    Args:
        user: User
        activityFilename: Filename of the activity

    Returns:
        trajectoriesInActivity [{}]
    """

    try:
        trajectoriesInActivity = []

        filePath = generatePltFilePath(user["path"], activityFilename)

        file = open(filePath, "r")

        # Save lines to an array and discard the first one ("Start Time	End Time Transportation Mode")
        trajectories = file.readlines()[6:]

        # Discard .plt files with more than 2500 entries
        if len(trajectories) < 2500:
            for trajectory in trajectories:
                # Only parse trajectories of type string
                if isinstance(trajectory, str):
                    trajectory = trajectory.split(",")

                    trajectoryData = {
                        "latitude": trajectory[0],
                        "longitude": trajectory[1],
                        "altitude": trajectory[3],
                        "date": trajectory[5],
                        "time": trajectory[6].replace("\n", "")
                    }

                    trajectoriesInActivity.append(trajectoryData)

        return trajectoriesInActivity

    except:
        print("An exception occurred")



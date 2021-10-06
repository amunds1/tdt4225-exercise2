# Convert from 2007/04/29 to 2007-04-29
def convertToCorrectDateFormat(date):
    return date.replace("/", "-")


def generatePltFilePath(root, file):
    return "{root}/{file}".format(root=root, file=file)

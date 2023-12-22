from matplotlib import pyplot as plt


def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


def plotDistribution(data , xlabel , ylabel, title):
	x, y = zip(*data)


	plt.figure(figsize=(10, 6))
	plt.bar(x, y)
	plt.xlabel(xlabel)
	plt.ylabel(ylabel)
	plt.title(title)
	plt.show()


def generateJson(key , value):
	return {key: value}

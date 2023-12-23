
import time
import functools



def execTime(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__!r} executed in {(end_time - start_time):.4f} seconds")
        return result
    return wrapper


def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


def plotDistribution(data , xlabel , ylabel, title):
	return


def generateJson(key , value):
	return {key: value}

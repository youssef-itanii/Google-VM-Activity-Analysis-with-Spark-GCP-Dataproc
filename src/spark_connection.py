from pyspark import SparkContext


class SparkConnection:
    def __init__(self , workers = "local[*]"):
        self.sc = SparkContext(workers)
        self.sc.setLogLevel("ERROR")
   
    def loadData(self, file_path):
        rdd = self.sc.textFile(file_path)
        rdd = rdd.map(lambda x : x.split(','))
        return rdd
        
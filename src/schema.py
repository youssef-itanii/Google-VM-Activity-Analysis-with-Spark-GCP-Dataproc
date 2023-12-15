from util import findCol
from pyspark import SparkContext

class Schema:
    def __init__(self , sc:SparkContext, file_path:str):
        self.entries = sc.textFile(file_path)
        self.fields = self.entries.filter(lambda x: "content" in x).collect()[0].replace('"','').split(',')
        self.entries = self.entries.map(lambda x : x.split(','))
    
    
    def getFieldNoByContent(self, file_pattern, content):
        content_index = findCol(self.fields,"content")
        field_no = findCol(self.fields,"field number")
        data = self.entries.filter(lambda x: file_pattern in x[0] and content in x[content_index]).collect()
        try:
            return int(data[0][field_no]) - 1
        except IndexError:
            return -1
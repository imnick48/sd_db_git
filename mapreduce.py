import csv
import re
from mrjob.job import MRJob

class MRWordFrequency(MRJob):

    def mapper(self, _, line):
        reader = csv.reader([line]) 
        for row in reader:
            text = ' '.join(row)  
            words = re.findall(r'\w+', text.lower()) 
            for word in words:
                yield (word, 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    MRWordFrequency.run()

from mrjob.job import MRJob
import re

class MRUniqueWordCounter(MRJob):

    def mapper(self, _, line):
        # Tokenize the line into words using a regular expression
        words = re.findall(r'\b\w+\b', line.lower())

        # Emit each word as a key with a value of 1
        for word in words:
            yield (word, 1)

    def combiner(self, word, counts):
        # Sum the counts of each word locally
        yield (word, sum(counts))

    def reducer(self, word, counts):
        # Count the number of unique words by simply emitting the word with a value of 1
        yield (word, 1)

if __name__ == '__main__':
    MRUniqueWordCounter.run()

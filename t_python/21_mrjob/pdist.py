# -*- coding: UTF-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import itertools

WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_user,
                   reducer=self.reducer_user)
        ]
    
    def mapper_user(self, _, line):
        url, user, weight = line.split('\t')[:3]
        yield user, (url, weight)

    def reducer_user(self, user, url_weight):
        weight = {}
        for x in url_weight:
            url, w = x
            weight[url] = int(w)
        url_pair = itertools.combinations(sorted(weight.keys()), 2)
        for url1, url2 in url_pair:
            prod = weight[url1] * weight[url2]
            yield (url1, url2), prod

if __name__ == '__main__':
    MRMostUsedWord.run()
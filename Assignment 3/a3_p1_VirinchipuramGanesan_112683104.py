# author gh: @adithya8
# To add: Normalization before testing, Check p testing logic, Replace Counter, Multi variate correction.

###################################
import pyspark
from sklearn import linear_model
from scipy import stats
import numpy as np

import json
import re
import sys
#Check out if Counter is ok 
from collections import Counter
from pprint import pprint

###################################
# Regex match for pattern
re_pattern =  r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))'
# Top n common words
n_words = 1000
# Seed value for random split
seed = 43
###################################

def prepareDist(x, y):
    '''
        Function to prepare the Ratings list, RelFrequency list and Verified list for each word
    '''
    #x, y: (('Ratings', [...]), ('RelFreq', [...]), ('VERIFIED', [...]))
    result = [None]* len(x)
    for i in range(len(x)):
        result[i] = (x[i][0], x[i][1] + y[i][1])
    return tuple(result)

def doTest(x):

    def performTest(x, y):
        model = linear_model.LinearRegression().fit(x, y)
        sse = np.sum((model.predict(x) - ratings) ** 2, axis=0) / float(x.shape[0] - x.shape[1] - 1)
        se = np.array([np.sqrt(sse/np.sum((x[:, i] - np.mean(x[:, i]))**2) ) for i in range(x.shape[1]) ])
        t = model.coef_ / se
        p = 2 * (1 - stats.t.cdf(np.abs(t), ratings.shape[0] - relFreq.shape[1] - 1))
        return p[:, -1]

    #Reference from https://gist.github.com/brentp/5355925 and slides
    #x: (word, (('Ratings', [Rating1, ...]), ('RelFreq', [ratio1, ...]), ('VERIFIED', [Verified1, ...])))
    ratings = np.array(x[1][0][1]).reshape(-1,1)
    relFreq = np.array(x[1][1][1]).reshape(-1,1)
    verify = np.array(x[1][2][1]).reshape(-1,1)
    relFreq_ver = np.concatenate((relFreq, verify), axis=1)
    p_uni = performTest(relFreq, ratings)
    p_multi = performTest(relFreq_ver, ratings)
    r = np.corrcoef(relFreq.reshape(-1,), ratings.reshape(-1,))[0,1]

    return tuple([x[0], (p_uni, p_multi, r)])

###################################

sc = pyspark.SparkContext()
#Read the file and turn it to dictionary
txt = sc.textFile('Data/Software_5.json').map(lambda x: json.loads(x))
#Filter out the records that don't have reviewText and ratings;followed create such records.
txt = txt.filter(lambda x: (('reviewText' in x) and ('overall' in x) and ('verified' in x))).map(lambda x: (x['overall'], x['reviewText'].lower(), x['verified']))
#Change the reviewText to list of words based on regex pattern match, and filtering the corner case
txt = txt.map(lambda x: (x[0], (re.findall(re_pattern, x[1]), x[2]))).filter(lambda x: len(x[1][0])>0)

#Let's run a word count....
common_words = txt.flatMap(lambda x: list(dict(Counter(x[1][0])).items()))
common_words = common_words.reduceByKey(lambda a, b: a+b).sortBy(lambda x: -x[1]).keys().take(n_words)
#Broadcast common_words for efficiency
#common_words = sc.broadcast(sc.parallelize(common_words))

#txt = txt.map(lambda x: (x[0], (list(set(x[1][0]).intersection(set(common_words))), x[1][1])))


#Turning records into (rating, [(words1, relfreq), (word2, relfreq)]) (sorted based on count)
txt = txt.map(lambda x: (x[0], [(i, [x[1][0].count(i)/len(set(x[1][0]))]) for i in common_words], x[1][1]))
# Records: ( (word1, (('Ratings', [Rating]), ('RelFreq', [ratio]), ('VERIFIED', [Verified]))), (word2, (....)), ... )
txt = txt.flatMap(lambda x: [(x[1][i][0], tuple([('Ratings', [x[0]])] + [('RelFreq', x[1][i][1])] + [('VERIFIED', [int(x[2]==True)])])) for i in range(len(x[1])) ])
txt = txt.reduceByKey(prepareDist)
txt = txt.map(doTest)
without_controls = txt.sortBy(lambda x: x[1][0])
with_controls = txt.sortBy(lambda x: x[1][1][0])

pprint ('Without Controls: +ve')
pprint (without_controls.filter(lambda x: x[1][2]>0).take(10))
pprint ('----------------------')
pprint ('Without Controls: -ve')
pprint (without_controls.filter(lambda x: x[1][2]<0).take(10))
pprint ('----------------------')
pprint ('----------------------')
pprint ('With Controls: +ve')
pprint (with_controls.filter(lambda x: x[1][2]>0).take(10))
pprint ('----------------------')
pprint ('With Controls: -ve')
pprint (with_controls.filter(lambda x: x[1][2]<0).take(10))

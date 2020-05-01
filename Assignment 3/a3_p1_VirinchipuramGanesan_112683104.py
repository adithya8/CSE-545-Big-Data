# author gh: @adithya8
# To add: Normalization before testing, Check p testing logic, Multi variate correction.

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
n_words = 100
# Epsilon 
e = 7./3 - 4./3 -1
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
    def zScore(x):
        return (x - np.mean(x, axis=0))/(np.std(x, axis=0)+e)
    def performTest(x, y):
        x = np.concatenate((x, np.ones((x.shape[0],1)) ), axis=1)
        beta = np.dot(np.dot(np.linalg.inv(np.dot(x.T,x)),x.T),y)
        y_pred = np.dot(x, beta)
        sse = np.sum((y_pred - y) ** 2, axis=0) / float(x.shape[0] - x.shape[1] - 1)
        se = np.array([np.sqrt(sse/(np.sum((x[:, i] - np.mean(x[:, i]))**2)+e) ) for i in range(x.shape[1]) ])
        t = beta / se
        p = 2 * (1 - stats.t.cdf(np.abs(t), x.shape[0] - x.shape[1] - 1))
        return p[:, -1]
    #Reference from https://gist.github.com/brentp/5355925 and slides
    #x: (word, ([Rating1, ...], [ratio1, ...], [Verified1, ...]))
    ratings = np.array(x[1][0]).reshape(-1,1)
    relFreq = np.array(x[1][1]).reshape(-1,1)
    verify = np.array(x[1][2]).reshape(-1,1)
    #Transform to Normal(0,1)
    #ratings, relFreq, verify = zScore(ratings), zScore(relFreq), zScore(verify)
    relFreq_ver = np.concatenate((relFreq, verify), axis=1)
    p_uni = performTest(relFreq, ratings)[0]
    p_multi = performTest(relFreq_ver, ratings)[0]
    r = np.corrcoef(relFreq.reshape(-1,), ratings.reshape(-1,))[0,1]
    return tuple([x[0], (p_uni, p_multi, r)])

###################################

sc = pyspark.SparkContext()
#Read the file and turn it to dictionary
txt = sc.textFile('Data/Software_5.json').flatMap(lambda x: (json.loads(x),))
#Filter out the records that don't have reviewText and ratings;followed create such records.
txt = txt.filter(lambda x: (('reviewText' in x) and ('overall' in x) and ('verified' in x))).flatMap(lambda x: ((x['overall'], x['reviewText'].lower(), x['verified']),) )
#Change the reviewText to list of words based on regex pattern match, and filtering the corner case
txt = txt.flatMap(lambda x: ((x[0], (re.findall(re_pattern, x[1]), x[2])),) ).filter(lambda x: len(x[1][0])>0)

#Let's run a word count....
common_words = txt.flatMap(lambda x: list(map(lambda y: (y,1),  x[1][0])))
common_words = sc.broadcast(common_words.reduceByKey(lambda a, b: a+b).sortBy(lambda x: -x[1]).keys().take(n_words))

#Format: (word, ([rating], [relFreq], [VERIFIED]))
txt = txt.flatMap(lambda x: [(i, ([x[0]], [x[1][0].count(i)/len(set(x[1][0]))], [x[1][1]])) for i in common_words.value])
#Format: (word, ([ratings...], [relFreq...], [VERIFIED...]))
txt = txt.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
txt = txt.map(doTest)

without_controls = txt.sortBy(lambda x: (x[1][0], x[1][2]))
with_controls = txt.sortBy(lambda x: (x[1][1], x[1][2]))

pprint ('Without Controls: +ve')
pprint (without_controls.filter(lambda x: x[1][2]>0).take(10))
pprint ('----------------------')
pprint ('Without Controls: -ve')
pprint (without_controls.filter(lambda x: x[1][2]<0).collect()[-10:])
pprint ('----------------------')
pprint ('----------------------')
pprint ('With Controls: +ve')
pprint (with_controls.filter(lambda x: x[1][2]>0).take(10))
pprint ('----------------------')
pprint ('With Controls: -ve')
pprint (with_controls.filter(lambda x: x[1][2]<0).collect()[-10:])

sc.stop()
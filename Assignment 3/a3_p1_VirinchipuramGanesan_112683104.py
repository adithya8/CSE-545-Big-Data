# author gh: @adithya8
# To add: Normalization before testing, Check p testing logic, Multi variate correction.

###################################
import pyspark
from scipy import stats
import numpy as np

import json
import re
import sys
from pprint import pprint

###################################
# Regex match for pattern
re_pattern =  r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))'
# Top n common words
n_words = 1000
# Epsilon 
e = 7./3 - 4./3 -1
# Seed value for random split
seed = 43
# file_path
input_file = 'Data/Software_5.json' if len(sys.argv)<2 else sys.argv[1]
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
        return (x - np.mean(x, axis=0))/(np.std(x, axis=0))
    def performTest(x, y):
        #x = np.concatenate((x, np.ones((x.shape[0],1)) ), axis=1)
        beta = np.dot(np.dot(np.linalg.inv(np.dot(x.T,x)),x.T),y)
        y_pred = np.dot(x, beta)
        sse = np.sum((y_pred - y) ** 2, axis=0) / float(x.shape[0] - x.shape[1] - 1)
        se = np.sqrt(sse/(np.sum((x[:, 0] - np.mean(x[:, 0]))**2)) ) 
        t = beta / se
        p = stats.t.cdf(t, x.shape[0] - x.shape[1] - 1)*1e3 if(beta[0]<0) else (1 - stats.t.cdf(t, x.shape[0] - x.shape[1] - 1))*1e3
        return p
    #Reference from https://gist.github.com/brentp/5355925 and slides
    x_ = np.array(x[1])
    ratings = zScore(x_[:,0].reshape(-1,1))
    relFreq = zScore(x_[:,1].reshape(-1,1))
    verify = zScore(x_[:,2].reshape(-1,1))
    #Transform to Normal(0,1)
    #ratings, relFreq, verify = zScore(ratings), zScore(relFreq), zScore(verify)
    relFreq_ver = np.concatenate((relFreq, verify), axis=1)
    r = np.corrcoef(relFreq.reshape(-1,), ratings.reshape(-1,))[0,1]
    p_uni = performTest(relFreq, ratings)[0]
    p_multi = performTest(relFreq_ver, ratings)[0]
    return tuple([x[0], (p_uni, p_multi, r)])

###################################

sc = pyspark.SparkContext()
#Read the file and turn it to dictionary
txt = sc.textFile(input_file).flatMap(lambda x: (json.loads(x),))
#Filter out the records that don't have reviewText and ratings;followed create such records.
txt = txt.filter(lambda x: (('reviewText' in x) and ('overall' in x) and ('verified' in x))).flatMap(lambda x: ((x['overall'], x['reviewText'].lower(), x['verified']),) )
#Change the reviewText to list of words based on regex pattern match, and filtering the corner case
txt = txt.flatMap(lambda x: ((x[0], (re.findall(re_pattern, x[1]), x[2])),) ).filter(lambda x: len(x[1][0])>0)

#Let's run a word count....
common_words = txt.flatMap(lambda x: tuple(map(lambda y: (y,1),  x[1][0])))
common_words = common_words.reduceByKey(lambda a, b: a+b)
common_words = sc.broadcast(tuple(map(lambda y: y[0], common_words.takeOrdered(n_words, lambda x: -x[1]))))

#Format: (word, (rating, relFreq, VERIFIED))
txt = txt.flatMap(lambda x: tuple([(i, (x[0], x[1][0].count(i)/len(set(x[1][0])), x[1][1])) for i in common_words.value]))

#Format: (word, [(ratings, relFreq, VERIFIED) ...])
txt = txt.groupByKey().map(lambda x: (x[0], tuple(x[1])))

txt = txt.map(doTest)

positive_corr = txt.filter(lambda x: x[1][2]>0)
negative_corr = txt.filter(lambda x: x[1][2]<0)
#without_controls = txt.takeOrdered(20, lambda x: (x[1][0], x[1][2]))
#with_controls = txt.takeOrdered(20, lambda x: (x[1][1], x[1][2]))

pprint ('Without Controls: +ve')
pprint (positive_corr.takeOrdered(20, lambda x: (-x[1][2], x[1][0])))
pprint ('----------------------')
pprint ('Without Controls: -ve')
pprint (negative_corr.takeOrdered(20, lambda x: (x[1][2], x[1][0])))
pprint ('----------------------')
pprint ('----------------------')
pprint ('With Controls: +ve')
pprint (positive_corr.takeOrdered(20, lambda x: (-x[1][2], x[1][1])))
pprint ('----------------------')
pprint ('With Controls: -ve')
pprint (negative_corr.takeOrdered(20, lambda x: (x[1][2], x[1][1])))

sc.stop()
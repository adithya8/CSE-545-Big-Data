# author gh: @adithya8
# To add: None
###################################
import pyspark
import numpy as np

import json
import sys
from pprint import pprint

###################################
# Enter condition
if len(sys.argv)<3:
    print ('Enter all arguments....')
    sys.exit(0)

###################################
# Min reviewers threshold
min_reviewers = 25
# Min products reviewed threshold
min_products = 5 
# Epsilon 
e = 7./3 - 4./3 -1
# Product asins
product_asins = eval(sys.argv[2])
# Input File path
file_path = sys.argv[1]

###################################

def applyMeanCentering(x):
    # (asin, [(reviewerID, rating), (reviewerID, rating)....])
    meanRating = 0
    #meanRating  = np.mean(np.array(list(x[1]))[:, -1].astype(float)) #Alter appproach
    for i in list(x[1]):
        meanRating += i[1]
    meanRating = meanRating/len(x[1])
    meanCentered = [(i[0], i[1] - meanRating) for i in x[1]]
    return [(x[0], meanCentered)]

###################################
sc = pyspark.SparkContext()
#Broadcasting product_asins
product_asins = sc.broadcast(product_asins)
#Read the file and turn it to dictionary
txt = sc.textFile(file_path).flatMap(lambda x: (json.loads(x),) )
#Filter out the records that don't have necessary fields; followed by creating key val pairs.
txt = txt.filter(lambda x: (('reviewerID' in x) and ('overall' in x) and ('asin' in x) and ('unixReviewTime' in x))).flatMap(lambda x: (((x['asin'], x['reviewerID']), (x['overall'], x['unixReviewTime'])), ) )
#txt = txt.filter(lambda x: (('reviewerID' in x) and ('overall' in x) and ('asin' in x) and ('unixReviewTime' in x))).map(lambda x: ((x['asin'], x['reviewerID']), (x['overall'], x['unixReviewTime'])) )
#Getting the last review per user per product and forming a 'sparse' utility matrix. Format: (asin, (reviewerID, rating))
txt = txt.reduceByKey(lambda x, y: x if(x[1]>y[1]) else y).flatMap(lambda x: ((x[0][0], (x[0][1], x[1][0])),))
#Filtering out products with fewer than 25 unique reviewrs and format to: (reviewerID, (asin, rating))
txt = txt.groupByKey().filter(lambda x: len(x[1])>=min_reviewers).flatMap(lambda x: [(i[0], (x[0], i[1])) for i in list(x[1])] )
#Filtering out users with fewer than 5 unique reviews and turning data to: (asin, (reviewerID, rating))
txt = txt.groupByKey().filter(lambda x: len(x[1])>=min_products).flatMap(lambda x: [(i[0], (x[0], i[1])) for i in list(x[1])])
#Format to: [(asin, [(reviewerID, rating)...]), (asin, [(reviewerID, rating)...])...] 
txt = txt.groupByKey()
#Apply mean centering and turning data to: (reviewerID, (asin, rating))
txt_processed = txt.flatMap(applyMeanCentering).flatMap(lambda x: [(i[0], (x[0], i[1])) for i in list(x[1])])


#Extracting the products we want to find neighbors for
product_asins_ratings = txt_processed.filter(lambda x: x[1][0] in product_asins.value)
product_asins_ratings_static = sc.broadcast(product_asins_ratings.map(lambda x: (x[1][0], x[0])).collect())
#Applying join based on reviewers to perform cosine similarity and then removing unwanted joins; We will need Utility matrix later
sim_search = txt_processed.join(product_asins_ratings).filter(lambda x: x[1][0][0] != x[1][1][0])
#Format to: ((asin1, asin2), (rating1, rating2))
sim_search = sim_search.map(lambda x: ((x[1][0][0], x[1][1][0]), (x[1][0][1], x[1][1][1])) )
#Changing to Commutative + Associative format for faster cmputation. Format: ((asin1, asin2), (rating1*rating2, rating1^2, rating2^2, 1))
sim_search = sim_search.map(lambda x: (x[0], (x[1][0]*x[1][1], x[1][0]**2, x[1][1]**2, 1)) )
#Computing the similarity fo items and dropping items that had fewer than 2 reviewers in common
sim_search = sim_search.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3])).filter(lambda x: x[1][3]>=2).map(lambda x: (x[0], x[1][0]/(np.sqrt(x[1][1]+e)*np.sqrt(x[1][2]+e)), x[1][3]) )
#Filtering positive similarities
sim_search = sim_search.filter(lambda x: x[1]>0)
#Ordering based on decreasing similarity and decreasing number of reviewers in common. Finally making to easy to group by the asin(s) we wanted neighbours for.
sim_search  = sim_search.sortBy(lambda x: (-x[1], -x[2])).map(lambda x: (x[0][0], (x[0][1], x[1], x[2])) if x[0][0] in product_asins.value else (x[0][1], (x[0][0], x[1], x[2])))
#Limit neighbors to 50 or less and format to: [(asin2, asin1, sim)....]. asin1 -> asin in the product_asins
sim_search = sim_search.groupByKey().map(lambda x: (x[0], list(x[1])[:50])).flatMap(lambda x: [(i[0], (x[0], i[1])) for i in x[1]])

#joinging (asin, (reviewerID, rating)) with (asin2, (asin1, sim)) and 
cf = txt.flatMap(lambda x: [(x[0], (i[0], i[1])) for i in x[1]]).join(sim_search)
#Format to: ((asin1, reviewewID), (sim*rating, sim, 1)); [Commutative + Associative]
cf = cf.map(lambda x: ((x[1][1][0], x[1][0][0]), (x[1][1][1]*x[1][0][1], x[1][1][1], 1)) )
#Computing all possible values for the item and filtering out ratings that was computed from fewer than 2 ratings of its neighbors.
cf = cf.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2])).filter(lambda x: x[1][2]>=2).map(lambda x: (x[0], x[1][0]/x[1][1]))

#Removing the ratings that already existed.
cf = cf.filter(lambda x: x[0] not in product_asins_ratings_static.value)
#Combining the new computed ratings with the existing ratings,
cf = cf.map(lambda x: (x[0], int(x[1]*1000)/1000.0)).union(txt.filter(lambda x: x[0] in product_asins.value).flatMap(lambda x: [((x[0], i[0]), i[1]) for i in list(x[1])] ))
#Sorting based on alphabetical order of products, followed by reviewerIDs, followed by decreasing order of ratings
cf = cf.sortBy(lambda x: (x[0], -x[1]))

###################################
#collecting end result
pprint (cf.collect())
#cf.coalesce(1).saveAsTextFile('hdfs:///Output.txt')

###################################
sc.stop()

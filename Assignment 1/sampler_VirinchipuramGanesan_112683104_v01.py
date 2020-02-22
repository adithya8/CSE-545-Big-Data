##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: Adithya, Virinchipuram Ganesan
## Student ID: 112683104

##Data Science Imports: 
import numpy as np
import mmh3

##IO, Process Imports: 
import sys
import timeit
from pprint import pprint


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 0):
    # Implements the standard non-streaming sampling method
    # Step 1: read file to pull out unique user_ids from file
    # Step 2: subset to random  1% of user_ids
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn

    ##<<START>>
    
    #Randomizing the users sampled.
    np.random.seed(np.random.randint(0,1e5))

    mean, standard_deviation = 0.0, 0.0
    lines = filename.readlines()
    sampled_users = set()
    for i in lines:
        user_id = i.strip().split(',')[sample_col]
        sampled_users.add(user_id)
    
    sampled_users = np.random.choice(list(sampled_users), int(percent*len(sampled_users)), replace=False)

    x_sum = 0
    x_sq_sum = 0
    n = 0
    #STD: SQRT(SUM([X-X']^2)/N) = SQRT(SUM(X^2 + [X']^2 - 2X[X']))/SQRT(N)
    #SQRT(SUM(X^2 + [X']^2 - 2X[X']))/SQRT(N) = SQRT(SUM(X^2) + SUM([X']^2) - 2SUM(X)[X']))/SQRT(N)
    #SQRT(SUM(X^2) + (N[X']^2) - 2[X'][X']N))/SQRT(N) = SQRT(SUM(X^2) - (N[X']^2)))/SQRT(N)
    #SQRT: SQRT(SUM([X^2])/N - ([SUM(X)/N]^2)))
    for i in lines:
        data = i.strip().split(',')
        user_id = data[sample_col]
        if(user_id in sampled_users):
            x = float(data[-1])
            x_sum += x
            x_sq_sum += (x*x)
            n += 1
    
    mean = x_sum/n
    standard_deviation = np.sqrt((x_sq_sum/n) - (mean*mean))
    ##<<COMPLETE>>

    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler

def streamSampler(stream, percent = .01, sample_col = 0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    #Turning Percentange into x out of y samples, for example, 0.005 would be 5 out of 1000 samples
    percent_adjust_factor = np.power(10,-np.floor((np.log10(percent))))
    max_allowed_bin = int(percent_adjust_factor*percent)

    seedv = np.random.randint(1,1e5)
    mean, standard_deviation = 0.0, 0.0
    x_sum = 0
    x_sq_sum = 0
    n = 0
    ##<<COMPLETE>>
    for line in stream:
        data = line.strip().split(',')
        user_id = data[sample_col]
        if(mmh3.hash(user_id, seed= seedv, signed=False)%percent_adjust_factor<max_allowed_bin):
            x = float(data[-1])
            x_sum += x
            x_sq_sum += (x*x)
            n += 1
        ##<<COMPLETE>>
    ##<<COMPLETE>>

    mean = x_sum/n
    standard_deviation = np.sqrt((x_sq_sum/n) - (mean*mean))

    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.C Timing

files=['transactions_small.csv', 'transactions_medium.csv', 'transactions_large.csv']
percents=[.02, .005]

if __name__ == "__main__": 

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            if f == 'transactions_large.csv': #large file is located in a different location in my system
                f = '/data/avirinchipur/'+f
            print("\nFile: ", f)
            fstream = open(f, "r")
            start_time = timeit.default_timer()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            compute_time = timeit.default_timer()
            print ("Typical sampler completed in: ",'{:.3f}'.format(compute_time - start_time),".")
            fstream.close()
            fstream = open(f, "r")
            start_time = timeit.default_timer()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            compute_time = timeit.default_timer()
            print ("Stream sampler completed in: ",'{:.3f}'.format(compute_time - start_time),".")
            



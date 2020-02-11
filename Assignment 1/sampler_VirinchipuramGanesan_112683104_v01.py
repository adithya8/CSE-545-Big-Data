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
## Student Name: KEY
## Student ID: 

##Data Science Imports: 
import numpy as np
import math
import mmh3
from random import random


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

    #Turning Percentange into x out of y samples, for example, 0.005 would be 5 out of 1000 samples
    percent_adjust_factor = math.pow(10,-math.floor((math.log10(5e-3))))
    max_allowed_bin = int(percent_adjust_factor*percent)

    mean, standard_deviation = 0.0, 0.0
    lines = filename.readlines()
    sampled_users = set()
    start_time = timeit.default_timer()
    for i in lines:
        user_id = i.strip().split(',')[sample_col]
        if(mmh3.hash(user_id, seed=42, signed=False)%percent_adjust_factor<max_allowed_bin):
            sampled_users.add(user_id)
    
    sampling_time = timeit.default_timer() 
    print ("Number of Sampled users: ", len(sampled_users))
    print ("Finished sampling for ",max_allowed_bin/float(percent_adjust_factor)*100," percent in: ",sampling_time - start_time)
    
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
    
    compute_time = timeit.default_timer()
    print ("Finished computing mean and STD for the undersampled in ",compute_time - sampling_time,".")
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
    percent_adjust_factor = math.pow(10,-math.floor((math.log10(5e-3))))
    max_allowed_bin = int(percent_adjust_factor*percent)

    mean, standard_deviation = 0.0, 0.0
    #sampled_users = set()
    start_time = timeit.default_timer()
    x_sum = 0
    x_sq_sum = 0
    n = 0
    ##<<COMPLETE>>
    for line in stream:
        data = line.strip().split(',')
        user_id = data[sample_col]
        if(mmh3.hash(user_id, seed=42, signed=False)%percent_adjust_factor<max_allowed_bin):
            #sampled_users.add(user_id)
            x = float(data[-1])
            x_sum += x
            x_sq_sum += (x*x)
            n += 1
            #mean = x_sum/n
            #standard_deviation = np.sqrt((x_sq_sum/n) - (mean*mean))
        ##<<COMPLETE>>
    ##<<COMPLETE>>
    compute_time = timeit.default_timer()
    print ("Finished computing mean and STD for the undersampled in ",compute_time - start_time,".")

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
            if f == 'transactions_large.csv':
                continue
            print("\nFile: ", f)
            fstream = open(f, "r")
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            fstream.close()
            fstream = open(f, "r")
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            



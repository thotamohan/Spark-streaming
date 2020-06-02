# Spark-streaming
Bloom filtering, Flajolet Martin algorithm, and reservoir sampling algorithm

# **Introduction:**
In this repository, there is implementation of  three algorithms: 
* Bloom filtering.
* FlajoletMartin algorithm. 
* reservoir sampling. 


# **Programming Requirements:**
* You will need Spark Streaming library for task2- FlajoletMartin Algorithm. You will use Twitter streaming API
  streaming for task3: you can use the Python library, tweepy, and Scala library, sparkstreaming-twitter for this task.
* You can only use Spark RDD and standard Python or Scala libraries except for the ones in
  (b). i.e. no point if using Spark DataFrame or DataSet.

## **Programming Environment:**
Python 3.6, Scala 2.11 and Spark 2.3.2

# **Dataset:**
Yelp Business Data i.e., business_first.json and business_second.json
* For Bloom filtering you need to download the business_first.json and business_second.json from
* The first file is used to set up the bit array for Bloom fitering, and the second file is used for prediction.


# Tasks

## **Task1: Bloom Filtering:**
* I implemented the Bloom Filtering algorithm to estimate whether the city of a business in
business_second.json has shown before in business_first.json. 
* We need to find proper bit array size, hash functions and the number of hash functions in the Bloom Filtering algorithm.
Some possible the hash functions are:

  f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m

  where p is any prime number and m is the length of the filter bit array. You can use any combination for the parameters (a, b, p). The   hash functions should keep the same once you created them.
* Since the city of a business is a string, you need to convert it into an integer and then apply hash functions to it., the following  code shows one possible solution:

import binascii

int(binascii.hexlify(s.encode('utf8')),16)

(We only treat the exact the same strings as the same cities. I did not consider alias. If one record in the business_second.json file does not contain the city field, or the city field is empty, I predicted zero for that record.) 

## **Execution Details:**
the code ran within 60 seconds and it is evaluated on the false positive rate (FPR) and the false negative rate(FNR). 

## **Task2: Flajolet-Martin algorithm**
* In task2, I implement the Flajolet-Martin algorithm (including the step of combining
estimations from groups of hash functions) to estimate the number of unique cities within a
window in the data stream. 
* I found proper hash functions and the proper number of hash functions in the Flajolet-Martin algorithm.


## **Task3: Fixed Size Sampling on Twitter Streaming**
* You will use Twitter API of streaming to implement the fixed size sampling method (Reservoir
Sampling Algorithm) and find popular tags on tweets based on the samples.
* In this task, we assume that the memory can only save 100 tweets, so we need to use the
fixed size sampling method to only keep part of the tweets as a sample in the streaming.
* When the streaming of the Twitter coming, for the first 100 tweets, you can directly save
them in a list. 
* After that, for the nth twitter, you will keep the nth tweet with the probability
of 100/n, otherwise discard it. 
* If you keep the nth tweet, you need to randomly pick one in the list to be replaced. If the coming tweet has no tag, you can directly ignore it.
* You also need to keep a global variable representing the sequence number of the tweet. If the coming tweet has no tag, the sequence number will not increase, otherwise the sequence number increases by one.
* Every time you receive a new tweet, you need to find the tags in the sample list with the top 3 frequencies. 
* All the results are printed in a csv file.



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

where p is any prime number and m is the length of the filter bit array. You can use any combination for the parameters (a, b, p). The hash functions should keep the same once you created them.
* Since the city of a business is a string, you need to convert it into an integer and then apply hash functions to it., the following code shows one possible solution:

import binascii

int(binascii.hexlify(s.encode('utf8')),16)

(We only treat the exact the same strings as the same cities. I did not consider alias. If one record in the business_second.json file does not contain the city field, or the city field is empty, I predicted zero for that record.) 

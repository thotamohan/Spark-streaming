import random
import binascii
from statistics import mean,median
from datetime import datetime
import findspark
findspark.init()
findspark.find()
import pyspark
import random
import json
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

def Prime_check(num):
    if num<=1:
        return False
    if num<=3:
        return True
    if num%2==0 or num%3==0:
        return False
    k = 5
    while k*k <= num:
        if (num%k == 0 or num%(k+2)==0):
            return False
        k=k + 6
    return True



def hasNumbers(inputString):
    for char in inputString:
        if char.isdigit() or inputString=='':
            return True
    if inputString=='':
        return True
    else:
        return False
    


def hashed_values2(hashfunction,conv_value,m):
    calc=((((hashfunction[0]*conv_value)+hashfunction[1])%hashfunction[2])%m)
    return calc


hash_count=9
random.seed(9001)
a = random.choices([x for x in range(1000, 30000) if Prime_check(x)], k=hash_count+1)
b = random.choices([x for x in range(1000, 30000) if Prime_check(x)], k=hash_count+1)  
generated_prime = random.choices([x for x in range(1000000000, 1000000100) if Prime_check(x)],k=hash_count+1)
hashed_list=[]
for points in zip(a,b,generated_prime):
    hashed_list.append([points[0],points[1],points[2]])
    


def Flajolet_Martin(stream):
    sizeGroup=3
    numHashes=9
    city_list=stream.collect()
    #print(len(city_list))
    m=2**(numHashes)
    true_value=len(set(city_list))
    #print(true_value)
    global hashed_list
    global outputFile
    
    L=[]
    for hashes in hashed_list:
        max_value=-1
        for cities in city_list:
            v=hasNumbers(cities)
            if v:
                pass
            else:
                hashing=int(binascii.hexlify(cities.encode('utf8')), 16)
                #print(hashing)
                hashed_value=hashed_values2(hashes,hashing,m)
                #print(hashed_value)
                #print(hashed_value)
                hashed_value=bin(hashed_value)[2:]
                length=len(hashed_value)-len(hashed_value.rstrip('0'))
                if (length > max_value):
                    max_value = length
                #tail_zero.append(length)
        #max_value=max(tail_zero)
        L.append(2**max_value) 
    Index_start=0
    groupAvgs=[]    
    for end_Index in range(sizeGroup, numHashes, sizeGroup):
        groupAvgs.append(mean(L[Index_start:end_Index]))
        Index_start=end_Index
    estimated_value=median(groupAvgs)
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out = str(current_timestamp) + "," + str(true_value) + "," + str(estimated_value) + "\n"
    outputFile.write(out)
    outputFile.flush()
    return



if __name__ == "__main__":
    print('Mohan')
    port=int(sys.argv[1])
    output_file_path=sys.argv[2]

    batch_size=5 
    

    sc = SparkContext('local[*]','test')
    sc.setLogLevel("OFF")
    ssc = StreamingContext(sc, batch_size)
    dataRDD = ssc.socketTextStream("localhost", port)


    outputFile = open(output_file_path, "w", encoding="utf-8")
    out="Time,Ground Truth,Estimation"+"\n"
    outputFile.write(out)

    
    business_rdd=dataRDD.map(lambda x:json.loads(x))
    city_rdd=business_rdd.map(lambda x:x['city'])
    city_list=city_rdd.window(30, 10).foreachRDD(Flajolet_Martin)
    
    
    ssc.start()
    ssc.awaitTermination()




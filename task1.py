import findspark
findspark.init()
findspark.find()
import pyspark
import random
import json
import binascii
import sys
import csv


def hasNumbers(inputString):
    for char in inputString:
        if char.isdigit() or inputString=='':
            return True
    if inputString=='':
        return True
    else:
        return False
    
def hashed_value(hashfunction,conv_value,p,m):
    v=[]
    for values in hashfunction:
        calc=((((values[0]*conv_value)+values[1])%p)%m)
        v.append(calc)
    return v

def bloom_filtering(list_cities,count_cities,fp,array_pred,visited_cities,p):
    i=0
    fn=0
    for cities in list_cities:
        v=hasNumbers(cities)
        if v:
            pass
        else:
            count_cities+=1
            converted_city=int(binascii.hexlify(cities.encode('utf8')), 16)
            hashed_values=hashed_value(hashfunction,converted_city,p,m)
            new_city=False
            for values in hashed_values:
                if array_pred[values]==0:
                    array_pred[values]=1
                    new_city=True
            if cities not in visited_cities and not new_city:
                fp+=1
            if cities in visited_cities and not new_city:
                fn+=1
            visited_cities.add(cities)
    return fp,fn,count_cities,array_pred


def prediction(list_cities,array_pred,p,m):
    dict1={}
    L=[]
    for cities in list_cities:
        v=hasNumbers(cities)
        if v:
            dict1[cities]=0
            L.append(0)
        else:
            converted_city=int(binascii.hexlify(cities.encode('utf8')), 16)
            hashed_values=hashed_value(hashfunction,converted_city,p,m)
            l=[]
            for values in hashed_values:
                if array_pred[values]==1:
                    l.append(1)
                else:
                    l.append(0)
            if 0 in l:
                dict1[cities]=0
                L.append(0)
            else:
                dict1[cities]=1
                L.append(1)
    return L





if __name__ == "__main__":
    
    input_file_path=sys.argv[1]
    prediction_file=sys.argv[2]
    output_file_path=sys.argv[3]
    
    from pyspark import SparkContext, SparkConf
    sc = SparkContext('local[*]','test')
    
    Num_hash =6
    hashfunction=[]
    random.seed(10000)
    for i in range(Num_hash):  # generate hash functions
        hashfunction.append([random.randint(0, 10000), random.randint(0, 10000)])


    p=6977
    m=7000

    array_pred=[0]*m
    visited_cities=set()
    false_positives=0
    count_cities=0
    reviews = sc.textFile(input_file_path).persist()
    rdd=reviews.map(lambda x:json.loads(x))
    city_list=rdd.map(lambda x:x['city']).collect()
    nn=bloom_filtering(list(set(city_list)),count_cities,false_positives,array_pred,visited_cities,p)
    
    second = sc.textFile(prediction_file).persist()
    second_rdd=second.map(lambda x:json.loads(x))
    pred_list=second_rdd.map(lambda x:x['city']).collect()
    print(nn[0],' ',nn[1],' ',nn[2])
    prediction_list=prediction(pred_list,nn[3],p,m)
    kk=(" ".join(str(x) for x in prediction_list))
    with open(output_file_path,'w') as f:
        for items in prediction_list:
            f.write(str(items))
            f.write(' ')
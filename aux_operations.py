import glob
import os
import pandas as pd
from csv import reader	

#print amount of each value in l
def count(l, unique_l):
    results = {}
    for i in unique_l:
        results[i] = l.count(i) 
    #print(results)
    print(sorted(results.items(), key = lambda kv:(kv[1], kv[0]))) 
    
#return unique values from l    
def unique(l): 
  
    unique_l = [] 
      
    for x in l:
        if x not in unique_l: 
            unique_l.append(x) 
    #print(len(unique_l))
    return (unique_l)

def get_data(fname, l):
    
    
    dados = pd.read_csv(fname, quotechar='"', sep=",", header=0)
    #print(dados)
    array = dados.idpessoa
    for n in array:
        l.append(n)
     
#count the biggest sequence of contributed months, considering each user
def read_csv():
    count = 0
    maxi = 0
    array = []
    # open file in read mode
    with open('/home/tales/.config/spyder-py3/csv/unique.csv', 'r') as read_obj:
        csv_reader = reader(read_obj, delimiter = ",")
        header = next(csv_reader)
        for row in csv_reader:
            for column in row:
                if(column=='' or count==18):
                    count = 0                
                else:
                    count = count + 1
                    if maxi < count:
                        maxi = count
            array.append(maxi)
            maxi = 0
    return (array)

#count all sequences of contributed months, considering each user
def read_csv2():
    count = 0
    array = []
    start = 0
    # open file in read mode
    with open('/home/tales/.config/spyder-py3/csv/unique.csv', 'r') as read_obj:
        csv_reader = reader(read_obj, delimiter = ",")
        header = next(csv_reader)
        for row in csv_reader:
            for column in row:
                if(column!='' and count==18):
                    array.append(count)
                    count = 0  
                    start = 0
                elif(column!=''):
                    start = 1
                    count = count + 1
                elif((column=='' or count==18) and start == 1):
                    array.append(count)
                    count = 0  
                    start = 0
    return (array)

#count the biggest sequence of idle months between contributions periods, considering each user
def read_csv3():
    count = 0
    maxi = 0
    flag = 0
    prox = 0
    array = []
    # open file in read mode
    with open('/home/tales/.config/spyder-py3/csv/unique.csv', 'r') as read_obj:
        csv_reader = reader(read_obj, delimiter = ",")
        header = next(csv_reader)
        for row in csv_reader:
            flag = 0
            prox = 0
            coun = 0
            for column in row:
                if(column!='' and prox==1):
                    if (maxi < count):
                        maxi = count
                    prox = 0
                    flag = 1
                    count = 0
                elif(column=='' and flag==1):
                    prox = 1
                    count = count + 1
                elif(column!=''):
                    count = 0 
                    flag = 1
            array.append(maxi)
            maxi = 0
    return (array)

#count all sequences of idle months between contribution periods, considering each user
def read_csv4():
    count = 0
    flag = 0
    prox = 0
    array = []
    # open file in read mode
    with open('/home/tales/.config/spyder-py3/csv/unique.csv', 'r') as read_obj:
        csv_reader = reader(read_obj, delimiter = ",")
        header = next(csv_reader)
        for row in csv_reader:
            flag = 0
            prox = 0
            count = 0
            for column in row:
                if(column!='' and prox==1):
                    array.append(count)
                    prox = 0
                    flag = 1
                    count = 0
                elif(column=='' and flag==1):
                    prox = 1
                    count = count + 1
                elif(column!=''):
                    count = 0 
                    flag = 1
    return (array)

def main():
    array = []
    array = read_csv3()
    #print(array)
    unique_array = unique(array) 
    count(array, unique_array)
    
    """
    os.chdir('/home/tales/.config/spyder-py3/csv2/')
    l = []
    for fname in glob.glob("*.csv"):
        get_data(fname, l)
    #print(len(l))
    unique_l = []
    unique_l = unique(l)
    count(l,unique_l)
    """

if __name__=="__main__":
    main()
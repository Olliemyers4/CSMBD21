import re
import multiprocessing as mp

def shuffle(mapperOutput):
    data = {}

    # remove None/Empty values from the list of items to process
    mapperOutput = list(filter(None, mapperOutput)) 

    for key,value in mapperOutput:
        if key not in data:
            # if the dict doesnt have the key in it, make a new key with the value
            data[key] = [value]
        else:
            # if the key is already in the dict, append the value to the key - list of values
            data[key].append(value)
    return data

def mapper(x):
    cols = x.split('\t') # this is for tab separated files
    if re.match('^\\d{1,9}$', cols[4]): # number that is 1-9 digits long
        return (cols[2], int(cols[4]))
    
def reducer(keyValuesTuple):
    key,values = keyValuesTuple # unpack the tuple into key and values
    return (key, sum(values))

mapInput = []

if __name__ == '__main__':

    with open('City-simple.tsv',encoding='utf-8') as f: # load the tsv -> will need to be switched to the required csv
        mapInput = f.readlines()
    
    cpus = mp.cpu_count()

    with mp.Pool(processes=cpus) as pool:
        mapOutput = pool.map(mapper, mapInput,chunksize=int(len(mapInput)/cpus)) # Map
        reduceInput = shuffle(mapOutput) # Shuffle
        reduceOutput = pool.map(reducer, reduceInput.items(),chunksize=int(len(reduceInput.keys())/cpus)) # Reduce
        print(reduceOutput)
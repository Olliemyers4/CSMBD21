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
    cols = x.split(',') # this is for comma separated files
    if re.match('^[A-Z]{3}\\d{4}[A-Z]{2}\\d{1}$', cols[0]): # Passenger id Format: 𝑋𝑋𝑋𝑛𝑛𝑛𝑛𝑋𝑋𝑛
        return (cols[0], int(1)) # return the passenger id and 1 (for counting)
    
def reducer(keyValuesTuple):
    key,values = keyValuesTuple # unpack the tuple into key and values
    return (key, sum(values))

mapInput = []

if __name__ == '__main__':

    with open('AComp_Passenger_data_no_error.csv','r',encoding='utf-8') as f: # load the tsv -> will need to be switched to the required csv
        mapInput = f.readlines()
    
    cpus = mp.cpu_count()

    with mp.Pool(processes=cpus) as pool:
        mapOutput = pool.map(mapper, mapInput,chunksize=int(len(mapInput)/cpus)) # Map
        reduceInput = shuffle(mapOutput) # Shuffle
        reduceOutput = pool.map(reducer, reduceInput.items(),chunksize=int(len(reduceInput.keys())/cpus)) # Reduce
        with open('output.csv','w',encoding='utf-8') as f:
            for item in reduceOutput:
                f.write(str(item[0]) + ',' + str(item[1]) + '\n')
import re
import multiprocessing as mp

def shuffle(mapper_out):
    data = {}
    mapper_out = list(filter(None, mapper_out))

    for k,v in mapper_out:
        if k not in data:
            data[k] = [v]
        else:
            data[k].append(v)
    return data

def up_map(x):
    cols = x.split('\t')
    if re.match('^\\d{1,9}$', cols[4]):
        return (cols[2], int(cols[4]))
    
def up_reduce(z):
    k,v = z
    return (k, sum(v))

map_in = []

if __name__ == '__main__':

    with open('City-simple.tsv',encoding='utf-8') as f:
        map_in = f.read().splitlines()
    
    with mp.Pool(processes=mp.cpu_count()) as pool:
        map_out = pool.map(up_map, map_in,chunksize=int(len(map_in)/mp.cpu_count()))
        reduce_in = shuffle(map_out)
        reduce_out = pool.map(up_reduce, reduce_in.items(),chunksize=int(len(reduce_in.keys())/mp.cpu_count()))
        print(reduce_out)
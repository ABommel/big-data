from concurrent.futures import ProcessPoolExecutor
from time import sleep, time

def f(x):
    sleep(1)
    return x*x

L = list(range(8))

if __name__ == '__main__':
    
    begin = time()
    with ProcessPoolExecutor() as pool:

        result = sum(pool.map(f, L))
    end = time()
    
    print(f"result = {result} and time = {end-begin}")

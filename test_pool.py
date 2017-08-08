import multiprocessing

def f(name):
    print 'hello', name

if __name__ == '__main__':
    pool = multiprocessing.Pool() #use all available cores, otherwise specify the number you want as an argument
    for i in xrange(0, 512):
        pool.apply_async(f, args=(i,))
    pool.close()
    pool.join()

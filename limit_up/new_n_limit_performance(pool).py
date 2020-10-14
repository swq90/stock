from multiprocessing import Pool
import os, time, random

def long_time_task(name,age):
    print('Run task %s (%s)...' % (name, os.getpid()))
    start = time.time()
    time.sleep(random.random() * 3)
    print('----'*age)
    end = time.time()
    print('Task %s runs %0.2f seconds.' % (name, (end - start)))

if __name__=='__main__':
    print('Parent process %s.' % os.getpid())
    p = Pool(4)
    # constraint condition
    code_condition={'a':1,'b':3}

    for code,condition in code_condition.items():
        p.apply_async(long_time_task, args=(code,condition))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
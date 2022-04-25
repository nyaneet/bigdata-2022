import math
import mmap
import time
import os

from concurrent.futures import ThreadPoolExecutor

DATA_FILE_NAME = 'data.txt'
NUMBER_BITS = 32

def process_data_sequentially():
    number_bytes = int(NUMBER_BITS / 8)
    total_sum = 0
    total_min = 2**NUMBER_BITS
    total_max = 0

    with open(DATA_FILE_NAME, 'rb') as f:
        bytes = f.read(number_bytes)
        while bytes:
            uint = int.from_bytes(bytes, byteorder='big')
            total_sum += uint
            if uint > total_max:
                total_max = uint
            if uint < total_min:
                total_min = uint
            bytes = f.read(number_bytes)
    
    return total_sum, total_min, total_max

def process_data_part(length, offset):
    number_bytes = int(NUMBER_BITS / 8)
    part_sum = 0
    part_min = 2**NUMBER_BITS
    part_max = 0

    with open(DATA_FILE_NAME, 'rb') as f:
        with mmap.mmap(f.fileno(), length=length, offset=offset, access=mmap.ACCESS_READ) as mmap_obj:
            bytes = mmap_obj.read(number_bytes)
            while bytes:
                uint = int.from_bytes(bytes, byteorder='big')
                part_sum += uint
                if uint > part_max:
                    part_max = uint
                if uint < part_min:
                    part_min = uint
                bytes = mmap_obj.read(number_bytes)

    return part_sum, part_min, part_max

def process_data_concurrency():
    number_bytes = int(NUMBER_BITS / 8)
    total_bytes = os.path.getsize(DATA_FILE_NAME)

    max_workers = min(32, (os.cpu_count() or 1) + 4)
    min_part_bytes = mmap.ALLOCATIONGRANULARITY
    total_min_parts = math.ceil(total_bytes / min_part_bytes)
    worker_parts_num = math.ceil(total_min_parts / max_workers)
    worker_part_bytes = worker_parts_num * min_part_bytes

    lengths = []
    offsets = []

    for i in range(max_workers):
        length = worker_part_bytes
        offset = i * worker_part_bytes
        if length + offset > total_bytes:
            length = total_bytes % worker_part_bytes
        lengths.append(length)
        offsets.append(offset)

    total_sum = 0
    total_min = 2**NUMBER_BITS
    total_max = 0    

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(process_data_part, lengths, offsets):
            part_sum, part_min, part_max = result
            total_sum += part_sum
            if part_max > total_max:
                total_max = part_max
            if part_min < total_min:
                total_min = part_min
    
    return total_sum, total_min, total_max

if __name__ == '__main__':
    # Process data sequentially
    start_time = time.time()
    sum_, min_, max_ = process_data_sequentially()
    end_time = time.time()
    print('Sequentially')
    print(f'Elapsed time: {int(end_time - start_time)}s \nSum: {sum_} \nMin: {min_} \nMax: {max_}')

    # Sequentially
    # Elapsed time: 153s 
    # Sum: 1073764728450057022 
    # Min: 5 
    # Max: 4294967295

    # Process data concurrency
    start_time = time.time()
    sum_, min_, max_ = process_data_concurrency()
    end_time = time.time()
    print('Concurrency')
    print(f'Elapsed time: {int(end_time - start_time)}s \nSum: {sum_} \nMin: {min_} \nMax: {max_}')

    # Concurrency
    # Elapsed time: 170s 
    # Sum: 1073764728450057022 
    # Min: 5 
    # Max: 4294967295

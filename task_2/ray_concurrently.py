import time
import ray

@ray.remote(num_cpus=1)
def get_factors_number(number):
    factors_number = 0
    factor = 2
    while factor * factor <= number:
        if number % factor == 0:
            factors_number += 1
            number /= factor
        else:
            factor += 1
    if number > 1:
        factors_number += 1
    return factors_number

if __name__ == '__main__':
    ray.init()

    start_time = time.time()
    total_factors = 0

    with open('data.txt', 'r') as f:
        refs = []
        while True:
            line = f.readline()
            if not line:
                break
            refs.append(get_factors_number.remote(int(line)))
        for ref in refs:
            total_factors += ray.get(ref)

    print(f'Factors number: {total_factors}')
    print(f'Elapsed time: {int((time.time() - start_time)*1000)}ms')
    # Factors number: 8240
    # Elapsed time: 756ms

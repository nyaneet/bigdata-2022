import random

TOTAL_NUMBERS = 2000
NUMBER_BITS = 32


def fill_file(file_name):
    with open(file_name, 'w') as f:
        for _ in range(TOTAL_NUMBERS):
            f.write(str(random.randint(0, 2**NUMBER_BITS)) + '\n')


if __name__ == '__main__':
    fill_file('data.txt')

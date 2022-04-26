import random

def fill_file(file_name, file_size_gigabytes = 2, number_bits = 32):
    number_bytes = int(number_bits / 8)
    total_bytes = file_size_gigabytes * 1e9
    total_numbers = int(total_bytes / number_bytes) 

    # for progress
    update_every = int(total_numbers / 100)

    with open(file_name, 'wb') as file:
        for i in range(total_numbers):
            file.write(random.randint(0, 2**8).to_bytes(number_bytes, byteorder='big'))

            # for progress
            if (i + 1) % update_every == 0:
                print(f'\rProgress: {100 * (i + 1) / total_numbers}%', end='')

    print('\nDone.')

if __name__ == '__main__':
    fill_file('bin_data')
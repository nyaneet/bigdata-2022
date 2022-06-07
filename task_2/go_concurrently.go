package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

func factor_len(number int) int {
	len_factors := 0
	for factor := 2; factor*factor <= number; factor = factor + 1 {
		for number%factor == 0 {
			len_factors = len_factors + 1
			number = number / factor
		}
	}
	if number > 1 {
		len_factors = len_factors + 1
	}
	return len_factors
}

func main() {
	start := time.Now()
	results := make(chan int)
	total_numbers := 0
	total_factors := 0

	file, _ := os.Open("data.txt")
	scanner := bufio.NewScanner(file)
	defer file.Close()

	for scanner.Scan() {
		number, _ := strconv.Atoi(scanner.Text())
		go func(ch chan int) {
			ch <- factor_len(number)
		}(results)
		total_numbers = total_numbers + 1
	}

	for i := 0; i < total_numbers; i++ {
		value := <-results
		total_factors = total_factors + value
	}

	fmt.Printf("Factors number: %d\n", total_factors)
	fmt.Printf("Elapsed time: %s\n", time.Since(start))
	// Factors number: 8240
	// Elapsed time: 9.8753ms
}

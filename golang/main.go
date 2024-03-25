package main

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MaxFloat32 = float64(1<<24 - 1)
	MinFloat32 = -float64(1<<24 - 1)
)

type Record struct {
	city        string
	temperature float32
}

func NewRecord(city string, temperature float32) Record {
	return Record{
		city:        city,
		temperature: temperature,
	}
}

type Stats struct {
	min   float32
	max   float32
	sum   float32
	count float32
}

func DefaultStats() *Stats {
	return &Stats{
		min:   float32(MaxFloat32),
		max:   float32(MinFloat32),
		sum:   0.0,
		count: 0.0,
	}
}

func (s *Stats) AddTemperature(temp float32) *Stats {
	s.max = float32(math.Max(float64(s.max), float64(temp)))
	s.min = float32(math.Min(float64(s.min), float64(temp)))
	s.sum += temp
	s.count += 1
	return s
}

func processLine(line string) (Record, error) {
	infos := strings.Split(line, ";")
	city := infos[0]
	temp, err := strconv.ParseFloat(infos[1], 32)
	if err != nil {
		return Record{}, err
	}

	return Record{
		city:        city,
		temperature: float32(temp),
	}, nil
}

func reader(ctx context.Context, file *os.File, batchSize int) <-chan []string {
	out := make(chan []string)
	scanner := bufio.NewScanner(file)
	batch := make([]string, batchSize)

	go func() {
		defer close(out)
		index := 0
		count := 0 

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
				line := scanner.Text()
				batch[index] = line
				index++
				if index == batchSize {
					out <- batch
					count++
					fmt.Println("batch count: ", count)
					batch = make([]string, batchSize)
					index = 0
				}
			}

		}
	}()
	return out
}

func worker(ctx context.Context, in <-chan []string) <-chan Record {
	out := make(chan Record)
	go func() {
		defer close(out)
		for sub := range in {
			for _, line := range sub {
				select {
				case <-ctx.Done():
					return
				default:
					record, err := processLine(line)
					if err != nil {
						fmt.Println(err)
						continue
					}
					out <- record
				}
			}
		}
	}()
	return out
}

func combiner(ctx context.Context, inputs ...<-chan Record) map[string]*Stats {
	result := make(map[string]*Stats)
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(len(inputs))

	multiplexer := func(in <-chan Record) {
		defer wg.Done()

		for record := range in {
			select {
			case <-ctx.Done():
			default:
				mu.Lock()
				stats, ok := result[record.city]
				if !ok {
					result[record.city] = DefaultStats().AddTemperature(record.temperature)
				} else {
					stats.AddTemperature(record.temperature)
				}
				mu.Unlock()
			}

		}
	}

	for _, in := range inputs {
		go multiplexer(in)
	}
	wg.Wait()

	return result
}

func main() {

	start := time.Now()

	file := "../measurements.txt"
	numWorkers := 12
	batchSize := 1000
	processFile(file, numWorkers, batchSize)
	elapsed := time.Since(start)
	fmt.Printf("Execution time: %s\n", elapsed)

}

func processFile(file string, numWorkers, batchSize int) {

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := reader(ctx, f, batchSize)

	workersCh := make([]<-chan Record, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workersCh[i] = worker(ctx, out)
	}


	res := combiner(ctx, workersCh...)

	city_list := make([]string, 0, len(res))
	for key := range res {
		city_list = append(city_list, key)
	}
	sort.Strings(city_list)

	for _, city := range city_list {
		stats := res[city]
		mean := stats.sum / stats.count
		fmt.Printf("%s: %.2f/%.2f/%.2f \n", city, stats.min, mean, stats.max)

	}

}

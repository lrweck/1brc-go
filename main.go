package main

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	//	"runtime"
	//"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	KiloByte = 1000
	KibiByte = 1024
	MegaByte = 1000 * KiloByte
	MebiByte = 1024 * KibiByte
	GigaByte = 1000 * MegaByte
	GibiByte = 1024 * MebiByte
)

type RunResult struct {
	elapsed    time.Duration
	bufferSize int64
	consumers  int
}

func main() {

	// cpuprof, err := os.Create("./profiles/cpu_profile.prof")
	// if err != nil {
	// 	panic(err)
	// }
	// defer cpuprof.Close()

	// if err := pprof.StartCPUProfile(cpuprof); err != nil {
	// 	panic(err)
	// }
	// defer pprof.StopCPUProfile()

	start := time.Now()

	buffs := []int64{8 * MebiByte}
	consumers := []int{12}
	runsPerConfig := 1

	results := make(map[string]*RunResult, len(buffs)*len(consumers)*runsPerConfig)

	filename := "../1brc/measurements_1b.txt"

	//consumerLoop:
	for _, cons := range consumers {
		for _, buff := range buffs {
			for i := 0; i < runsPerConfig; i++ {

				start := time.Now()
				restring := process(filename, buff, cons)
				elapsed := time.Since(start)
				fmt.Println(restring)
				//break consumerLoop

				fmt.Printf("Consumers: %d, Buffsize: %d, Time taken: %s\n", cons, buff, elapsed)

				stringKey := fmt.Sprintf("Consumers:%d_Buffsize:%d", cons, buff)
				res, ok := results[stringKey]
				if !ok {
					results[stringKey] = &RunResult{
						elapsed:    elapsed,
						bufferSize: buff,
						consumers:  cons,
					}
					continue
				}

				res.elapsed += elapsed

			}
		}
	}

	elapsed := time.Since(start)
	fmt.Println("Time taken to read the file: ", elapsed)

	res := findFastestRun(results)
	if res == nil {
		fmt.Println("No results")
		return

	}

	fmt.Printf("Fastest run: Consumers: %d, Buffsize: %d, Time taken: %s\n", res.consumers, res.bufferSize, res.elapsed/time.Duration(runsPerConfig))

}

func findFastestRun(results map[string]*RunResult) *RunResult {
	var fastest *RunResult
	for _, res := range results {
		if fastest == nil {
			fastest = res
			continue
		}

		if res.elapsed < fastest.elapsed {
			fastest = res
		}
	}

	return fastest
}

const (
	BUFF_SIZE    = 1 << 19
	CONSUMERS    = 6
	AVG_ROW_SIZE = 14
)

var (
	lineSep = []byte("\r\n")
)

func process(filename string, buffSize int64, consumers int) string {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	consumerQueue := make(chan []byte, 100)
	resultsQueue := make(chan map[uint64]*Measurament, 10)

	wgConsumers := &sync.WaitGroup{}
	wgConsumers.Add(consumers)

	for i := 0; i < consumers; i++ {
		go func() {
			defer wgConsumers.Done()
			consumer2(consumerQueue, resultsQueue)
		}()
	}

	chanResult := make(chan string, 1)
	go func() {
		chanResult <- processResults(resultsQueue)
		close(chanResult)
	}()

	buff := make([]byte, buffSize)
	lastRemains := make([]byte, 100)

	for {
		n, err := f.Read(buff)

		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		lastRemains = processBuffer(buff[:n], lastRemains, consumerQueue)

	}

	close(consumerQueue)
	wgConsumers.Wait()
	close(resultsQueue)

	return <-chanResult

}

func processBuffer(buff, remaining []byte, ch chan []byte) []byte {

	before, after, _ := bytes.Cut(buff, lineSep)

	if len(remaining) > 0 {
		// prepend last remains to the new chunk
		before = append(remaining, before...)
		remaining = nil
	}

	if len(after) > 0 {
		remaining = make([]byte, len(after))
		copy(remaining, after)
	}

	buftosend := make([]byte, len(before))
	copy(buftosend, before)
	ch <- buftosend
	return remaining
}

type Measurament struct {
	Name  []byte
	Min   int
	Max   int
	Sum   int
	Count int
}

func (m *Measurament) String() string {
	return fmt.Sprintf("{Min: %d, Max: %d, Sum: %d, Count: %d}", m.Min, m.Max, m.Sum, m.Count)
}

func consumer2(
	input chan []byte,
	output chan map[uint64]*Measurament,
) {

	hh := fnv.New64a()

	for v := range input {
		output <- parseChunk(hh, v)
	}

}

func fnv32(key []byte) uint64 {
	hash := uint64(2166136261)
	const prime32 = uint64(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint64(key[i])
	}
	return hash
}

func NewMeasurament(name []byte, value int) *Measurament {
	return &Measurament{
		Name:  name,
		Min:   value,
		Max:   value,
		Sum:   value,
		Count: 1,
	}

}

func parseChunk(_ hash.Hash64, chunk []byte) map[uint64]*Measurament {

	results := make(map[uint64]*Measurament, 412)

	var start int
	var cityName []byte
	var cityValue int

	_ = chunk[len(chunk)-1] // avoid bounds check error

	for i := 3; i < len(chunk); i++ {
		b := chunk[i]
		switch b {
		case ';':
			cityName = chunk[start:i]
			start = i + 1
			i += 3 // at least 3 digits for value
		case '\n':
			if i-start > 1 {

				cityValue = parseFloatBytesAsInt(chunk[start : i-1])
				start = i + 1
				i += 4 //advance at least 4 bytes, bc thats the smallest name

				hashed := fnv32(cityName)

				found, ok := results[hashed]
				if !ok {
					results[hashed] = NewMeasurament(cityName, cityValue)
					continue
				}

				found.Count++
				found.Sum += cityValue

				found.Min = min(found.Min, cityValue)
				found.Max = max(found.Max, cityValue)

			}
		}
	}

	return results
}

func parseFloatBytesAsInt(data []byte) int {

	negative := data[0] == '-'
	if negative {
		data = data[1:]
	}

	var result int
	switch len(data) {
	// 1.2
	case 3:
		result = int(data[0])*10 + int(data[2]) - '0'*(10+1)
	// 12.3
	case 4:
		result = int(data[0])*100 + int(data[1])*10 + int(data[3]) - '0'*(100+10+1)
	}

	if negative {
		return -result
	}
	return result
}

func processResults(output chan map[uint64]*Measurament) string {

	results := make(map[uint64]*Measurament, 415)

	for sliceOfMap := range output {

		//fmt.Printf("received map of size %d\n", len(sliceOfMap))

		for k, v := range sliceOfMap {

			found, ok := results[k]

			if !ok {
				results[k] = v
				continue
			}

			found.Count += v.Count
			found.Sum += v.Sum

			found.Min = min(found.Min, v.Min)
			found.Max = max(found.Max, v.Max)

		}
	}

	// fmt.Printf("Results: %v\n", results)

	slice := make([]string, 0, len(results))

	for _, v := range results {
		slice = append(slice, stationResultString(string(v.Name), v))
	}

	slices.SortFunc(slice, func(a, b string) int {
		a, _, _ = strings.Cut(a, "=")
		b, _, _ = strings.Cut(b, "=")
		return strings.Compare(a, b)
	})

	sb := strings.Builder{}

	sb.Grow(10660)

	sb.WriteString("{")
	for i, res := range slice {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(res)
	}

	sb.WriteString("}")

	ress := sb.String()
	println(len(ress))

	return ress
}

func stationResultString(name string, m *Measurament) string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, float64(m.Min)/10, (float64(m.Sum)/float64(m.Count))/10, float64(m.Max)/10)
}

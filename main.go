package main

import (
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	BUFF_SIZE    = 1 << 19
	CONSUMERS    = 6
	AVG_ROW_SIZE = 10
)

var (
	lineSep = byte('\r')
)

func process(buffSize int64, consumers int) {
	f, err := os.Open("../1brc/measurements_100m.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fi, _ := f.Stat()

	avgSizePerConsumer := fi.Size() / int64(consumers)
	consumerPreallocSize := avgSizePerConsumer / AVG_ROW_SIZE

	consumerQueue := make(chan []byte, 1000)
	resultsQueue := make(chan []map[uint64]*Measurament, 1000)

	wgConsumers := &sync.WaitGroup{}
	wgConsumers.Add(consumers)

	for i := 0; i < consumers; i++ {
		go func() {
			defer wgConsumers.Done()
			consumer1(consumerPreallocSize, consumerQueue, resultsQueue)
		}()
	}

	chanResult := make(chan string)
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

	<-chanResult

}

func main() {

	cpuprof, err := os.Create("cpu_profile.prof")
	if err != nil {
		panic(err)
	}
	defer cpuprof.Close()

	if err := pprof.StartCPUProfile(cpuprof); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	start := time.Now()

	buffs := []int64{19, 20, 21, 22, 23}
	cons := []int{4, 5, 6, 7, 8, 9, 10}

	for _, b := range cons {

		for _, c := range buffs {
			start := time.Now()
			buffSize := int64(1) << c
			process(buffSize, b)
			elapsed := time.Since(start)
			fmt.Printf("Consumers: %d, Buffsize: 1<<%d, Time taken: %s\n", b, c, elapsed)
		}

	}

	elapsed := time.Since(start)
	fmt.Println("Time taken to read the file: ", elapsed)
}

func processBuffer(buff, remaining []byte, ch chan []byte) []byte {
	before, after := CutLast(buff, lineSep)
	// before, after := make([]byte, len(b)), make([]byte, len(a))
	// copy(before, b)
	// copy(after, a)

	if len(remaining) > 0 {
		// prepend last remains to the new chunk
		before = append(remaining, before...)
		remaining = remaining[:0]
	}

	if len(after) > 0 {
		copy(remaining, after)
	}

	buftosend := make([]byte, len(before))
	copy(buftosend, before)
	ch <- buftosend
	return remaining
}

func CutLast(s []byte, sep byte) (before []byte, after []byte) {
	i := LastIndex(s, sep)

	if len(s) == 0 {
		panic("Empty slice for cutlast")
	}

	if i == len(s)-1 {
		return s[:i-1], nil
	}

	return s[:i-1], s[i+1:]

}

func LastIndex(s []byte, sep byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == sep {
			return i
		}
	}
	return -1
}

type Measurament struct {
	Name  []byte
	Min   int
	Max   int
	Sum   int
	Count int
}

// func (m *Measurament) String() string {
// 	return fmt.Sprintf("{Min: %d, Max: %d, Sum: %d, Count: %d}", m.Min, m.Max, m.Sum, m.Count)
// }

func consumer1(
	allocSize int64,
	input chan []byte,
	output chan []map[uint64]*Measurament,
) {

	results := make([]map[uint64]*Measurament, 0, allocSize/BUFF_SIZE)

	hh := fnv.New64a()

	for v := range input {
		results = append(results, parseChunk(hh, v))
	}

	output <- results

}

// this functions splits s into chunks by sep
func BytesSplit(s []byte, sep byte) [][]byte {

	// Define an initial capacity for the result slice
	result := make([][]byte, 0, len(s)/(AVG_ROW_SIZE+5))
	start := 0
	for i, b := range s {
		if b == sep {
			// Append the sub-slice from start to i (excluding i)
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	// Include the remaining portion of the input slice
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result

}

func nameAndValueFromBytes(m []byte) ([]byte, int) {
	var bname []byte
	var bvalue []byte

	for i, b := range m {
		if b == 59 { // ;
			bname = m[:i]
			bvalue = m[i+1:]
			break
		}
	}

	// remove crap from start of name
	for bname[0] < 65 {
		bname = bname[1:]
	}

	value := parseFloatBytesAsInt(bvalue)

	return bname, value

}

func parseChunk(hh hash.Hash64, chunk []byte) map[uint64]*Measurament {

	measurementsByLine := BytesSplit(chunk, lineSep)
	results := make(map[uint64]*Measurament, len(chunk)/10)

	for _, m := range measurementsByLine {

		bname, value := nameAndValueFromBytes(m)

		hh.Reset()

		hh.Write(bname)

		hash := hh.Sum64()

		found, ok := results[hash]
		if !ok {
			results[hash] = &Measurament{
				Name:  bname,
				Min:   value,
				Max:   value,
				Sum:   value,
				Count: 1,
			}
			continue
		}

		found.Count++
		found.Sum += value

		if value < found.Min {
			found.Min = value
		}

		if value > found.Max {
			found.Max = value
		}
	}
	return results
}

func parseFloatBytesAsInt(data []byte) int {

	var temp int
	negative := data[0] == '-'
	if negative {
		data = data[1:]
	}

	switch len(data) {
	case 3: // 1.2
		temp = int(data[0])*10 + int(data[2]) - '0'*(10+1)
	case 4: // 12.3
		_ = data[3]
		temp = int(data[0])*100 + int(data[1])*10 + int(data[3]) - '0'*(100+10+1)
	}

	if negative {
		return -temp
	}
	return temp
}

func processResults(output chan []map[uint64]*Measurament) string {

	sb := strings.Builder{}
	results := make(map[uint64]*Measurament, 500)

	for sliceOfMap := range output {

		for _, mp := range sliceOfMap {

			for k, v := range mp {

				found, ok := results[k]

				if !ok {
					results[k] = v
					continue
				}

				found.Count++
				found.Sum += v.Sum

				if v.Min < found.Min {
					found.Min = v.Min

				}

				if v.Max > found.Max {
					found.Max = v.Max
				}
			}
		}
	}

	slice := make([]string, 0, len(results))

	for _, v := range results {
		slice = append(slice, stationResultString(string(v.Name), v))
	}

	slices.SortFunc(slice, func(a, b string) int {
		a, _, _ = strings.Cut(a, "=")
		b, _, _ = strings.Cut(b, "=")
		return strings.Compare(a, b)
	})

	sb.WriteString("{")
	for i, res := range slice {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(res)
	}

	sb.WriteString("}")

	ress := sb.String()

	return ress
}

func stationResultString(name string, m *Measurament) string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, float64(m.Min)/10, (float64(m.Sum)/float64(m.Count))/10, float64(m.Max)/10)
}

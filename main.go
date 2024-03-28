package main

import (
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"time"

)

const (
	BUFF_SIZE    = 4096
	CONSUMERS    = 10
	AVG_ROW_SIZE = 16
)

var (
	lineSep = []byte("\r\n")
)

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

	f, err := os.Open("../1brc/measurements_50m.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fi, _ := f.Stat()

	avgSizePerConsumer := fi.Size() / CONSUMERS
	consumerPreallocSize := avgSizePerConsumer / AVG_ROW_SIZE

	consumerQueue := make(chan []byte)
	resultsQueue := make(chan []map[string]*Measurament, 1000)

	wgConsumers := &sync.WaitGroup{}
	wgConsumers.Add(CONSUMERS)

	for i := 0; i < CONSUMERS; i++ {
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

	buff := make([]byte, BUFF_SIZE)
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

	fmt.Println(<-chanResult)

	elapsed := time.Since(start)
	fmt.Println("Time taken to read the file: ", elapsed)
}

func processBuffer(buff, remaining []byte, ch chan []byte) []byte {
	b, a := CutLast(buff, lineSep)
	before, after := make([]byte, len(b)), make([]byte, len(a))
	copy(before, b)
	copy(after, a)

	if len(remaining) > 0 {
		// prepend last remains to the new chunk
		before = append(remaining, before...)
		remaining = nil
	}

	if len(after) > 0 {
		remaining = make([]byte, len(after))
		copy(remaining, after)
	}

	if len(before) == 0 {
		panic(" Empty chunk")
	}

	buftosend := make([]byte, len(before))
	copy(buftosend, before)
	ch <- buftosend
	return remaining
}

func CutLast(s []byte, sep []byte) (before []byte, after []byte) {
	i := LastIndex(s, sep)

	if len(s) == 0 {
		panic("Empty slice for cutlast")
	}

	if i == len(s)-1 {
		// fmt.Printf("CUT found at last index %d = '%s'\n", i, string(s[i]))
		return s[:i-len(sep)], nil
	}

	return s[:i-len(sep)], s[i+len(sep):]

}

func LastIndex(s []byte, sep []byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == sep[0] {
			return i
		}
	}
	return -1
}

type Measurament struct {
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
	output chan []map[string]*Measurament,
) {

	results := make([]map[string]*Measurament, 0, allocSize/BUFF_SIZE)

	for v := range input {

		// 		fmt.Printf(`
		// Arrived at consumer: %s
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// `, string(v))

		results = append(results, parseChunk(v))
	}

	output <- results

}

// this functions splits s into chunks of sep
func BytesSplit(s []byte, sep byte) [][]byte {

	result := make([][]byte, 0, (len(s)/AVG_ROW_SIZE)+50)

	

	start := 0
	for i, b := range s {
		if b == sep {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result

}

func parseChunk(chunk []byte) map[string]*Measurament {

	measurementsByLine := BytesSplit(chunk, lineSep[0])
	results := make(map[string]*Measurament, len(chunk)/AVG_ROW_SIZE)

	for _, m := range measurementsByLine {

		// fmt.Printf("Chunk: %s\n", string(m))

		//sv := bytes.Split(m, stationSep)

		var bname []byte
		var bvalue []byte

		for i, b := range m {
			if b == 59 {
				bname = m[:i]
				bvalue = m[i+1:]
				break
			}
		}

		if len(bname) == 0 || len(bvalue) == 0 {
			panic("station name or value")
		}

		name := string(bname)

		value := parseFloatBytesAsInt(bvalue)

		found, ok := results[name]
		if !ok {
			results[name] = &Measurament{
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

func parseFloatBytesAsInt(b []byte) int {
	var result int
	negative := false

	// ignore last char bc it is a new line -- \n
	for _, b := range b[:len(b)-1] {
		if b == 45 { // 45 = `-` signal
			negative = true
			continue
		}
		result = result*10 + int(b-48)
	}

	if negative {
		return -result
	}

	return result
}

func processResults(output chan []map[string]*Measurament) string {

	sb := strings.Builder{}
	results := make(map[string]*Measurament, 500)

	for sliceOfMap := range output {

		for _, mp := range sliceOfMap {

			for k, v := range mp {

				found, ok := results[k]

				if !ok {
					results[k] = v
					continue
				}

				found.Count += v.Count
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

	// fmt.Printf("results before: %v\n", results)

	slice := make([]string, 0, len(results))

	for k, v := range results {
		slice = append(slice, stationResultString(k, v))
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
		sb.WriteString(`"`)
		sb.WriteString(res)
		sb.WriteString(`"`)
	}

	sb.WriteString("}")

	ress := sb.String()

	// which := ress[2]

	// fmt.Printf("napoli: %v - %c - %T - %#v - %+v\n", which, which, which, which, which)

	return ress
}

func stationResultString(name string, m *Measurament) string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, float64(m.Min)/10, (float64(m.Sum)/float64(m.Count))/10, float64(m.Max)/10)
}

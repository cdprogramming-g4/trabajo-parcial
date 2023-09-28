// Go program to illustrate
// How to read a csv file
package main

import (
	// Reading CSV
	"encoding/csv"
	"fmt"
	"log"
	"os"

	// MapReduce
	"math"
	"math/rand"
	"sync"
	"time"
)

/********** READING CSV **********/
func readCsv() {

	// os.Open() opens specific file in
	// read-only mode and this return
	// a pointer of type os.File
	file, err := os.Open("Datos.csv")

	// Checks for the error
	if err != nil {
		log.Fatal("Error while reading the file", err)
	}

	// Closes the file
	defer file.Close()

	// The csv.NewReader() function is called in
	// which the object os.File passed as its parameter
	// and this creates a new csv.Reader that reads
	// from the file
	reader := csv.NewReader(file)

	// ReadAll reads all the records from the CSV file
	// and Returns them as slice of slices of string
	// and an error if any
	records, err := reader.ReadAll()

	// Checks for the error
	if err != nil {
		fmt.Println("Error reading records")
	}

	// Loop to iterate through
	// and print each of the string slice
	for i, eachrecord := range records {
		if i == 10 {
			break
		}
		fmt.Println(eachrecord[0])
	}
}

/********** MAP REDUCING **********/
// Defining the structure
type Record struct {
	Department    string
	Province      string
	District      string
	Period        int // date as an integer (YYYYMM)
	KWConsumption float64
	Billing       float64
	Status        string
}

// Function to generate sample data // TODO: REMOVE
func generateSampleData() []Record {
	var data []Record
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 100; i++ {
		data = append(data, Record{
			Department:    fmt.Sprintf("Department%d", rand.Intn(5)),
			Province:      fmt.Sprintf("Province%d", rand.Intn(3)),
			District:      fmt.Sprintf("District%d", rand.Intn(10)),
			Period:        rand.Intn(10000) + 200000, // Random year and month format
			KWConsumption: rand.Float64() * 1000,
			Billing:       rand.Float64() * 200,
			Status:        "Active",
		})
	}
	return data
}

// Function to perform the Map operation
func Map(records []Record, mapper func(Record) (string, float64), ch chan map[string]float64, wg *sync.WaitGroup) {
	defer wg.Done()
	result := make(map[string]float64)

	for _, record := range records {
		key, value := mapper(record)
		result[key] += value
	}

	ch <- result
}

// Function to perform the Reduce operation
func Reduce(maps []map[string]float64) map[string]float64 {
	finalResult := make(map[string]float64)

	for _, m := range maps {
		for key, value := range m {
			finalResult[key] += value
		}
	}

	return finalResult
}

func MapReduce(data []Record) {
	// Channels and WaitGroup for concurrency
	ch := make(chan map[string]float64, len(data))
	var wg sync.WaitGroup

	// Define the mapping function for KWConsumption
	kwConsumptionMapper := func(record Record) (string, float64) {
		// Here, we group KWConsumption into ranges (0-100, 100-200, etc.)
		// TODO: Change to lower range
		rangeStart := math.Floor(record.KWConsumption/100) * 100
		rangeEnd := rangeStart + 100
		rangeKey := fmt.Sprintf("%.0f-%.0f KW", rangeStart, rangeEnd)
		return rangeKey, record.KWConsumption
	}

	// Define the mapping function for Billing
	billingMapper := func(record Record) (string, float64) {
		// Here, we group Billing into ranges (0-50, 50-100, etc.)
		// TODO: Change to lower range
		rangeStart := math.Floor(record.Billing/50) * 50
		rangeEnd := rangeStart + 50
		rangeKey := fmt.Sprintf("$%.0f-$%.0f", rangeStart, rangeEnd)
		return rangeKey, record.Billing
	}

	// Map phase: apply mapping functions concurrently
	wg.Add(2)
	go Map(data, kwConsumptionMapper, ch, &wg)
	go Map(data, billingMapper, ch, &wg)

	wg.Wait()
	close(ch)

	// Reduce phase: combine results from mapping phase
	var intermediateResults []map[string]float64
	for m := range ch {
		intermediateResults = append(intermediateResults, m)
	}

	finalKWConsumptionResult := Reduce(intermediateResults[:len(intermediateResults)/2])
	finalBillingResult := Reduce(intermediateResults[len(intermediateResults)/2:])

	// Show the final results
	fmt.Println("Final KW Consumption Result:")
	for key, value := range finalKWConsumptionResult {
		fmt.Printf("%s: %.2f KW\n", key, value)
	}

	fmt.Println("\nFinal Billing Result:")
	for key, value := range finalBillingResult {
		fmt.Printf("%s: $%.2f\n", key, value)
	}
}

func main() {
	data := generateSampleData() // TODO: USE CSV
	MapReduce(data)
}

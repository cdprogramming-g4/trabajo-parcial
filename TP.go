package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
)

// Definir la estructura del registro para todas las columnas del CSV
type Record struct {
	Department    string
	Province      string
	District      string
	Period        int
	KWConsumption float64
	Billing       float64
	Status        string
}

// Función para leer datos del CSV
func readCSV(filePath string) ([]Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	var out []Record
	for _, record := range records[1:] {
		period, _ := strconv.Atoi(record[7])
		consumption, _ := strconv.ParseFloat(record[8], 64)
		billing, _ := strconv.ParseFloat(record[9], 64)

		out = append(out, Record{
			Department:    record[1],
			Province:      record[2],
			District:      record[3],
			Period:        period,
			KWConsumption: consumption,
			Billing:       billing,
			Status:        record[10],
		})
	}

	return out, nil
}

// Definir la estructura para guardar los resultados del map-reduce
type Result struct {
	Province      string
	KWConsumption float64
	Billing       float64
}

// Función para realizar la operación de map-reduce en los registros del CSV
func mapReduce(records []Record) []Result {
	m := make(map[string]Result)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, record := range records {
		wg.Add(1)
		go func(record Record) {
			defer wg.Done()
			mu.Lock()
			r, exists := m[record.Province]
			if !exists {
				r = Result{Province: record.Province}
			}
			r.KWConsumption += record.KWConsumption
			r.Billing += record.Billing
			m[record.Province] = r
			mu.Unlock()
		}(record)
	}

	wg.Wait()

	var results []Result
	for _, result := range m {
		results = append(results, result)
	}

	return results
}

// Función principal
func main() {
	// Asegúrate de cambiar la ruta del archivo según sea necesario
	filePath := "./Datos.csv"

	records, err := readCSV(filePath)
	if err != nil {
		log.Fatal(err)
	}

	results := mapReduce(records)

	// Ordenar los resultados por consumo
	sort.Slice(results, func(i, j int) bool {
		return results[i].KWConsumption > results[j].KWConsumption
	})

	// Imprimir el consumo de energía total por provincias
	fmt.Println("Consumo de Energía por Provincia en los últimos 11 meses:")
	for _, result := range results {
		fmt.Printf("%s: %.2f KW\n", result.Province, result.KWConsumption)
	}

	// Ordenar los resultados por facturación
	sort.Slice(results, func(i, j int) bool {
		return results[i].Billing > results[j].Billing
	})

	// Imprimir la facturación total en las provincias
	fmt.Println("\nFacturación por Provincia en los últimos 11 meses:")
	for _, result := range results {
		fmt.Printf("%s: %.2f PEN\n", result.Province, result.Billing)
	}
}

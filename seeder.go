package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

func main() {

	fNames := []string{"Caleb", "Andrew", "Ashley", "David", "Donald", "Edwin", "Harry", "Neil", "Owen", "Simon", "Wayne"}
	lNames := []string{"Abramson", "Hoggarth", "Blare", "Kendal", "Benson", "Black", "Chandter", "Oakman", "Day", "Evans"}

	f1, _ := os.Create("patients.json")
	f2, _ := os.Create("payments.json")

	w1 := bufio.NewWriter(f1)
	w2 := bufio.NewWriter(f2)

	var i, j, m, n int64
	j, _ = strconv.ParseInt(os.Args[1], 10, 32)
	n, _ = strconv.ParseInt(os.Args[2], 10, 32)

	_, _ = w1.WriteString("[\n")
	_, _ = w2.WriteString("[\n")
	for i = 0; i < j; i++ {

		_, _ = w1.WriteString(
			fmt.Sprintf(
				`{"firstName":"%s","lastName":"%s","dateOfBirth":"2000-01-01","externalId":"%d"}`,
				fNames[rand.Intn(len(fNames))], lNames[rand.Intn(len(lNames))], i+1))

		if i+1 < j {
			_, _ = w1.WriteString(",\n")
		}

		for m = 0; m < n; m++ {
			_, _ = w2.WriteString(fmt.Sprintf(`{"amount":%f,"patientId":"%d","externalId":"%d"}`,
				float64(rand.Intn(10000))/100, i+1, 1000000000+((i+1)*10000)+m))

			if i+1 < j || m+1 < n {
				_, _ = w2.WriteString(",\n")
			}
		}
	}
	_, _ = w1.WriteString("\n]")
	_, _ = w2.WriteString("\n]")

	_ = w1.Flush()
	_ = f1.Close()

	_ = w2.Flush()
	_ = f2.Close()
}

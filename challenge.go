package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-pg/pg"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var db *pg.DB

func uploadFile(dir string, body io.ReadCloser, res http.ResponseWriter) {
	epoch := time.Now().UnixNano()
	tmp := fmt.Sprintf("tmp/%d.json", epoch)
	dst := fmt.Sprintf("upload/%s/%d.json", dir, epoch)

	f, _ := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	w := bufio.NewWriter(f)
	io.Copy(w, body)
	w.Flush()
	f.Close()

	os.Rename(tmp, dst)

	res.WriteHeader(http.StatusCreated)
}

func getPatients(res http.ResponseWriter, req *http.Request) {
	res.Write([]byte("/patients"))
}

func getPayments(res http.ResponseWriter, req *http.Request) {
	res.Write([]byte("/payments"))
}

func patients(res http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		uploadFile("patients", req.Body, res)
	default:
		getPatients(res, req)
	}
}

func payments(res http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "POST":
		uploadFile("payments", req.Body, res)
	default:
		getPayments(res, req)
	}
}

func patientsParser() {
	dir := "upload/patients"
	for {
		files, _ := ioutil.ReadDir(dir)
		for _, f := range files {
			start := time.Now()
			file := fmt.Sprintf("%s/%s", dir, f.Name())

			fmt.Printf("Новый файл %s\n", file)

			pr, pw := io.Pipe()

			go parseJsonFile(file, pw)

			tx, _ := db.Begin()
			tx.Exec(`DROP TABLE IF EXISTS patients_new`)
			tx.Exec(`CREATE UNLOGGED TABLE patients_new (LIKE patients INCLUDING ALL)`)

			t := time.Now()
			tx.CopyFrom(pr, `COPY patients_new(external_id,first_name,last_name,date_of_birth) FROM STDIN WITH CSV`)
			fmt.Printf("Команда COPY выполнена за %s\n", time.Since(t))

			t = time.Now()
			tx.Exec(`ALTER TABLE patients_new SET LOGGED`)
			tx.Exec(`ALTER TABLE patients_new ADD PRIMARY KEY (id)`)
			tx.Exec(fmt.Sprintf("CREATE UNIQUE INDEX patients_external_id_uidx_%d ON patients_new (external_id)", time.Now().UnixNano()))
			tx.Exec(`ANALYSE patients_new`)
			fmt.Printf("Индексы созданы за %s\n", time.Since(t))

			t = time.Now()
			tx.Exec(`ALTER TABLE IF EXISTS patients_sub NO INHERIT patients`)
			tx.Exec(`ALTER TABLE IF EXISTS patients_sub RENAME TO patients_old`)
			tx.Exec(`ALTER TABLE IF EXISTS patients_new RENAME TO patients_sub`)
			tx.Exec(`ALTER TABLE IF EXISTS patients_sub INHERIT patients`)
			tx.Exec(`DROP TABLE IF EXISTS patients_old`)
			fmt.Printf("Таблицы свопнуты за %s\n", time.Since(t))

			tx.Commit()

			fmt.Printf("Обработано за %s\n", time.Since(start))

			os.Remove(file)
		}
		time.Sleep(time.Second)
	}
}

func paymentsParser() {
	dir := "upload/payments"
	for {
		files, _ := ioutil.ReadDir(dir)
		for _, f := range files {
			start := time.Now()

			file := fmt.Sprintf("%s/%s", dir, f.Name())

			fmt.Printf("Новый файл %s\n", file)

			pr, pw := io.Pipe()

			go parseJson(file, pw)

			tx, _ := db.Begin()

			tx.Exec(`DROP TABLE IF EXISTS payments_new`)
			tx.Exec(`CREATE UNLOGGED TABLE payments_new (LIKE payments INCLUDING ALL)`)

			t := time.Now()
			tx.CopyFrom(pr, `COPY payments_new(external_id,patient_id,amount) FROM STDIN WITH CSV`)
			fmt.Printf("Команда COPY выполнена за %s\n", time.Since(t))

			t = time.Now()
			tx.Exec(`ALTER TABLE payments_new SET LOGGED`)
			tx.Exec(`ALTER TABLE payments_new ADD PRIMARY KEY (id)`)
			tx.Exec(fmt.Sprintf("CREATE UNIQUE INDEX payments_external_id_uidx_%d ON payments_new (external_id)", time.Now().UnixNano()))
			tx.Exec(fmt.Sprintf("CREATE INDEX payments_patient_id_uidx_%d ON payments_new (patient_id)", time.Now().UnixNano()))
			tx.Exec(`ANALYSE payments_new`)
			fmt.Printf("Индексы созданы за %s\n", time.Since(t))

			t = time.Now()
			tx.Exec(`ALTER TABLE IF EXISTS payments_sub NO INHERIT payments`)
			tx.Exec(`ALTER TABLE IF EXISTS payments_sub RENAME TO payments_old`)
			tx.Exec(`ALTER TABLE IF EXISTS payments_new RENAME TO payments_sub`)
			tx.Exec(`ALTER TABLE IF EXISTS payments_sub INHERIT payments`)
			tx.Exec(`DROP TABLE IF EXISTS payments_old`)
			fmt.Printf("Таблицы свопнуты за %s\n", time.Since(t))

			t = time.Now()
			tx.Exec(`DROP TABLE IF EXISTS patients_stats_new`)
			tx.Exec(`CREATE UNLOGGED TABLE patients_stats_new (LIKE patients_stats INCLUDING ALL)`)

			tx.Exec(`INSERT INTO patients_stats_new (patients_id, patient_id, total_amount)
				SELECT patients.id, patient_id, SUM(amount) FROM payments
				JOIN patients ON patients.external_id = payments.patient_id GROUP BY patients.id, patient_id`)

			tx.Exec(`ALTER TABLE patients_stats_new SET LOGGED`)
			tx.Exec(fmt.Sprintf("CREATE INDEX patients_stats_idx_%d ON patients_stats_new (patients_id, patient_id)", time.Now().UnixNano()))
			tx.Exec(`ANALYSE patients_stats_new`)

			tx.Exec(`ALTER TABLE IF EXISTS patients_stats_sub NO INHERIT patients_stats`)
			tx.Exec(`ALTER TABLE IF EXISTS patients_stats_sub RENAME TO patients_stats_old`)
			tx.Exec(`ALTER TABLE IF EXISTS patients_stats_new RENAME TO patients_stats_sub`)
			tx.Exec(`ALTER TABLE IF EXISTS patients_stats_sub INHERIT patients_stats`)
			tx.Exec(`DROP TABLE IF EXISTS patients_stats_old`)
			fmt.Printf("Статиситка посчитана за %s\n", time.Since(t))

			tx.Commit()

			fmt.Printf("Обработано за %s\n", time.Since(start))

			os.Remove(file)
		}
		time.Sleep(time.Second)
	}
}

func split(data []byte, atEOF bool) (advance int, token []byte, err error) {

	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	switch data[0] {
	case '{':
		for i, b := range data {
			if b == '}' {
				return i + 1, data[:i+1], nil
			}
		}
		return 0, nil, nil
	default:
		advance, token, err = 1, data[:1], nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return
}

func parseJson(file string, w io.WriteCloser) {
	//var uniq = map[string]interface{}{}

	bw := bufio.NewWriter(w)
	f, _ := os.Open(file)

	dec := json.NewDecoder(bufio.NewReader(f))
	dec.Token() // Пропускаем "["

	payment := &struct {
		Amount     float64 `json:"amount"`
		PatientId  string  `json:"patientId"`
		ExternalId string  `json:"externalId"`
	}{}

	for dec.More() {
		dec.Decode(&payment)

		//if _, ok := uniq[payment.ExternalId]; ok {
		//	fmt.Printf(" !!! Пропускаем externalId=%s\n", payment.ExternalId)
		//	continue
		//}

		//uniq[payment.ExternalId] = nil

		bw.WriteString(payment.ExternalId)
		bw.WriteByte(',')
		bw.WriteString(payment.PatientId)
		bw.WriteByte(',')
		bw.WriteString(fmt.Sprintf("%f", payment.Amount))
		bw.WriteByte('\n')
	}

	bw.Flush()
	w.Close()
	f.Close()
}

func parseJsonToken(b []byte, name []byte) []byte {
	z := bytes.Index(b, name)
	z += len(name)
	o := bytes.IndexByte(b[z:], '"')
	o += z + 1
	c := bytes.IndexByte(b[o:], '"')
	c += o
	return b[o:c]
}

func parseJsonFile(file string, w io.WriteCloser) {
	//var extId []byte
	//var uniq = map[string]interface{}{}

	bw := bufio.NewWriter(w)
	f, _ := os.Open(file)
	scanner := bufio.NewScanner(bufio.NewReader(f))
	scanner.Split(split)

	ext := []byte(`"externalId"`)
	fnm := []byte(`"firstName"`)
	lnm := []byte(`"lastName"`)
	dob := []byte(`"dateOfBirth"`)

	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) > 1 {
			//extId = parseJsonToken(b, []byte(`"externalId"`))

			//if _, ok := uniq[string(extId)]; ok {
			//	fmt.Printf(" !!! Пропускаем externalId=%s\n", extId)
			//	continue
			//}

			//uniq[string(extId)] = nil

			//bw.Write(extId)
			bw.Write(parseJsonToken(b, ext))
			bw.WriteByte(',')
			bw.Write(parseJsonToken(b, fnm))
			bw.WriteByte(',')
			bw.Write(parseJsonToken(b, lnm))
			bw.WriteByte(',')
			bw.Write(parseJsonToken(b, dob))
			bw.WriteByte('\n')
		}
	}

	bw.Flush()
	w.Close()
	f.Close()
}

func createDb() {
	tx, err := db.Begin()
	if err != nil {
		log.Panic(err)
	}

	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS patients (
  id             BIGSERIAL    NOT NULL,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ,
  external_id    TEXT         NOT NULL,
  first_name     TEXT         NOT NULL,
  last_name      TEXT         NOT NULL,
  middle_name    TEXT,
  date_of_birth  DATE         NOT NULL
)`)
	if err != nil {
		log.Panic(err)
	}

	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS payments (
  id             BIGSERIAL      NOT NULL,
  created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ,
  external_id    TEXT           NOT NULL,
  patient_id     TEXT           NOT NULL,
  amount         DECIMAL(10,2)  NOT NULL
)`)
	if err != nil {
		log.Panic(err)
	}

	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS patients_stats (
  patients_id    BIGINT         NOT NULL,
  patient_id     TEXT           NOT NULL,
  total_amount   DECIMAL(10,2)  NOT NULL
)`)
	if err != nil {
		log.Panic(err)
	}

	err = tx.Commit()
	if err != nil {
		log.Panic(err)
	}
}

func main() {
	db = pg.Connect(&pg.Options{
		Network:  "unix",
		Addr:     "/tmp/.s.PGSQL.5432",
		User:     "admin",
		Password: "",
		Database: "demo",
	})
	defer db.Close()

	createDb()

	go patientsParser()
	go paymentsParser()

	http.HandleFunc("/patients", patients)
	http.HandleFunc("/payments", payments)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

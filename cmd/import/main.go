/*
CREATE TABLE books (id UUID PRIMARY KEY, isbn text, title text, author text, rating text, good_reads_id text);
*/

package main

import (
	"encoding/csv"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"os"
	"time"
)

var (
	cluster *gocql.ClusterConfig
	session *gocql.Session
)

func init() {
	cluster = gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "books"
	cluster.Consistency = gocql.Quorum

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
}

func main() {
	books, err := readCSV(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	books = books[1:]

	for i, book := range books {
		good_reads_id := book[1]
		isbn := book[5]
		author := book[7]
		title := book[10]
		rating := book[12]

		fmt.Println(fmt.Sprintf("%d -> %s %s %s %s %s", i, good_reads_id, isbn, author, title, rating))

		if err := session.Query(`INSERT INTO books (id, isbn, title, author, rating, good_reads_id,added_at) VALUES (?, ?, ?, ?, ?, ?,?)`, gocql.TimeUUID(), isbn, title, author, rating, good_reads_id, time.Now()).Exec(); err != nil {
			log.Fatal(err)
		}
	}

	defer session.Close()
}

func readCSV(filepath string) ([][]string, error) {
	csvfile, err := os.Open(filepath)

	if err != nil {
		return nil, err
	}

	defer csvfile.Close()

	reader := csv.NewReader(csvfile)
	fields, err := reader.ReadAll()

	return fields, nil
}

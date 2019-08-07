package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"html/template"
	"log"
	"net/http"
	"runtime"
	"strconv"
)

type Book struct {
	Id          string
	ISBN        string
	Title       string
	Author      string
	Rating      string
	GoodReadsId string
}

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
	tmpl := template.Must(template.ParseFiles("templates/books.html"))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		books := []*Book{}

		total := 0
		size := IntParam(r, "size", 10)
		skip := IntParam(r, "skip", 0)

		var (
			iter   *gocql.Iter
			lastId string
		)

		allocBefore := Mem()
		if skip > 0 {

			iter = session.Query(`SELECT id FROM books LIMIT ?`, skip).Iter()

			for iter.Scan(&lastId) {
				break
			}

		}
		allocAfter := Mem()

		if err := session.Query(`SELECT COUNT(id) FROM books`).Consistency(gocql.One).Scan(&total); err != nil {
			log.Fatal(err)
		}

		if skip > 0 {
			iter = session.Query(`SELECT id,isbn,title,author,rating,good_reads_id FROM books WHERE token(id) > token(?) LIMIT ?`, lastId, size).Iter()
		} else {
			iter = session.Query(`SELECT id,isbn,title,author,rating,good_reads_id FROM books LIMIT ?`, size).Iter()
		}

		var (
			id            string
			isbn          string
			title         string
			author        string
			rating        string
			good_reads_id string
		)

		for iter.Scan(&id, &isbn, &title, &author, &rating, &good_reads_id) {
			books = append(books, &Book{
				Id:          id,
				ISBN:        isbn,
				Title:       title,
				Author:      author,
				Rating:      rating,
				GoodReadsId: good_reads_id,
			})
		}

		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}

		data := &struct {
			Total       int
			Skip        int
			Size        int
			Books       []*Book
			AllocBefore int
			AllocAfter  int
		}{
			Total:       total,
			Skip:        skip,
			Size:        size,
			Books:       books,
			AllocBefore: allocBefore,
			AllocAfter:  allocAfter,
		}

		tmpl.Execute(w, data)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
	defer session.Close()
}

func IntParam(r *http.Request, name string, defaultValue int) int {
	input := r.FormValue(name)
	number, err := strconv.Atoi(input)
	if err == nil {
		return number
	}

	return defaultValue
}

func Mem() int {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int(m.Alloc / 1024)
}

package main

import (
	"flag"
	"froopydb"
)

func main() {
	folder := flag.String("folder", "./.froopydb", "folder path for the database")
	flag.Parse()

	db := froopydb.NewDB(froopydb.DefaultConfig(*folder))
	defer db.Close()

	println("====GET====")
	db.Set([]byte{1}, []byte("foo"))
	println(db.Get([]byte{1}))

	db.Set([]byte{1}, []byte("bar"))
	println(db.Get([]byte{1}))
	for i := range 100 {
		db.Set([]byte{byte(i + 2)}, []byte("spam"))
	}

	println("====DELETE====")
	print(db.Delete([]byte{12}))

	println("====GET====")
	print(db.Get([]byte{12}))
}

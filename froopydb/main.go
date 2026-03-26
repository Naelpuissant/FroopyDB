package main

import (
	"flag"
	"froopydb"
	"froopydb/logger"
)

func main() {
	folder := flag.String("folder", "./.froopydb", "folder path for the database")
	flag.Parse()

	db := froopydb.NewDB(&froopydb.DBConfig{
		Folder:          *folder,
		MemTableMaxSize: froopydb.KB,
		ClearOnStart:    false,
		LogLevel:        logger.DEBUG,
	})
	defer db.Close()

	txn := db.NewTransaction()

	println("====GET====")
	txn.Set([]byte{1}, []byte("foo"))
	println(txn.Get([]byte{1}))

	txn.Set([]byte{1}, []byte("bar"))
	println(txn.Get([]byte{1}))
	for i := range 100 {
		txn.Set([]byte{byte(i + 2)}, []byte("spam"))
	}

	println("====DELETE====")
	txn.Delete([]byte{12})

	println("====GET====")
	println(txn.Get([]byte{12}))

	txn.Commit()
}

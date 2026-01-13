package main

import (
	fpdb "froopydb"
	"froopydb/logger"
)

func main() {
	db := fpdb.NewDB("/tmp/froopydb/test/main", 0, 256, false, logger.DEBUG)
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

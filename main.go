package main

import (
	"froopydb/src"
)

func main() {
	db := src.NewDB("./db/main", 0, 0, false)
	defer db.Close()

	println("====GET====")
	db.Set(12, "foo")
	println(db.Get(12))

	db.Set(12, "bar")
	println(db.Get(12))

	println("====DELETE====")
	print(db.Delete(12))

	println("====GET====")
	print(db.Get(12))
}

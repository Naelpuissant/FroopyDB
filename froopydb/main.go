package main

import fpdb "froopydb"

func main() {
	db := fpdb.NewDB("./test/main", 0, 256, false)
	defer db.Close()

	println("====GET====")
	db.Set(1, "foo")
	println(db.Get(1))

	db.Set(1, "bar")
	println(db.Get(1))

	for i := range 100 {
		db.Set(i+2, "spam")
	}

	println("====DELETE====")
	print(db.Delete(12))

	println("====GET====")
	print(db.Get(12))
}

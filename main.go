package main

import (
	"TEST2024/customeranalysis"
	"TEST2024/database"
	"TEST2024/datageneration"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	db := database.GetDBInstance()
	defer db.Close()

	datageneration.GenerateData(db)

	customeranalysis.RunCustomerAnalysis(db)

}

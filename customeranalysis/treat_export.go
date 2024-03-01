package customeranalysis

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type CustomerEvent struct {
	CustomerID int
	ContentID  int
	Quantity   int
}

type CustomerData struct {
	CustomerID   int
	ChannelValue string
}

type ContentPrice struct {
	ContentID int
	Price     float64
}

type Customer struct {
	CustomerID  int
	Information string
	TotalSales  float64
}

type QuantileInfo struct {
	NumberOfCustomers int
	MaxSales          float64
}

func FetchContentPrices(db *sql.DB) (map[int]float64, error) {
	query := `SELECT ContentID, Price FROM ContentPrice`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	contentPrices := make(map[int]float64)
	for rows.Next() {
		var contentID int
		var price float64
		if err := rows.Scan(&contentID, &price); err != nil {
			return nil, err
		}
		contentPrices[contentID] = price
	}
	return contentPrices, nil
}
func FetchCustomerData(db *sql.DB) (map[int]string, error) {
	query := `SELECT CustomerID, ChannelValue FROM CustomerData`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	customerData := make(map[int]string)
	for rows.Next() {
		var customerID int
		var channelValue string
		if err := rows.Scan(&customerID, &channelValue); err != nil {
			return nil, err
		}
		customerData[customerID] = channelValue
	}
	return customerData, nil
}
func FetchCustomerEvents(db *sql.DB) ([]CustomerEvent, error) {
	query := `
	SELECT CustomerID, ContentID, Quantity
	FROM CustomerEventData
	WHERE EventDate >= '2020-04-01 00:00:00' AND EventTypeID = 6
	`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []CustomerEvent
	for rows.Next() {
		var e CustomerEvent
		if err := rows.Scan(&e.CustomerID, &e.ContentID, &e.Quantity); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}
func MakeCustomerSales(events []CustomerEvent, customerData map[int]string, contentPrices map[int]float64) []Customer {
	customerSales := make(map[int]*Customer)
	for _, event := range events {
		price, ok := contentPrices[event.ContentID]
		if !ok {
			continue // Skip if the content price is not found
		}
		totalSale := price * float64(event.Quantity)

		if cust, exists := customerSales[event.CustomerID]; exists {
			cust.TotalSales += totalSale
		} else {
			customerSales[event.CustomerID] = &Customer{
				CustomerID:  event.CustomerID,
				Information: customerData[event.CustomerID],
				TotalSales:  totalSale,
			}
		}
	}

	var customers []Customer
	for _, cust := range customerSales {
		customers = append(customers, *cust)
	}
	return customers
}

// fetchCustomers retrieves customer data from the database.
func fetchCustomers(db *sql.DB) ([]Customer, error) {
	// Fetch data
	events, err := FetchCustomerEvents(db)
	if err != nil {
		log.Fatal("Error fetching customer events: ", err)
	}
	customerData, err := FetchCustomerData(db)
	if err != nil {
		log.Fatal("Error fetching customer data: ", err)
	}
	contentPrices, err := FetchContentPrices(db)
	if err != nil {
		log.Fatal("Error fetching content prices: ", err)
	}
	// Aggregate customer sales
	customers := MakeCustomerSales(events, customerData, contentPrices)
	sort.Slice(customers, func(i, j int) bool {
		return customers[i].TotalSales > customers[j].TotalSales
	})

	return customers, nil
}

// createAndPopulateCustomerTable creates a new customer table and populates it with data.
func createAndPopulateCustomerTable(db *sql.DB, customers []Customer) error {
	today := time.Now().Format("20060102") // Get current date for naming the table.
	tableName := fmt.Sprintf("test_2024_%s", today)

	createTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		ID INT AUTO_INCREMENT PRIMARY KEY,
		CustomerID INT,
		INFO CHAR(255),
		TotalSales FLOAT
	);
	`, tableName) // SQL statement to create a new table.

	// Calculate the number for the top 2.5% customers.
	topPercent := int(float64(len(customers)) * 0.025)

	// Keep track of the top customers already inserted into the table.
	topCustomers := make(map[int]bool)

	_, err := db.Exec(createTable)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Prepare SQL statements for checking existence, updating, inserting and deleting the no longer top customers.
	existingQuery := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE CustomerID = ?)`, tableName)
	updateQuery := fmt.Sprintf(`UPDATE %s SET TotalSales = ? WHERE CustomerID = ?`, tableName)
	insertQuery := fmt.Sprintf(`INSERT INTO %s (CustomerID, INFO, TotalSales) VALUES (?, ?, ?)`, tableName)
	deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE CustomerID = ?`, tableName)

	for i, c := range customers {

		if i <= topPercent {
			topCustomers[c.CustomerID] = true // Mark this customer as part of the top.
		}

		var exists bool
		err = db.QueryRow(existingQuery, c.CustomerID).Scan(&exists) // Check if the customer already exists in the table.
		if err != nil {
			return err
		}

		if exists {
			if topCustomers[c.CustomerID] {
				_, err = db.Exec(updateQuery, c.TotalSales, c.CustomerID) // Update existing customer.
			} else {
				_, err = db.Exec(deleteQuery, c.CustomerID) // deleting customers that exist and no longer in the to customers .
			}

		} else {
			if topCustomers[c.CustomerID] {
				_, err = db.Exec(insertQuery, c.CustomerID, c.Information, c.TotalSales) // Insert new customer.
			}

		}

		if err != nil {
			return err
		}
	}
	_, err = db.Exec(fmt.Sprintf(`ALTER TABLE %s AUTO_INCREMENT = 1;`, tableName))
	if err != nil {
		fmt.Println("Error occurred:", err)
	}
	return nil
}

// createAndPopulateQuantilesTable creates a new table for quantile data and populates it.
func createAndPopulateQuantilesTable(db *sql.DB, customers []Customer) error {
	quantileSize := int(0.025 * float64(len(customers))) // Calculate the size of each quantile.
	Map := make(map[string]QuantileInfo)                 // Create a map to store quantile information.

	for i, c := range customers {
		index := i / quantileSize
		if index >= 40 {
			index = 39 // Force the last entries into the 40th quantile
		}
		quantile := fmt.Sprintf("%f%% - %f%%", 2.5*float64(index), 2.5*float64(index+1))
		info := Map[quantile]
		info.NumberOfCustomers++
		if c.TotalSales > info.MaxSales {
			info.MaxSales = c.TotalSales
		}
		Map[quantile] = info // Update the map with the new quantile information.
	}

	createTableSQL := `
	CREATE TABLE Quantilesdata (
		ID INT AUTO_INCREMENT PRIMARY KEY,
		QuantileRange CHAR(50),
		NumberOfCustomers INT,
		MaxSales FLOAT
	);` // SQL statement to create a table for quantile data.

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("Error creating table: %v", err)
	}

	CustomerByQuantile, err := db.Prepare("INSERT INTO Quantilesdata (QuantileRange, NumberOfCustomers, MaxSales) VALUES (?, ?, ?)")
	if err != nil {
		panic(err)
	}
	defer CustomerByQuantile.Close()

	for quantileRange, info := range Map {
		_, err = CustomerByQuantile.Exec(quantileRange, info.NumberOfCustomers, info.MaxSales) // Insert quantile data into the table.
		if err != nil {
			fmt.Println("Error occurred:", err)
		}
	}

	return nil
}

func calculateAndInsertAboveAverageCustomers(db *sql.DB, customers []Customer) error {
	var totalSales float64
	for _, c := range customers {
		totalSales += c.TotalSales
	}
	averageSales := totalSales / float64(len(customers))

	// Create table for above average customers
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS AboveAverageCustomers (
		ID INT AUTO_INCREMENT PRIMARY KEY,
		CustomerID INT,
		TotalSales FLOAT
	);
	`)
	if err != nil {
		return fmt.Errorf("error creating AboveAverageCustomers table: %v", err)
	}

	// Insert customers with sales above average into the table.
	for _, c := range customers {
		if c.TotalSales > averageSales {
			_, err := db.Exec("INSERT INTO AboveAverageCustomers (CustomerID, TotalSales) VALUES (?, ?)", c.CustomerID, c.TotalSales)
			if err != nil {
				return fmt.Errorf("error inserting above average customer: %v", err)
			}
		}
	}

	return nil
}

func quantileBYCA(db *sql.DB, customers []Customer) error {

	maxCA := customers[0].TotalSales
	minCA := customers[len(customers)-1].TotalSales
	rangePerQuantile := (maxCA - minCA) / 40 // devide the CA range into 40 categories

	// Create a map to store quantile information.
	quantileMap := make(map[string]QuantileInfo)

	// Populate the quantile map.
	for i := 0; i < 40; i++ {
		startRange := minCA + (rangePerQuantile * float64(i))
		endRange := minCA + (rangePerQuantile * float64(i+1))
		quantile := fmt.Sprintf("%.2f - %.2f", startRange, endRange) // CA range
		info := quantileMap[quantile]
		for _, c := range customers {

			// see customer's total sales is in the range of the quantile
			if c.TotalSales > startRange && c.TotalSales <= endRange {
				// If it does, increment the number of customers in this quantile.
				info.NumberOfCustomers++

				// Update the maximum sales if applicable.
				if c.TotalSales > info.MaxSales {
					info.MaxSales = c.TotalSales
				}
			}

		}
		quantileMap[quantile] = info // Update the map with the new quantile information.

	}
	createTableSQL := `
	CREATE TABLE Quantiles_BY_CA (
		ID INT AUTO_INCREMENT PRIMARY KEY,
		QuantileRange CHAR(50),
		NumberOfCustomers INT,
		MaxSales FLOAT
	);`

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("Error creating table: %v", err)
	}

	insertQuery := `INSERT INTO Quantiles_BY_CA (QuantileRange, NumberOfCustomers, MaxSales) VALUES (?, ?, ?)`

	for quantileRange, info := range quantileMap {

		_, err := db.Exec(insertQuery, quantileRange, info.NumberOfCustomers, info.MaxSales)
		if err != nil {
			fmt.Println("Error occurred:", err)
		}
	}

	return nil
}

// //////////////////////////////////////////////////////// main funtion
func RunCustomerAnalysis(db *sql.DB) {

	customers, err := fetchCustomers(db) //fetching all the customers
	if err != nil {
		log.Fatal(err)
	}

	err = createAndPopulateCustomerTable(db, customers) // creating the top customers table
	if err != nil {
		log.Fatal(err)
	}

	err = createAndPopulateQuantilesTable(db, customers) // quantile table
	if err != nil {
		log.Fatal(err)
	}
	err = quantileBYCA(db, customers) // seconde quantile table
	if err != nil {
		log.Fatal(err)
	}
	err = calculateAndInsertAboveAverageCustomers(db, customers) // all customer above Average
	if err != nil {
		log.Fatal(err)
	}
}

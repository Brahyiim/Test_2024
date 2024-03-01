package datageneration

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/icrowley/fake"
)

// Define your data structures
type Customer struct {
	CustomerID       int
	ClientCustomerID string
	InsertDate       time.Time
}

type CustomerData struct {
	CustomerChannelID int
	CustomerID        int
	ChannelTypeID     int
	ChannelValue      string
	InsertDate        time.Time
}

type Content struct {
	ContentID       int
	ClientContentID string
	InsertDate      time.Time
}

type ContentPrice struct {
	ContentPriceID int
	ContentID      int
	Price          float64
	Currency       string
	InsertDate     time.Time
}

type CustomerEvent struct {
	EventID       int
	ClientEventID string
	InsertDate    time.Time
}

type CustomerEventData struct {
	EventDataID int
	EventID     int
	ContentID   int
	CustomerID  int
	EventTypeID int
	EventDate   time.Time
	Quantity    int
	InsertDate  time.Time
}

//////////////////////FUNCTION

func weightedRandomChoice(r *rand.Rand, weights []float64) int {
	// Create a cumulative distribution from weights
	cumulative := make([]float64, len(weights))
	total := 0.0
	for i, weight := range weights {
		total += weight
		cumulative[i] = total
	}

	// Generate a random number in the range of the total weight
	number := r.Float64() * total

	// Find where the random number falls within the cumulative distribution
	for i, value := range cumulative {
		if number <= value {
			return i + 1 // Return the index which corresponds to the EventTypeID
		}
	}
	return 6
}

func fakePriceAndCurrency(r *rand.Rand) (float64, string) {

	price := r.Float64() * 1000 // Generate a price between 0 and 1000

	currency := "USD"

	return price, currency
}

func randomTimestamp(r *rand.Rand, year int) time.Time {

	month := time.Month(r.Intn(12) + 1)
	day := r.Intn(28) + 1 // keeping it simple, not accounting for month length
	hour := r.Intn(24)
	minute := r.Intn(60)
	second := r.Intn(60)

	// Create a new timestamp with these components
	return time.Date(year, month, day, hour, minute, second, 0, time.UTC)
}

func generateCustomers(r *rand.Rand) ([]Customer, []CustomerData) {
	// generating 1000 fake customer
	var customers []Customer
	var customerData []CustomerData
	for i := 1; i < 1000; i++ {
		// Use the fake package or similar to generate realistic data
		date := randomTimestamp(r, 2023)
		channelType := r.Intn(5) + 1
		var chv string
		switch channelType {
		case 1:
			chv = fake.EmailAddress()
		case 2:
			chv = fake.Phone()
		case 3:
			chv = fake.Zip()
		case 4:
			chv = fake.DigitsN(8)
		case 5:
			chv = fake.DigitsN(13)

		}
		customers = append(customers, Customer{
			CustomerID:       i,
			ClientCustomerID: fake.DigitsN(6),
			InsertDate:       date,
		})
		customerData = append(customerData, CustomerData{
			CustomerChannelID: i,
			CustomerID:        i,
			ChannelTypeID:     channelType,
			ChannelValue:      chv,
			InsertDate:        date,
		})
	}
	return customers, customerData
}
func generateContents(r *rand.Rand) ([]Content, []ContentPrice) {
	// generating 1000 fake customer
	var contents []Content
	var contentPrices []ContentPrice
	for j := 1; j <= 100; j++ {
		randomTime := randomTimestamp(r, 2023)
		price, currency := fakePriceAndCurrency(r)
		contentPrices = append(contentPrices, ContentPrice{
			ContentPriceID: j,
			ContentID:      j,
			Price:          price,
			Currency:       currency,
			InsertDate:     randomTime,
		})
		contents = append(contents, Content{
			ContentID:       j,
			ClientContentID: fake.DigitsN(8), // Using the `fake` package for demonstration
			InsertDate:      randomTime,
		})

	}
	return contents, contentPrices

}

func generateEvents(r *rand.Rand, db *sql.DB) ([]CustomerEvent, []CustomerEventData) {

	var events []CustomerEvent
	var eventdata []CustomerEventData
	for i := 1; i <= 5000; i++ {

		var customerID int
		var contentID int
		// random CustomerID
		err := db.QueryRow("SELECT CustomerID FROM Customer ORDER BY RAND() LIMIT 1").Scan(&customerID)
		if err != nil {
			fmt.Println("Error occurred:", err)
		}

		// random ContentID
		err = db.QueryRow("SELECT ContentID FROM Content ORDER BY RAND() LIMIT 1").Scan(&contentID)
		if err != nil {
			fmt.Println("Error occurred:", err)
		}
		eventTypeWeights := []float64{0.1, 0.2, 0.19, 0.18, 0.165, 0.165} // this is my choice
		eventTypeIndex := weightedRandomChoice(r, eventTypeWeights)       // trying to get reel event type
		randomTime := randomTimestamp(r, 2023)

		events = append(events, CustomerEvent{
			EventID:       i,
			ClientEventID: fake.DigitsN(10),
			InsertDate:    randomTime,
		})
		eventdata = append(eventdata, CustomerEventData{
			EventDataID: i,
			EventID:     i,
			ContentID:   contentID,
			CustomerID:  customerID,
			EventTypeID: eventTypeIndex,
			EventDate:   randomTime,
			Quantity:    r.Intn(6) + 1,
			InsertDate:  randomTime,
		})

	}
	return events, eventdata
}

//////////////////////////////////////////////////////////////

//main function

func GenerateData(db *sql.DB) {

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	customers, customersData := generateCustomers(r)
	contents, contentprices := generateContents(r)
	//CUSTOMER
	customerInsertQuery := "INSERT INTO Customer (CustomerID, ClientCustomerID, InsertDate) VALUES "
	customerDataInsertQuery := "INSERT INTO CustomerData (CustomerChannelID, CustomerID, ChannelTypeID, ChannelValue, InsertDate) VALUES "
	valueStrings := make([]string, 0, len(customers))
	valueArgs := make([]interface{}, 0, len(customers)*3) // 3 fields per customer
	data_valueStrings := make([]string, 0, len(customersData))
	datavalueArgs := make([]interface{}, 0, len(customersData)*5)

	for _, customer := range customers {
		valueStrings = append(valueStrings, "(?, ?, ?)")
		valueArgs = append(valueArgs, customer.CustomerID, customer.ClientCustomerID, customer.InsertDate)
	}
	customerInsertQuery += strings.Join(valueStrings, ",")
	_, err := db.Exec(customerInsertQuery, valueArgs...)
	if err != nil {
		panic(err)
	}

	for _, customerdata := range customersData {
		data_valueStrings = append(data_valueStrings, "(?, ?, ?, ?, ?)")
		datavalueArgs = append(datavalueArgs, customerdata.CustomerChannelID, customerdata.CustomerID, customerdata.ChannelTypeID, customerdata.ChannelValue, customerdata.InsertDate)
	}
	customerDataInsertQuery += strings.Join(data_valueStrings, ",")
	_, err2 := db.Exec(customerDataInsertQuery, datavalueArgs...)
	if err2 != nil {
		panic(err2)
	}
	//Content
	contentInsertQuery := "INSERT INTO Content (ContentID, ClientContentID, InsertDate) VALUES "
	contentPriceInsertQuery := "INSERT INTO ContentPrice (ContentPriceID, ContentID, Price, Currency, InsertDate) VALUES "
	c_valueStrings := make([]string, 0, len(contents))
	c_valueArgs := make([]interface{}, 0, len(contents)*3)
	cp_valueStrings := make([]string, 0, len(contentprices))
	cp_valueArgs := make([]interface{}, 0, len(contentprices)*5)

	for _, c := range contents {
		c_valueStrings = append(c_valueStrings, "(?, ?, ?)")
		c_valueArgs = append(c_valueArgs, c.ContentID, c.ClientContentID, c.InsertDate)
	}
	contentInsertQuery += strings.Join(c_valueStrings, ",")
	_, err3 := db.Exec(contentInsertQuery, c_valueArgs...)
	if err3 != nil {
		panic(err3)
	}

	for _, cp := range contentprices {
		cp_valueStrings = append(cp_valueStrings, "(?, ?, ?, ?, ?)")
		cp_valueArgs = append(cp_valueArgs, cp.ContentPriceID, cp.ContentID, cp.Price, cp.Currency, cp.InsertDate)
	}
	contentPriceInsertQuery += strings.Join(cp_valueStrings, ",")
	_, err4 := db.Exec(contentPriceInsertQuery, cp_valueArgs...)
	if err4 != nil {
		panic(err4)
	}
	//EVENT
	events, eventsdata := generateEvents(r, db)
	EventInsertQuery := "INSERT INTO CustomerEvent (EventID, ClientEventID, InsertDate) VALUES "
	EventDataInsertQuery := "INSERT INTO CustomerEventData (EventDataID, EventID, ContentID, CustomerID, EventTypeID, EventDate, Quantity, InsertDate) VALUES "
	e_valueStrings := make([]string, 0, len(events))
	e_valueArgs := make([]interface{}, 0, len(events)*3)
	ed_valueStrings := make([]string, 0, len(eventsdata))
	ed_valueArgs := make([]interface{}, 0, len(eventsdata)*8)

	for _, e := range events {
		e_valueStrings = append(e_valueStrings, "(?, ?, ?)")
		e_valueArgs = append(e_valueArgs, e.EventID, e.ClientEventID, e.InsertDate)
	}
	EventInsertQuery += strings.Join(e_valueStrings, ",")

	_, err5 := db.Exec(EventInsertQuery, e_valueArgs...)
	if err5 != nil {
		panic(err5)
	}

	for _, ed := range eventsdata {
		ed_valueStrings = append(ed_valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?)")
		ed_valueArgs = append(ed_valueArgs, ed.EventDataID, ed.EventID, ed.ContentID, ed.CustomerID, ed.EventTypeID, ed.EventDate, ed.Quantity, ed.InsertDate)
	}
	EventDataInsertQuery += strings.Join(ed_valueStrings, ",")

	_, err6 := db.Exec(EventDataInsertQuery, ed_valueArgs...)
	if err6 != nil {
		panic(err6)
	}

}

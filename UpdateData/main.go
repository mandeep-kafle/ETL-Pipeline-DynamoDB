package main

import (
	"fmt"
	"math/rand"
	"time"

	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type DateId struct {
	Date string
	Id   string
}

type record struct {
	Id                                                         int
	DateRep                                                    string
	Day                                                        string
	Month                                                      string
	Year                                                       string
	Cases                                                      int
	Deaths                                                     int
	CountriesAndTerritories                                    string
	GeoId                                                      string
	CountryterritoryCode                                       string
	PopData2019                                                int
	ContinentExp                                               string
	Cumulative_number_for_14_days_of_COVID_19_cases_per_100000 string
}

const SecretKey = ""
const AccessKey = ""
const TOTAL_ITEMS int = 10000
const BUFFER_SIZE int = 20
const ITEMS_SIZE_FOR_EACH_THREAD = 500

var countUpdated int = 0

func main() {
	// lambda.Start(Handler)
	Handler()

}

func Handler() {

	for ThreadNo := 0; ThreadNo < 20; ThreadNo++ {
		go Update(ThreadNo)
	}
	time.Sleep(59 * time.Second)
	fmt.Println("PROGRAM FINSIHED SUCCESFULLY___________________________________________________________________________________________")
}

func Update(Threadno int) {
	for i := 0; i < ITEMS_SIZE_FOR_EACH_THREAD/BUFFER_SIZE; i++ {
		Pagination(Threadno)
	}
}
func Pagination(Threadno int) {

	randomIdAndDate := make([]DateId, 20, 20)
	for i := 0; i < BUFFER_SIZE; i++ {
		randomIdAndDate[i].Date = getRandomDate(i + Threadno)
		randomIdAndDate[i].Id = fmt.Sprintf("%d", rand.Intn(61000)+1)

	}
	setData(randomIdAndDate, Threadno)

}

func getRandomDate(count int) string {
	rand.Seed(time.Now().UnixNano() + int64(count))
	min := 1900
	max := 2021

	year := rand.Intn(max-min+1) + 1900
	month := rand.Intn(12) + 1
	day := rand.Intn(27) + 1
	var str string
	var daystr string = fmt.Sprintf("%d", day)
	var monthstr string = fmt.Sprintf("%d", month)
	if day < 10 {
		daystr = fmt.Sprintf("0%d", day)

	}

	if month < 10 {
		monthstr = fmt.Sprintf("0%d", month)

	}

	str = fmt.Sprintf("%v/%v/%02d", daystr, monthstr, year)
	// fmt.Println(str)
	return str
}

func setData(randomIdAndDate []DateId, Threadno int) {

	if countUpdated > TOTAL_ITEMS {
		return
	}
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(AccessKey, SecretKey, ""),
	})
	if err != nil {
		fmt.Println(err)
	}

	svc := dynamodb.New(sess)

	prams := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			"BorderGo2": {
				Keys: []map[string]*dynamodb.AttributeValue{
					{
						"Id": {
							N: aws.String(randomIdAndDate[0].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[1].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[2].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[3].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[4].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[5].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[6].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[7].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[8].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[9].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[10].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[11].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[12].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[13].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[14].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[15].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[16].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[17].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[18].Id),
						},
					},
					{
						"Id": {
							N: aws.String(randomIdAndDate[19].Id),
						},
					},
				},
			},
		},
	}
	if err != nil {
		panic(err)
	}

	result, err := svc.BatchGetItem(prams)
	// fmt.Println(result)
	// // unmarshaling out response into array of struct

	cur := make([]record, 20, 20)
	for _, v := range result.Responses {

		err2 := dynamodbattribute.UnmarshalListOfMaps(v, &cur)
		if err2 != nil {
			fmt.Println(err2)
		}

	}
	// fmt.Println("BATCH OF 20 READ SUCESSFUL FOR THEREAD: ", Threadno)
	WriteInDB(cur, randomIdAndDate, Threadno)

}

//-----------------------------------------------------------------------------------------------------------
func WriteInDB(allData []record, randomIdAndDate []DateId, Threadno int) {
	if countUpdated > TOTAL_ITEMS {
		return
	}
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(AccessKey, SecretKey, ""),
	})
	if err != nil {
		fmt.Println(err)
	}
	svc := dynamodb.New(sess)

	prams := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			"BorderGo2": {
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[0].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[0].Date),
							},
							"day": {
								S: aws.String(allData[0].Day),
							},
							"month": {
								S: aws.String(allData[0].Month),
							},
							"year": {
								S: aws.String(allData[0].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[0].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[0].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[0].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[0].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[0].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[0].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[0].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[0].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[1].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[1].Date),
							},
							"day": {
								S: aws.String(allData[1].Day),
							},
							"month": {
								S: aws.String(allData[1].Month),
							},
							"year": {
								S: aws.String(allData[1].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[1].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[1].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[1].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[1].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[1].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[1].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[1].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[1].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[2].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[2].Date),
							},
							"day": {
								S: aws.String(allData[2].Day),
							},
							"month": {
								S: aws.String(allData[2].Month),
							},
							"year": {
								S: aws.String(allData[2].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[2].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[2].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[2].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[2].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[2].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[2].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[2].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[2].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[3].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[3].Date),
							},
							"day": {
								S: aws.String(allData[3].Day),
							},
							"month": {
								S: aws.String(allData[3].Month),
							},
							"year": {
								S: aws.String(allData[3].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[3].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[3].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[3].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[3].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[3].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[3].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[3].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[3].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[4].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[4].Date),
							},
							"day": {
								S: aws.String(allData[4].Day),
							},
							"month": {
								S: aws.String(allData[4].Month),
							},
							"year": {
								S: aws.String(allData[4].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[4].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[4].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[4].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[4].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[4].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[4].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[4].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[4].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},

				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[5].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[5].Date),
							},
							"day": {
								S: aws.String(allData[5].Day),
							},
							"month": {
								S: aws.String(allData[5].Month),
							},
							"year": {
								S: aws.String(allData[5].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[5].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[5].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[5].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[5].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[5].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[5].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[5].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[5].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[6].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[6].Date),
							},
							"day": {
								S: aws.String(allData[6].Day),
							},
							"month": {
								S: aws.String(allData[6].Month),
							},
							"year": {
								S: aws.String(allData[6].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[6].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[6].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[6].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[6].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[6].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[6].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[6].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[6].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[7].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[7].Date),
							},
							"day": {
								S: aws.String(allData[7].Day),
							},
							"month": {
								S: aws.String(allData[7].Month),
							},
							"year": {
								S: aws.String(allData[7].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[7].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[7].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[7].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[7].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[7].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[7].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[7].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[7].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[8].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[8].Date),
							},
							"day": {
								S: aws.String(allData[8].Day),
							},
							"month": {
								S: aws.String(allData[8].Month),
							},
							"year": {
								S: aws.String(allData[8].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[8].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[8].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[8].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[8].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[8].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[8].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[8].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[8].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[9].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[9].Date),
							},
							"day": {
								S: aws.String(allData[9].Day),
							},
							"month": {
								S: aws.String(allData[9].Month),
							},
							"year": {
								S: aws.String(allData[9].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[9].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[9].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[9].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[9].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[9].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[9].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[9].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[9].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[10].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[10].Date),
							},
							"day": {
								S: aws.String(allData[10].Day),
							},
							"month": {
								S: aws.String(allData[10].Month),
							},
							"year": {
								S: aws.String(allData[10].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[10].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[10].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[10].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[10].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[10].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[10].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[10].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[10].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[11].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[11].Date),
							},
							"day": {
								S: aws.String(allData[11].Day),
							},
							"month": {
								S: aws.String(allData[11].Month),
							},
							"year": {
								S: aws.String(allData[11].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[11].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[11].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[11].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[11].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[11].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[11].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[11].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[11].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[12].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[12].Date),
							},
							"day": {
								S: aws.String(allData[12].Day),
							},
							"month": {
								S: aws.String(allData[12].Month),
							},
							"year": {
								S: aws.String(allData[12].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[12].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[12].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[12].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[12].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[12].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[12].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[12].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[12].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[13].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[13].Date),
							},
							"day": {
								S: aws.String(allData[13].Day),
							},
							"month": {
								S: aws.String(allData[13].Month),
							},
							"year": {
								S: aws.String(allData[13].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[13].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[13].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[13].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[13].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[13].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[13].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[13].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[13].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[14].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[14].Date),
							},
							"day": {
								S: aws.String(allData[14].Day),
							},
							"month": {
								S: aws.String(allData[14].Month),
							},
							"year": {
								S: aws.String(allData[14].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[14].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[14].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[14].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[14].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[14].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[14].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[14].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[14].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[15].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[15].Date),
							},
							"day": {
								S: aws.String(allData[15].Day),
							},
							"month": {
								S: aws.String(allData[15].Month),
							},
							"year": {
								S: aws.String(allData[15].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[15].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[15].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[15].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[15].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[15].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[15].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[15].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[15].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[16].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[16].Date),
							},
							"day": {
								S: aws.String(allData[16].Day),
							},
							"month": {
								S: aws.String(allData[16].Month),
							},
							"year": {
								S: aws.String(allData[16].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[16].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[16].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[16].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[16].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[16].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[16].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[16].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[16].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[17].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[17].Date),
							},
							"day": {
								S: aws.String(allData[17].Day),
							},
							"month": {
								S: aws.String(allData[17].Month),
							},
							"year": {
								S: aws.String(allData[17].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[17].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[17].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[17].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[17].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[17].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[17].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[17].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[17].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[18].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[18].Date),
							},
							"day": {
								S: aws.String(allData[18].Day),
							},
							"month": {
								S: aws.String(allData[18].Month),
							},
							"year": {
								S: aws.String(allData[18].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[18].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[18].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[18].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[18].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[18].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[18].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[18].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[18].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"Id": {
								N: aws.String(randomIdAndDate[19].Id),
							},
							"dateRep": {
								S: aws.String(randomIdAndDate[19].Date),
							},
							"day": {
								S: aws.String(allData[19].Day),
							},
							"month": {
								S: aws.String(allData[19].Month),
							},
							"year": {
								S: aws.String(allData[19].Year),
							},
							"cases": {
								N: aws.String(strconv.Itoa(allData[19].Cases)),
							},
							"deaths": {
								N: aws.String(strconv.Itoa(allData[19].Deaths)),
							},
							"countriesAndTerritories": {
								S: aws.String(allData[19].CountriesAndTerritories),
							},
							"geoId": {
								S: aws.String(allData[19].GeoId),
							},
							"countryterritoryCode": {
								S: aws.String(allData[19].CountryterritoryCode),
							},
							"popData2019": {
								N: aws.String(strconv.Itoa(allData[19].PopData2019)),
							},
							"continentExp": {
								S: aws.String(allData[19].ContinentExp),
							},
							"cumulative_number_for_14_days_of_COVID_19_cases_per_100000": {
								S: aws.String(allData[19].Cumulative_number_for_14_days_of_COVID_19_cases_per_100000),
							},
						},
					},
				},
			},
		},
	}

	_, err = svc.BatchWriteItem(prams)
	if err != nil {
		fmt.Println("Got error calling BatchWrite: ", err)
		return
	}
	// fmt.Println("BATCH OF 20 WRITE SUCCESFULL FOR THREAD NO", Threadno)
	countUpdated += 20
	fmt.Println("Current completed items ", countUpdated, "Printed by ThreadNO", Threadno)
}

//---------------------------------------------------------------------------------------------------------------------------------------

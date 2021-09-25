package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
)

type data struct {
	Id                      string
	DateRep                 string
	CountriesAndTerritories string
	GeoId                   string
	Operation               string
}

func main() {
	lambda.Start(Handler)
}

func Handler(ctx context.Context, e events.DynamoDBEvent) {

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})
	if err != nil {
		fmt.Println(err)
	}
	for _, record := range e.Records {

		if record.EventName == "MODIFY" {
			r := record.Change.NewImage

			temp := data{
				Id:                      r["Id"].Number(),
				DateRep:                 r["dateRep"].String(),
				CountriesAndTerritories: r["countriesAndTerritories"].String(),
				GeoId:                   r["geoId"].String(),
				Operation:               record.EventName,
			}

			body, err := json.Marshal(temp)

			if err != nil {
				fmt.Println(err)
				continue
			}

			fmt.Println("buffer", bytes.NewBuffer(body))

			uploader := s3manager.NewUploader(sess)
			u := uuid.New()
			date := strings.Replace(temp.DateRep, "/", "-", 3)

			key := fmt.Sprintf("dbstream/%s/%s.json", date, u.String())

			_, ierr := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String("borderdynamodbstream"),
				Key:    aws.String(key),
				Body:   bytes.NewReader(body),
			})

			if ierr != nil {
				log.Printf("There was an issue uploading to s3: %s", ierr.Error())

			}

		}
	}
}

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
)

const BUCKET_NAME_FOR_READ string = "borderdynamodbstream"
const BUCKET_FOLDER_NAME_FOR_READ string = "dbstream"
const BUCKET_NAME_FOR_WRITE string = "mergefilesbucket"
const BUCKET_FOLDER_NAME_FOR_WRITE string = "merged"

func main() {

	lambda.Start(Handler)
	// Handler()
}

// request events.APIGatewayProxyRequest
func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	fmt.Println("printng", request.Body)

	if request.HTTPMethod != "POST" {
		errName := errors.New("Method not allowed")
		ApiResponse := events.APIGatewayProxyResponse{StatusCode: 403}
		return ApiResponse, errName
	}

	var date string = request.Body
	if len(date) != 10 {
		errName := errors.New("Please Provide a valid Date in FORMAT: dd-mm-yyyy")
		ApiResponse := events.APIGatewayProxyResponse{StatusCode: 403}
		return ApiResponse, errName
	}
	var pathtos3 string = mergeFilesByDate(date)
	var stringReponse string = fmt.Sprintf("Files merged to S3 success, BucketName: %s, path: %s ", BUCKET_NAME_FOR_WRITE, pathtos3)
	ApiResponse := events.APIGatewayProxyResponse{Body: stringReponse, StatusCode: 200}
	return ApiResponse, nil

}
func mergeFilesByDate(date string) string {

	var FILE_TO_SEARCH = fmt.Sprintf("%v/%v", BUCKET_FOLDER_NAME_FOR_READ, date)

	s3session := s3.New(session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})))

	resp, err2 := s3session.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(BUCKET_NAME_FOR_READ),
		Prefix: aws.String(FILE_TO_SEARCH),
		// Prefix: aws.String(BUCKET_FOLDER_NAME_FOR_READ),
	})

	if err2 != nil {
		panic(err2)
	}
	// fileName := *resp.Contents[0].Key

	data := make([]interface{}, 0)

	for _, v := range resp.Contents {

		fileName := *v.Key
		temp := getObject(fileName)
		data = append(data, temp)

	}

	body, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	//-----------------------WRITE--------------------------

	sess, err2 := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})

	uploader := s3manager.NewUploader(sess)
	u := uuid.New()
	timeStamp := getFormattedTime()
	key := fmt.Sprintf("%s/%s/%s.json", BUCKET_FOLDER_NAME_FOR_WRITE, timeStamp, u.String())

	_, ierr := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(BUCKET_NAME_FOR_WRITE),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})

	if ierr != nil {
		fmt.Printf("There was an issue uploading to s3: %s", ierr.Error())

	}
	return key

}

func getObject(filename string) map[string]interface{} {

	s3session := s3.New(session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})))

	resp, err := s3session.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET_NAME_FOR_READ),
		Key:    aws.String(filename),
	})

	if err != nil {
		panic(err)
	}

	body, err := ioutil.ReadAll(resp.Body)

	data := make(map[string]interface{}, 0)
	json.Unmarshal([]byte(body), &data)

	return data
}

func getFormattedTime() string {
	currentTime := time.Now()
	year := currentTime.Year()
	month := currentTime.Month()
	day := currentTime.Day()
	hour := currentTime.Hour()
	min := currentTime.Minute()

	var daystr string = fmt.Sprintf("%d", day)
	var monthstr string = fmt.Sprintf("%d", month)
	if day < 10 {
		daystr = fmt.Sprintf("0%d", day)

	}

	if month < 10 {
		monthstr = fmt.Sprintf("0%d", month)

	}
	var time string = fmt.Sprintf("%d-%s-%s-%d-%d", year, monthstr, daystr, hour, min)

	return time
}

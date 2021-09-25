package main

import (
	"fmt"
	"time"

	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
)

var redshiftclient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI

type Redshift_Event struct {
	Redshift_cluster_id string `json:"redshift_cluster_id"`
	Redshift_database   string `json:"redshift_database"`
	Redshift_user       string `json:"redshift_user"`
	Redshift_iam_role   string `json:"redshift_iam_role"`
	Run_type            string `json:"run_type"`
}

func main() {

	lambda.Start(Handler)
}

func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})
	if err != nil {
		fmt.Println(err)
		stringResp := "Cannot create sess With Redshift"
		ApiResponse := events.APIGatewayProxyResponse{Body: stringResp, StatusCode: 500}
		return ApiResponse, err
	}

	redshiftclient = redshiftdataapiservice.New(sess)

	redshift_cluster_id := "borderfree"

	redshift_database := "dev"

	redshift_user := "awsuser"

	run_type := "synchronous"

	if run_type != "synchronous" && run_type != "asynchronous" {

		fmt.Println("Invalid Event run_type. \n run_type has to be synchronous or asynchronous.")
	}

	isSynchronous := false
	if run_type == "synchronous" {
		isSynchronous = true
	} else {
		isSynchronous = false
	}
	fmt.Println("Run-Type Mode ", run_type)

	command := request.HTTPMethod
	sql_statement := request.Body

	FinalResp := execute_sql_data_api(redshift_database, command, sql_statement, redshift_user, redshift_cluster_id, isSynchronous)

	fmt.Println(FinalResp)

	ApiResponse := events.APIGatewayProxyResponse{Body: FinalResp, StatusCode: 200}
	return ApiResponse, err

}

func execute_sql_data_api(redshift_database string, command string, query string, redshift_user string, redshift_cluster_id string, isSynchronous bool) string {
	var max_wait_cycles = 20
	var attempts = 0
	var query_status = ""
	done := false

	execstmt_req, execstmt_err := redshiftclient.ExecuteStatement(&redshiftdataapiservice.ExecuteStatementInput{
		ClusterIdentifier: aws.String(redshift_cluster_id),
		DbUser:            aws.String(redshift_user),
		Database:          aws.String(redshift_database),
		Sql:               aws.String(query),
	})

	if execstmt_err != nil {

		fmt.Println(execstmt_err)
	}

	descstmt_req, descstmt_err := redshiftclient.DescribeStatement(&redshiftdataapiservice.DescribeStatementInput{
		Id: execstmt_req.Id,
	})
	query_status = aws.StringValue(descstmt_req.Status)

	if descstmt_err != nil {

		fmt.Println(descstmt_err)
	}

	var successResp string
	for done == false && isSynchronous && attempts < max_wait_cycles {
		attempts += 1
		time.Sleep(1 * time.Second)
		descstmt_req, descstmt_err := redshiftclient.DescribeStatement(&redshiftdataapiservice.DescribeStatementInput{
			Id: execstmt_req.Id,
		})
		query_status = aws.StringValue(descstmt_req.Status)

		if query_status == "FAILED" {

			fmt.Println("Query status: ", query_status, " .... for query--> ", query)
		} else if query_status == "FINISHED" {
			fmt.Println("Query status: ", query_status, " .... for query--> ", query)
			done = true

			if *descstmt_req.HasResultSet {
				getresult_req, getresult_err := redshiftclient.GetStatementResult(&redshiftdataapiservice.GetStatementResultInput{
					Id: execstmt_req.Id,
				})

				if getresult_err != nil {

					fmt.Println(getresult_err)
				}
				// fmt.Printf("%T", getresult_req.Records)

				successResp = parsequeryresponse(getresult_req.Records)
			}
		} else {
			fmt.Println("Currently working... query status: ", query_status, " .... for query--> ", query)
		}

		if descstmt_err != nil {

			fmt.Println(descstmt_err)
		}
	}

	//Timeout Precaution
	if done == false && attempts >= max_wait_cycles && isSynchronous {
		fmt.Println("Query status: ", query_status, " .... for query--> ", query)

		fmt.Println("Limit for max_wait_cycles has been reached before the query was able to finish. We have exited out of the while-loop. You may increase the limit accordingly.")
	}
	//fmt.Println(query_status)
	return successResp
}

type record struct {
	Id                      string
	DateRep                 string
	CountriesAndTerritories string
	GeoId                   string
	Operation               string
}

func parsequeryresponse(res [][]*redshiftdataapiservice.Field) string {

	var arr []record
	for i := 0; i < len(res); i++ {

		var temp record

		temp.Id = *res[i][3].StringValue
		temp.DateRep = *res[i][1].StringValue
		temp.CountriesAndTerritories = *res[i][0].StringValue
		temp.GeoId = *res[i][2].StringValue
		temp.Operation = *res[i][4].StringValue

		arr = append(arr, temp)
	}

	resJson, err := json.Marshal(arr)
	if err != nil {
		fmt.Println("Cannot encode to JSON ", err)
	}
	// fmt.Printf("%T", resJson)

	// fmt.Fprintf(os.Stdout, "%s", resJson)
	return string(resJson)
}

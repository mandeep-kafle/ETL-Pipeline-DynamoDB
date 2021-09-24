package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
	"github.com/cevaris/ordered_map"
)

// Declare redshiftclient client
var redshiftclient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI

type Redshift_Event struct {
	Redshift_cluster_id string `json:"redshift_cluster_id"`
	Redshift_database   string `json:"redshift_database"`
	Redshift_user       string `json:"redshift_user"`
	Redshift_iam_role   string `json:"redshift_iam_role"`
	Run_type            string `json:"run_type"`
}

func main() {
	// Create session

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	redshiftclient = redshiftdataapiservice.New(sess)

	Handler()
}

func Handler() (string, error) {
	fmt.Println("Inside Go Handler function!")

	final_resp := ""
	responses := ordered_map.NewOrderedMap()

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

	// Initiate OrderedMap key value pair for query and its type
	sql_statements := ordered_map.NewOrderedMap()

	sql_statements.Set("SELECT", "SELECT * FROM borders3dataschema.dbstreamborderfree;")

	fmt.Println("Running sql queries in ", run_type, " mode!")

	// Iterating over ordered map to execute each sql statement
	iter := sql_statements.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() {
		command := kv.Key.(string)
		query := kv.Value.(string)
		fmt.Println("Example of ", command, ":")
		fmt.Println("Running Query ", query)
		responses.Set(command, execute_sql_data_api(redshift_database, command, query, redshift_user, redshift_cluster_id, isSynchronous))
	}

	// returning resultset in execution ordered fashion
	iter1 := responses.IterFunc()
	for kv, ok := iter1(); ok; kv, ok = iter1() {
		command := kv.Key.(string)
		status := kv.Value.(string)
		final_resp += command + ":" + status + " | "
	}

	fmt.Println(final_resp)
	return final_resp, nil
}

func execute_sql_data_api(redshift_database string, command string, query string, redshift_user string, redshift_cluster_id string, isSynchronous bool) string {
	var max_wait_cycles = 20
	var attempts = 0
	var query_status = ""
	done := false

	// Calling Redshift Data API with executeStatement()
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

				fmt.Println(getresult_req.Records)
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
	fmt.Println(query_status)
	return query_status
}

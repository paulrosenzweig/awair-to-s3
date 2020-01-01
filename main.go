package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// lambda trigger event
type Event struct {
	Time time.Time `json:"time"`
}

// the Awair API response contains an array of this type
type timepoint struct {
	Timestamp time.Time `json:"timestamp"`
	Sensors   []struct {
		Comp  string  `json:"comp"`
		Value float64 `json:"value"`
	} `json:"sensors"`
}

func main() { lambda.Start(handleRequest) }

// `handleRequest` is the primary function. It decides which hour to query based
// on the timestamp in the lambda event and calls out to other functions to
// query Awair, format the data, and upload it to s3.
func handleRequest(event Event) (string, error) {
	log.Printf("HandleRequest: %v\n", event)

	to := event.Time.Truncate(time.Hour)
	from := to.Add(-time.Hour)
	log.Printf("Time range: %s - %s\n", from, to)

	data, err := getData(from, to)
	if err != nil {
		return "", err
	}
	log.Printf("Retrieved %d rows\n", len(data))

	csvData := &bytes.Buffer{}

	err = writeData(data, csvData)
	if err != nil {
		return "", err
	}
	filename := to.Format(time.RFC3339)

	uploadLocation, err := uploadData(filename, csvData)
	if err != nil {
		return "", fmt.Errorf("failed to upload file, %v", err)
	}

	return fmt.Sprintf("Saved data for %s - %s to %s", from, to, uploadLocation), nil
}

// `getData` makes a request to Awair for a time range: `from` - `to`. It's
// unclear whether Awair's API treats one or both of those endpoints as
// inclusive. The returned JSON is parsed into a slice of `timepoint` structs.
func getData(from, to time.Time) ([]timepoint, error) {
	req, err := getReq(from, to)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response := struct {
		Data []timepoint `json:"data"`
	}{}
	err = json.NewDecoder(resp.Body).Decode(&response)
	return response.Data, err
}

// `getReq` creates the request using these environment variables:
// DEVICE_TYPE, DEVICE_ID, and AWAIR_API_KEY
func getReq(from, to time.Time) (*http.Request, error) {
	vals := url.Values{}
	vals.Set("fahrenheit", "false")
	vals.Set("from", from.Format(time.RFC3339))
	vals.Set("to", to.Format(time.RFC3339))

	path := fmt.Sprintf(
		"/v1/users/self/devices/%s/%s/air-data/raw",
		os.Getenv("DEVICE_TYPE"),
		os.Getenv("DEVICE_ID"),
	)

	u := url.URL{
		Scheme:   "https",
		Host:     "developer-apis.awair.is",
		Path:     path,
		RawQuery: vals.Encode(),
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", os.Getenv("AWAIR_API_KEY")))
	return req, nil
}

const ATHENA_TIMESTAMP_FORMAT = "2006-01-02 15:04:05.000000"

// `writeData` formats the data as csv with dates that can be parsed by Athena
func writeData(data []timepoint, output io.Writer) error {
	w := csv.NewWriter(output)
	for _, d := range data {
		ts := d.Timestamp.Format(ATHENA_TIMESTAMP_FORMAT)
		for _, s := range d.Sensors {
			w.Write([]string{
				ts,
				s.Comp,
				strconv.FormatFloat(s.Value, 'f', -1, 64),
			})
		}
	}
	w.Flush()
	return w.Error()
}

// `uploadData` saves the file to S3
func uploadData(filename string, csvData io.Reader) (string, error) {
	sess := session.Must(session.NewSession())
	uploader := s3manager.NewUploader(sess)

	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(os.Getenv("BUCKET")),
		Key:    aws.String(fmt.Sprintf("airdata/%s.csv", filename)),
		Body:   csvData,
	})
	if err != nil {
		return "", err
	}
	return result.Location, nil
}

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

func main() {
	godotenv.Load()
	var (
		awsAccessKeyID     = os.Getenv("AWS_ACCESS_KEY_ID")
		awsSecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		bucketName         = os.Getenv("S3_BUCKET")
		uploader           = NewUploader(awsAccessKeyID, awsSecretAccessKey, bucketName)
		ctx                = context.Background()
	)

	multipartUpload, err := uploader.CreateMultipartUpload(ctx, &CreateMultipartUploadReq{
		FileName: "test.csv",
		FileType: "csv",
	})
	if err != nil {
		log.Fatalf("CreateMultipartUpload failed %v", err)
	}

	defer func() {
		if err == nil {
			return
		}

		_, err = uploader.AbortMultipartUpload(ctx, &AbortMultipartUploadReq{
			UploadKey: multipartUpload.UploadKey,
			UploadID:  multipartUpload.UploadID,
		})
		if err != nil {
			log.Printf("[ERR] CreateMultipartUpload failed %v", err)
		}
	}()

	uploadParts := []*MultipartUploadPartRes{}

	for idx := 1; idx < 100; idx++ {
		uploadPart, err := uploader.MultipartUploadPart(ctx, &MultipartUploadPartReq{
			UploadKey:   multipartUpload.UploadKey,
			UploadID:    multipartUpload.UploadID,
			FileContent: MockData(idx),
			PartNumber:  int64(idx),
		})
		if err != nil {
			log.Printf("[ERR] MultipartUploadPart failed %v", err)
			return
		}

		log.Printf("[INFO] MultipartUploadPart %d", idx)
		uploadParts = append(uploadParts, uploadPart)
	}

	completeUpload, err := uploader.CompleteMultipartUpload(ctx, &CompleteMultipartUploadReq{
		UploadKey:      multipartUpload.UploadKey,
		UploadID:       multipartUpload.UploadID,
		CompletedParts: uploadParts,
	})
	if err != nil {
		log.Printf("[ERR] CompleteMultipartUpload failed %v", err)
	} else {
		log.Printf("[INFO] CompleteMultipartUpload %s", completeUpload.URL)
	}
}

func MockData(lastPartNumber int) []byte {
	data := [][]string{}
	for idx := 0; idx < 1000000; idx++ {
		item := []string{
			fmt.Sprintf("%d", 1*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 2*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 3*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 4*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 5*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 6*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 7*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 8*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 9*lastPartNumber+idx*10),
			fmt.Sprintf("%d", 10*lastPartNumber+idx*10),
		}
		data = append(data, item)
	}

	bb := &bytes.Buffer{}
	csvWriter := csv.NewWriter(transform.NewWriter(bb, unicode.UTF8BOM.NewEncoder()))

	err := csvWriter.WriteAll(data)
	if err != nil {
		log.Fatalf("failed to csv write data %v", err)
	}

	return bb.Bytes()
}

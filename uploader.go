package main

import (
	"bytes"
	"context"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	maxRetries = 3
)

type Uploader interface {
	CreateMultipartUpload(ctx context.Context, req *CreateMultipartUploadReq) (*CreateMultipartUploadRes, error)
	MultipartUploadPart(ctx context.Context, req *MultipartUploadPartReq) (*MultipartUploadPartRes, error)
	CompleteMultipartUpload(ctx context.Context, req *CompleteMultipartUploadReq) (*CompleteMultipartUploadRes, error)
	AbortMultipartUpload(ctx context.Context, req *AbortMultipartUploadReq) (*AbortMultipartUploadRes, error)
}

type upload struct {
	s3Svc      *s3.S3
	bucketName string
}

type CreateMultipartUploadReq struct {
	FileName string
	FileType string
}

type CreateMultipartUploadRes struct {
	UploadKey string
	UploadID  string
}

type MultipartUploadPartReq struct {
	UploadKey   string
	UploadID    string
	FileContent []byte
	PartNumber  int64
}

type MultipartUploadPartRes struct {
	Etag       string
	PartNumber int64
}

type CompleteMultipartUploadReq struct {
	UploadKey      string
	UploadID       string
	CompletedParts []*MultipartUploadPartRes
}

type CompleteMultipartUploadRes struct {
	URL string
}

type AbortMultipartUploadReq struct {
	UploadKey string
	UploadID  string
}

type AbortMultipartUploadRes struct{}

func NewUploader(awsAccessKeyID, awsSecretAccessKey, bucketName string) Uploader {
	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		log.Fatalf("aws bad credentials: %v", err)
	}

	cfg := aws.NewConfig().WithRegion("ap-northeast-1").WithCredentials(creds)
	s, err := session.NewSession(cfg)
	if err != nil {
		log.Fatalf("aws failed to new session: %v", err)
	}

	return &upload{
		s3Svc:      s3.New(s),
		bucketName: bucketName,
	}
}

func (u *upload) CreateMultipartUpload(ctx context.Context, req *CreateMultipartUploadReq) (*CreateMultipartUploadRes, error) {
	path := "/multipart-upload" + req.FileName
	createInput := s3.CreateMultipartUploadInput{
		Bucket:      &u.bucketName,
		Key:         &path,
		ContentType: &req.FileType,
	}

	createOuput, err := u.s3Svc.CreateMultipartUpload(&createInput)
	if err != nil {
		return nil, err
	}

	return &CreateMultipartUploadRes{
		UploadKey: *createOuput.Key,
		UploadID:  *createOuput.UploadId,
	}, nil
}

func (u *upload) MultipartUploadPart(ctx context.Context, req *MultipartUploadPartReq) (*MultipartUploadPartRes, error) {
	partInput := s3.UploadPartInput{
		Body:          bytes.NewReader(req.FileContent),
		Bucket:        &u.bucketName,
		Key:           &req.UploadKey,
		PartNumber:    &req.PartNumber,
		UploadId:      &req.UploadID,
		ContentLength: aws.Int64(int64(len(req.FileContent))),
	}

	var err error

	for tryNum := 0; tryNum < maxRetries; tryNum++ {
		var uploadRes *s3.UploadPartOutput
		uploadRes, err = u.s3Svc.UploadPart(&partInput)
		if err != nil {
			log.Printf("[ERR] failed to upload part %v", err)
			continue
		}

		return &MultipartUploadPartRes{
			Etag:       *uploadRes.ETag,
			PartNumber: req.PartNumber,
		}, nil

	}

	return nil, err
}

func (u *upload) CompleteMultipartUpload(ctx context.Context, req *CompleteMultipartUploadReq) (*CompleteMultipartUploadRes, error) {
	completeInput := s3.CompleteMultipartUploadInput{
		Bucket:   &u.bucketName,
		Key:      &req.UploadKey,
		UploadId: &req.UploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: func() []*s3.CompletedPart {
				parts := make([]*s3.CompletedPart, 0, len(req.CompletedParts))

				for idx := range req.CompletedParts {
					item := req.CompletedParts[idx]
					parts = append(parts, &s3.CompletedPart{
						ETag:       &item.Etag,
						PartNumber: &item.PartNumber,
					})
				}

				return parts
			}(),
		},
	}

	completeOutput, err := u.s3Svc.CompleteMultipartUpload(&completeInput)
	if err != nil {
		return nil, err
	}

	return &CompleteMultipartUploadRes{
		URL: completeOutput.String(),
	}, nil
}

func (u *upload) AbortMultipartUpload(ctx context.Context, req *AbortMultipartUploadReq) (*AbortMultipartUploadRes, error) {
	abortInput := s3.AbortMultipartUploadInput{
		Bucket:   &u.bucketName,
		Key:      &req.UploadKey,
		UploadId: &req.UploadID,
	}

	_, err := u.s3Svc.AbortMultipartUpload(&abortInput)

	return nil, err
}

package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	aws_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func ingestS3Source(r *Retriever, ic chan []driver.Value) error {
	connConfig := r.Source.Connection
	filename := "users/mock-user-data-1.csv"
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(connConfig.Host),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(connConfig.Username, connConfig.Password, "")),
	)

	if err != nil {
		return err
	}

	s3Client := aws_s3.NewFromConfig(cfg)

	var partMiB int64 = 10
	var MiBinBytes int64 = 1024 * 1024
	downloadManager := manager.NewDownloader(s3Client, func(d *manager.Downloader) {
		d.PartSize = partMiB * MiBinBytes
	})

	// Should this be preeallocatted?
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err = downloadManager.Download(ctx, buffer, &aws_s3.GetObjectInput{
		Bucket: aws.String(connConfig.Database),
		Key:    aws.String(filename),
	})

	if err != nil {
		return fmt.Errorf("failed to download S3 file %w", err)
	}
	fmt.Println(string(buffer.Bytes()))

	return nil
}

func listObjects(s3Client *aws_s3.Client, bucketName string) {
	input := &aws_s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	result, err := s3Client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		log.Fatalf("unable to list items in bucket %q, %v", bucketName, err)
	}

	for _, item := range result.Contents {
		fmt.Printf("Name: %s, Last modified: %s, Size: %d\n", *item.Key, item.LastModified, item.Size)
	}
}

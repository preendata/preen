package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aws_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	// Hardcoded AWS credentials and bucket information
	awsAccessKeyID := "AKIAW3MD6XDBU4ED7MKY"
	awsSecretAccessKey := "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"
	awsRegion := "us-east-2"
	bucketName := "hyphadb-internal-transfer"

	// Load the configuration with hardcoded credentials
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsAccessKeyID, awsSecretAccessKey, "")),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Create an S3 client
	s3Client := aws_s3.NewFromConfig(cfg)

	// List objects in the specified bucket
	listObjects(s3Client, bucketName)
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

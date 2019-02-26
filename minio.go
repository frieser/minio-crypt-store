package minio

import (
	"fmt"
	"github.com/minio/minio-go"
	"github.com/xordataexchange/crypt/backend"
	"log"
	"os"
)

type Client struct {
	client    *minio.Client
	waitIndex uint64
}

func New(machines []string) (*Client, error) {
	var endpoint string

	if len(machines) == 0 {
		return nil, fmt.Errorf("no Minio hosts supplied")
	}

	if len(machines) > 0 {
		endpoint = machines[0]
	}
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY_ID")

	if accessKeyID == "" {
		log.Printf("Neither MINIO_ACCESS_KEY_ID or a MINIO_ACCESS_KEY_ID/MINIO_SECRET_ACCESS_KEY are set. Can't auth to minio.")
		return nil, fmt.Errorf("can't Auth to Minio")
	}
	secretAccessKey := os.Getenv("MINIO_SECRET_ACCESS_KEY");
	if secretAccessKey == "" {
		log.Printf("MINIO_ACCESS_KEY_ID set but MINIO_SECRET_ACCESS_KEY is empty. Can't auth to minio.")
		return nil, fmt.Errorf("can't Auth to Minio")
	}
	mc, err := minio.New(endpoint, accessKeyID, secretAccessKey, true)

	if err != nil {
		return nil, err
	}

	return &Client{client: mc, waitIndex: 0}, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	bucketName := os.Getenv("MINIO_BUCKET_NAME")

	if bucketName == "" {
		log.Printf("No MINIO_BUCKET_NAME are set. Can't reach the Minio bucket.")
		return nil, fmt.Errorf("can't reach the Minio bucket")
	}

	reader, err := c.client.GetObject(bucketName, key, minio.GetObjectOptions{})

	if err != nil {
		return nil, err
	}
	defer reader.Close()

	objectInfo, err := reader.Stat()

	if err != nil {
		return nil, err
	}
	var b []byte
	var n int

	n, err = reader.Read(b)

	if err != nil {
		return nil, err
	}
	if n != int(objectInfo.Size) {
		return nil, fmt.Errorf("the size of the object doesn't match the object readed")
	}

	return b, nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	return nil, nil
}

func (c *Client) Set(key string, value []byte) error {
	return nil
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	return nil
}

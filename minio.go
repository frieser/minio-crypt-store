package minio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go"
	"github.com/xordataexchange/crypt/backend"
	"log"
	"os"
	"strings"
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
	mc, err := minio.New(endpoint, accessKeyID, secretAccessKey, false)
	
	if err != nil {
		return nil, err
	}

	return &Client{client: mc, waitIndex: 0}, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	pairsB := make([]byte, 0)
	pairsJson := make(map[string]interface{})
	bucketName := os.Getenv("MINIO_BUCKET_NAME")
	rootPath := os.Getenv("MINIO_ROOT_PATH")
	
	if bucketName == "" {
		log.Printf("No MINIO_BUCKET_NAME are set. Can't reach the Minio bucket.")
		return nil, fmt.Errorf("can't reach the Minio bucket")
	}
	KVPairs, err := c.List(key)

	if err != nil {
		return nil, err
	}
	for _, v := range KVPairs {
		value := make(map[string]interface{})
		err := json.Unmarshal(v.Value, &value)

		if err != nil {
			continue
		}
		// remove .json extension and root path to clean the keys
		key := strings.Replace(v.Key, ".json", "", 1)
		key = strings.Replace(key, rootPath, "", 1)
		
		pairsJson[key] = value
	}
	pairsB, err = json.Marshal(pairsJson)

	if err != nil {
		return nil, err
	}

	return pairsB, nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	done := make(chan struct{})

	defer close(done)

	bucketName := os.Getenv("MINIO_BUCKET_NAME")

	if bucketName == "" {
		log.Printf("No MINIO_BUCKET_NAME are set. Can't reach the Minio bucket.")
		return nil, fmt.Errorf("can't reach the Minio bucket")
	}
	objects := c.client.ListObjects(bucketName, key, true, done)
	retList := make(backend.KVPairs, len(objects))

	for object := range objects {
		if object.Err != nil {
			return nil, object.Err
		}
		v, err := c.getPairValue(object.Key)

		if err != nil {
			return nil, err
		}
		pair := &backend.KVPair{Key: object.Key}
		pair.Value = v
		retList = append(retList, pair)
	}

	return retList, nil
}

func (c *Client) Set(key string, value []byte) error {
	bucketName := os.Getenv("MINIO_BUCKET_NAME")

	if bucketName == "" {
		log.Printf("No MINIO_BUCKET_NAME are set. Can't reach the Minio bucket.")
		return fmt.Errorf("can't reach the Minio bucket")
	}
	reader := bytes.NewReader(value)

	_, err := c.client.PutObject(bucketName, key, reader, reader.Size(), minio.PutObjectOptions{})

	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	response := make(chan *backend.Response, 0)

	bucketName := os.Getenv("MINIO_BUCKET_NAME")

	if bucketName == "" {
		log.Printf("No MINIO_BUCKET_NAME are set. Can't reach the Minio bucket.")

		response <- &backend.Response{Error: fmt.Errorf("can't reach the Minio bucket")}

		return response
	}

	go func() {
		// Create a done channel to control 'ListenBucketNotification' go routine.
		done := make(chan struct{})

		// Indicate a background go-routine to exit cleanly upon return.
		defer close(done)

		// Listen for bucket notifications on the bucket filtered by prefix, suffix and events.
		for notificationInfo := range c.client.ListenBucketNotification(bucketName, "", "", []string{
			"s3:ObjectCreated:*",
			"s3:ObjectRemoved:*",
		}, done) {
			if notificationInfo.Err != nil {
				response <- &backend.Response{Error: notificationInfo.Err}
				continue
			}
			value, err := c.getPairValue(key)

			if err != nil {
				response <- &backend.Response{Error: err}
			}
			response <- &backend.Response{Value: value}
		}
	}()

	return response
}

func (c *Client) getPairValue(key string) ([]byte, error) {
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
	b := make([]byte, objectInfo.Size)
	var n int

	n, err = reader.Read(b)

	if err != nil && err.Error() != "EOF" {
		return nil, err
	}
	if n != int(objectInfo.Size) {
		return nil, fmt.Errorf("the size of the object doesn't match the object readed")
	}

	return b, nil
}

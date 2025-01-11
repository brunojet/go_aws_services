package session

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	sess *session.Session
	once sync.Once
)

func initAWSSession() {
	var err error
	sess, err = session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create session: %v", err))
	}
}

func GetAWSSession() *session.Session {
	once.Do(initAWSSession)
	return sess
}

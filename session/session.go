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

var newSession = session.NewSession // função auxiliar para criar a sessão

func initAWSSession() {
	var err error
	sess, err = newSession(&aws.Config{
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

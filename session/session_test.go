package session

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock para a função session.NewSession
type mockSession struct {
	mock.Mock
}

func (m *mockSession) NewSession(cfgs ...*aws.Config) (*session.Session, error) {
	args := m.Called(cfgs)
	return args.Get(0).(*session.Session), args.Error(1)
}

func TestGetAWSSession(t *testing.T) {
	// Criar um mock para a função session.NewSession
	mockSess := new(mockSession)
	mockSess.On("NewSession", mock.AnythingOfType("[]*aws.Config")).Return(&session.Session{}, nil)

	// Substituir a função NewSession pelo mock
	originalNewSession := newSession
	newSession = func(cfgs ...*aws.Config) (*session.Session, error) {
		return mockSess.NewSession(cfgs...)
	}
	defer func() { newSession = originalNewSession }()

	// Chamar a função GetAWSSession
	sess := GetAWSSession()

	// Verificar se a sessão foi criada corretamente
	assert.NotNil(t, sess)
	mockSess.AssertExpectations(t)
}

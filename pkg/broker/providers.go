package broker

import (
	"errors"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
)

type Providers string

const (
	AWSESInstance   		Providers = "aws-es"
	Unknown        			Providers = "unknown"
)

func GetProvidersFromString(str string) Providers {
	if str == "aws-es" {
		return AWSESInstance
	}
	return Unknown
}

type ProviderPlan struct {
	basePlan               osb.Plan  `json:"-"` /* NEVER allow this to be serialized into a JSON call as it may accidently send sensitive info to callbacks */
	Provider               Providers `json:"provider"`
	providerPrivateDetails string    `json:"-"` /* NEVER allow this to be serialized into a JSON call as it may accidently send sensitive info to callbacks */
	ID                     string    `json:"id"`
	Scheme                 string    `json:"scheme"`
}

type Provider interface {
	GetInstance(string, *ProviderPlan) (*Instance, error)
	Provision(string, *ProviderPlan, string) (*Instance, error)
	Deprovision(*Instance, bool) error
	Modify(*Instance, *ProviderPlan) (*Instance, error)
	Tag(*Instance, string, string) error
	Untag(*Instance, string) error
	PerformPostProvision(*Instance) (*Instance, error)
	GetUrl(*Instance) map[string]interface{}
}

func GetProviderByPlan(namePrefix string, plan *ProviderPlan) (Provider, error) {
	if plan.Provider == AWSESInstance {
		return NewAWSInstanceESProvider(namePrefix)
	} else {
		return nil, errors.New("Unable to find provider for plan.")
	}
}

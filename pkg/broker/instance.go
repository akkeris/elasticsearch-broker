package broker

import (
	"reflect"
)

type Stat struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Instance struct {
	Id            string        `json:"id"`
	Name          string        `json:"name"`
	ProviderId    string        `json:"provider_id"`
	Plan          *ProviderPlan `json:"plan,omitempty"`
	Username      string        `json:"username"`
	Password      string        `json:"password"`
	Endpoint      string        `json:"endpoint"`
	Status        string        `json:"status"`
	Ready         bool          `json:"ready"`
	Engine        string        `json:"engine"`
	EngineVersion string        `json:"engine_version"`
	Scheme        string        `json:"scheme"`
}

type Entry struct {
	Id       string
	Name     string
	PlanId   string
	Claimed  bool
	Tasks	 int
	Status   string
	Username string
	Password string
	Endpoint string
}

func (i *Instance) Match(other *Instance) bool {
	return reflect.DeepEqual(i, other)
}

type ResourceUrlSpec struct {
	Username string
	Password string
	Endpoint string
	Plan     string
}

type ResourceSpec struct {
	Name string `json:"name"`
}

func IsAvailable(status string) bool {
	return status == "available"
}

func InProgress(status string) bool {
	return status == "upgrading" || status == "creating"
}

func CanGetBindings(status string) bool {
	return status != "deleted" && status != "creating"
}
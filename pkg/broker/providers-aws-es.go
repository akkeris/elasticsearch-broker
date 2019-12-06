package broker

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticsearchservice"
	"github.com/nu7hatch/gouuid"
	"os"
	"strings"
	"time"
	"fmt"
)

type AWSInstanceESProvider struct {
	Provider
	svc              	*elasticsearchservice.ElasticsearchService
	namePrefix          string
	instanceCache 		map[string]*Instance
}

func IsReady(status *elasticsearchservice.ElasticsearchDomainStatus) bool {
	return *status.Created == true && *status.Deleted == false && *status.UpgradeProcessing == false
}

func GetStatus(status *elasticsearchservice.ElasticsearchDomainStatus) string {
	if *status.Created == true && *status.Deleted == false && *status.UpgradeProcessing == true {
		return "upgrading"
	} else if *status.Created == true && *status.Deleted == false && *status.Processing == true {
		return "processing"
	} else if *status.Deleted == true {
		return "deleted"
	} else if *status.Created == false && *status.Deleted == false {
		return "creating"
	} else {
		return "available"
	}
}

func NewAWSInstanceESProvider(namePrefix string) (*AWSInstanceESProvider, error) {
	if os.Getenv("AWS_REGION") == "" {
		return nil, errors.New("Unable to find AWS_REGION environment variable.")
	}
	if os.Getenv("AWS_ACCOUNT_ID") == "" {
		return nil, errors.New("Unable to find AWS_ACCOUNT_ID environment variable.")
	}
	t := time.NewTicker(time.Second * 5)
	AWSInstanceESProvider := &AWSInstanceESProvider{
		namePrefix:          namePrefix,
		instanceCache:		 make(map[string]*Instance),
		svc:              	 elasticsearchservice.New(session.New(&aws.Config{ Region: aws.String(os.Getenv("AWS_REGION")) })),
	}
	go (func() {
		for {
			AWSInstanceESProvider.instanceCache = make(map[string]*Instance)
			<-t.C
		}
	})()
	return AWSInstanceESProvider, nil
}


func (provider AWSInstanceESProvider) CreateRandomName() string {
	id, _ := uuid.NewV4()
	return provider.namePrefix + "-u" + (strings.Split(id.String(), "-")[0])
}

func (provider AWSInstanceESProvider) GetInstance(name string, plan *ProviderPlan) (*Instance, error) {
	if provider.instanceCache[name + plan.ID] != nil {
		return provider.instanceCache[name + plan.ID], nil
	}

	res, err := provider.svc.DescribeElasticsearchDomain(&elasticsearchservice.DescribeElasticsearchDomainInput{
		DomainName:aws.String(name),
	})

	if err != nil {
		return nil, err
	}

	endpoint := ""
	if res.DomainStatus != nil && res.DomainStatus.Endpoints != nil {
		endpoint = *res.DomainStatus.Endpoints["vpc"]
	}

	return &Instance{
		Id:            "", 						// provider should not store this.
		Name:          name,
		ProviderId:    *res.DomainStatus.ARN,
		Plan:          plan,
		Username:      "",						// provider should not store this.
		Password:      "",						// provider should not store this.
		Endpoint:      endpoint,
		Status:        GetStatus(res.DomainStatus),
		Ready:         IsReady(res.DomainStatus),
		Engine:        "elasticsearch",
		EngineVersion: *res.DomainStatus.ElasticsearchVersion,
		Scheme:        "https",
	}, nil
}

func (provider AWSInstanceESProvider) PerformPostProvision(db *Instance) (*Instance, error) {
	return db, nil
}

func (provider AWSInstanceESProvider) GetUrl(instance *Instance) map[string]interface{} {
	return map[string]interface{}{
		"KIBANA_URL": instance.Scheme + "://" + instance.Endpoint + "/_plugin/kibana",
		"ES_URL": instance.Scheme + "://" + instance.Endpoint,
	}
}

func (provider AWSInstanceESProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*Instance, error) {
	var settings elasticsearchservice.CreateElasticsearchDomainInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	
	settings.DomainName = aws.String(provider.CreateRandomName())
	settings.AccessPolicies = aws.String("{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"es:*\",\"Resource\":\"arn:aws:es:" + os.Getenv("AWS_REGION") + ":" + os.Getenv("AWS_ACCOUNT_ID") + ":domain/" + *settings.DomainName + "/*\"}]}")

	if os.Getenv("AWS_SECURITY_GROUP_ID") != "" && os.Getenv("AWS_SUBNET_ID") != "" {
		settings.VPCOptions.SubnetIds = make([]*string, 0)
		subnetIds := strings.Split(os.Getenv("AWS_SUBNET_ID"), ",")
		if settings.ElasticsearchClusterConfig != nil {
			if settings.ElasticsearchClusterConfig.InstanceCount != nil && settings.ElasticsearchClusterConfig.DedicatedMasterCount == nil && *settings.ElasticsearchClusterConfig.InstanceCount == 1 {
				// if we only have one instance without multicluster AZ availability only use one subnet
				// otherwise use all of them.
				settings.VPCOptions.SubnetIds = append(settings.VPCOptions.SubnetIds, aws.String(fmt.Sprintf("%v", subnetIds[0])))
			} else {
		        for _, element := range strings.Split(os.Getenv("AWS_SUBNET_ID"), ",") {
    		       settings.VPCOptions.SubnetIds = append(settings.VPCOptions.SubnetIds, aws.String(fmt.Sprintf("%v",element)))
        		}
        	}
			settings.VPCOptions.SecurityGroupIds = []*string{aws.String(os.Getenv("AWS_SECURITY_GROUP_ID"))}
		}
	} else {
		settings.VPCOptions = nil
	} 

	res, err := provider.svc.CreateElasticsearchDomain(&settings)
	if err != nil {
		return nil, err
	}

	endpoint := ""
	if res.DomainStatus != nil && res.DomainStatus.Endpoints != nil {
		endpoint = *res.DomainStatus.Endpoints["vpc"]
	}

	instance := &Instance{
		Id:            Id,
		Name:          *settings.DomainName,
		ProviderId:    *res.DomainStatus.ARN,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      endpoint,
		Status:        GetStatus(res.DomainStatus),
		Ready:         IsReady(res.DomainStatus),
		Engine:        "elasticsearch",
		EngineVersion: *res.DomainStatus.ElasticsearchVersion,
		Scheme:        "https",
	}

	time.Sleep( time.Second * time.Duration(10))
	if err := provider.Tag(instance, "billingcode", Owner); err != nil {
		return nil, err
	}
	return instance, nil
}

func (provider AWSInstanceESProvider) Deprovision(Instance *Instance, takeSnapshot bool) error {
	params := &elasticsearchservice.DeleteElasticsearchDomainInput{
		DomainName: aws.String(Instance.Name), // Required
	}
	_, err := provider.svc.DeleteElasticsearchDomain(params)
	return err
}

func (provider AWSInstanceESProvider) Modify(instance *Instance, plan *ProviderPlan) (*Instance, error) {
	var settings elasticsearchservice.CreateElasticsearchDomainInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	if os.Getenv("AWS_SECURITY_GROUP_ID") != "" && os.Getenv("AWS_SUBNET_ID") != "" {
		settings.VPCOptions.SubnetIds = make([]*string, 0)
		subnetIds := strings.Split(os.Getenv("AWS_SUBNET_ID"), ",")
		if settings.ElasticsearchClusterConfig != nil {
			if settings.ElasticsearchClusterConfig.InstanceCount != nil && settings.ElasticsearchClusterConfig.DedicatedMasterCount == nil && *settings.ElasticsearchClusterConfig.InstanceCount == 1 {
				// if we only have one instance without multicluster AZ availability only use one subnet
				// otherwise use all of them.
				settings.VPCOptions.SubnetIds = append(settings.VPCOptions.SubnetIds, aws.String(fmt.Sprintf("%v", subnetIds[0])))
			} else {
		        for _, element := range strings.Split(os.Getenv("AWS_SUBNET_ID"), ",") {
    		       settings.VPCOptions.SubnetIds = append(settings.VPCOptions.SubnetIds, aws.String(fmt.Sprintf("%v",element)))
        		}
        	}
			settings.VPCOptions.SecurityGroupIds = []*string{aws.String(os.Getenv("AWS_SECURITY_GROUP_ID"))}
		}
	} else {
		settings.VPCOptions = nil
	}
	
	settings.DomainName = aws.String(instance.Name)
	
	_, err := provider.svc.UpdateElasticsearchDomainConfig(&elasticsearchservice.UpdateElasticsearchDomainConfigInput{
		AccessPolicies: settings.AccessPolicies,
		AdvancedOptions: settings.AdvancedOptions,
		CognitoOptions: settings.CognitoOptions,
		DomainName: aws.String(instance.Name),
		EBSOptions: settings.EBSOptions,
		ElasticsearchClusterConfig: settings.ElasticsearchClusterConfig,
		LogPublishingOptions: settings.LogPublishingOptions,
		SnapshotOptions: settings.SnapshotOptions,
		VPCOptions: settings.VPCOptions,
	})
	if err != nil {
		return nil, err
	}
	
	res, err := provider.svc.DescribeElasticsearchDomain(&elasticsearchservice.DescribeElasticsearchDomainInput{
		DomainName:aws.String(instance.Name),
	})

	if err != nil {
		return nil, err
	}

	endpoint := ""
	if res.DomainStatus != nil && res.DomainStatus.Endpoints != nil {
		endpoint = *res.DomainStatus.Endpoints["vpc"]
	}

	return &Instance{
		Id:            instance.Id,
		Name:          *settings.DomainName,
		ProviderId:    *res.DomainStatus.ARN,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      endpoint,
		Status:        GetStatus(res.DomainStatus),
		Ready:         IsReady(res.DomainStatus),
		Engine:        "elasticsearch",
		EngineVersion: *res.DomainStatus.ElasticsearchVersion,
		Scheme:        "https",
	}, nil
}

func (provider AWSInstanceESProvider) Tag(Instance *Instance, Name string, Value string) error {
	_, err := provider.svc.AddTags(&elasticsearchservice.AddTagsInput{
		ARN: aws.String(Instance.ProviderId),
		TagList:[]*elasticsearchservice.Tag{&elasticsearchservice.Tag{Key: aws.String(Name), Value:aws.String(Value)}},
	})
	return err
}

func (provider AWSInstanceESProvider) Untag(Instance *Instance, Name string) error {
	_, err := provider.svc.RemoveTags(&elasticsearchservice.RemoveTagsInput{
		ARN: aws.String(Instance.ProviderId),
		TagKeys:[]*string{ aws.String(Name) },
	})
	return err
}

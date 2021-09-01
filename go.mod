module github.com/Azure/azure-service-bus-go

go 1.12

require (
	github.com/Azure/azure-amqp-common-go/v3 v3.1.1
	github.com/Azure/azure-sdk-for-go v51.1.0+incompatible
	github.com/Azure/go-amqp v0.13.13
	github.com/Azure/go-autorest/autorest v0.11.18
	github.com/Azure/go-autorest/autorest/adal v0.9.13
	github.com/Azure/go-autorest/autorest/date v0.3.0
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/devigned/tab v0.1.1
	github.com/joho/godotenv v1.3.0
	github.com/mitchellh/mapstructure v1.3.3
	github.com/stretchr/testify v1.6.1
	golang.org/x/sys v0.0.0-20200323222414-85ca7c5b95cd // indirect
	nhooyr.io/websocket v1.8.6
)

replace (
	github.com/Azure/azure-amqp-common-go/v3 v3.1.1 => github.com/richardpark-msft/azure-amqp-common-go/v3 v3.1.1-0.20210901225906-2b1c8554a415
	github.com/Azure/go-amqp v0.13.13 => github.com/serbrech/go-amqp v0.13.2-0.20210831215711-a68457cdbcc0
)

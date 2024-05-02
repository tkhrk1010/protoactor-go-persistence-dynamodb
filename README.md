# protoactor-go-persistence-dynamodb
protoactor-go persistence(journal, snapshot, state) plugin for AWS DynamoDB

In the [official protoactor-go repository](https://github.com/asynkron/protoactor-go), there is currently no DynamoDB persistence plugin available. Therefore, this provides a way to persist data to DynamoDB in your local environment. 

This is primarily intended for local hands-on experimentation and is not suitable for enterprise production use cases.

## Usage
docker needed.  
in concrete, see main.go

## References
I have referred to these repositories and borrowed some of their techniques.
- [protoactor-go-persistence-pg](https://github.com/ytake/protoactor-go-persistence-pg) by ytake
- [protoactor-go-cqrs-example](https://github.com/ytake/protoactor-go-cqrs-example) by ytake
- [event-store-adapter-go](https://github.com/j5ik2o/event-store-adapter-go) by j5ik2o
- [cqrs-es-example-go](https://github.com/j5ik2o/cqrs-es-example-go) by j5ik2o



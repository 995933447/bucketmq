package discover

const (
	GrpcResolveSchema = "bucketmq_"
	SrvNamePrefix     = "bucketmq_"
)

const SrvNameBroker = "broker"

func GetDiscoverNamePrefix(cluster string) string {
	return SrvNamePrefix + cluster + "_"
}

func GetGrpcResolveSchema(cluster string) string {
	return GrpcResolveSchema + cluster
}

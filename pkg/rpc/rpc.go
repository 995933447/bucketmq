package rpc

const (
	GrpcResolveSchema  = "bucketmq"
	DiscoverNamePrefix = "bucketmq_"
)

func GetDiscoverNamePrefix(cluster string) string {
	return DiscoverNamePrefix + cluster + "_"
}

package remoting

type RemotingConfig struct {
	ClientOnewaySemaphoreValue int64
	ClientAsyncSemaphoreValue  int64
	ConnectTimeoutMillis       int64
}

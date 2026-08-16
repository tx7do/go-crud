package cassandra

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/tx7do/go-wind/log"
)

func NewCassandraClient(opts ...Option) *gocql.Session {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	clusterConfig := gocql.NewCluster(o.Hosts...)

	// 设置用户名密码
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{
		Username: o.Username,
		Password: o.Password,
	}

	clusterConfig.Keyspace = o.Keyspace

	// 设置ssl
	if o.TLSConfig != nil {
	}

	// 设置超时时间
	clusterConfig.ConnectTimeout = o.ConnectTimeout
	clusterConfig.Timeout = o.Timeout

	clusterConfig.Consistency = gocql.Consistency(o.Consistency)

	// 禁止主机查找
	clusterConfig.DisableInitialHostLookup = o.DisableInitialHostLookup

	session, err := clusterConfig.CreateSession()
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed opening connection to cassandra: %v", err))
		return nil
	}

	return session
}

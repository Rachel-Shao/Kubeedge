package redisclient

import (
	"github.com/go-redis/redis"
	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/cloudcore/v1alpha1"
)

type RedisClient struct {
	address string
	password string
	DeviceDBClient *redis.Client

}


func NewRedisClient(config *v1alpha1.MessageController) (*RedisClient, error) {
	client := &RedisClient{
		address: config.RedisConfig.Address+":"+config.RedisConfig.Port,
		password: config.RedisConfig.Password,
	}

	// 建立连接
	client.DeviceDBClient = redis.NewClient(&redis.Options{
		Addr: client.address,
		Password: client.password,
		DB: 0, // sxy:
		PoolSize: 20,
		MinIdleConns: 3,

		//钩子函数
		OnConnect: func(conn *redis.Conn) error { //仅当客户端执行命令时需要从连接池获取连接时，如果连接池需要新建连接时则会调用此钩子函数
			klog.Infof("conn=%v\n", conn)
			return nil
		},

	})



	return client, nil
}


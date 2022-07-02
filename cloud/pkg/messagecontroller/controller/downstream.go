package controller

import (
	"github.com/go-redis/redis"
	"github.com/kubeedge/kubeedge/cloud/pkg/edgecontroller/manager"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/messagelayer"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/redisclient"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/cloudcore/v1alpha1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sync"
)

// DownstreamController watch kubernetes api server and send change to edge
type DownstreamController struct {
	kubeClient kubernetes.Interface
	redisClient redisclient.RedisClient

	messageLayer messagelayer.MessageLayer
	lc *manager.LocationCache

	// Device, key is device.Name, value is *v1alpha2.Device{}
	Device sync.Map

}

func NewDownstreamController(config *v1alpha1.MessageController) (*DownstreamController, error) {
	dc := &DownstreamController{

	}

	rc, err := redisclient.NewRedisClient(config)
	if err != nil {
		klog.Errorf("Failed to create redis client: %v", err)
	}
	dc.redisClient = rc


	return dc, nil
}

func (dc *DownstreamController) GetRedisClient() redisclient.RedisClient {
	return dc.redisClient
}

// Start DownstreamController
func (dc *DownstreamController) Start() error {
	klog.Info("start downstream controller")

	// 1.接收cloudhub的节点心跳信息，决定订阅与取消订阅的逻辑
	// 2.处理redis的event信息-更新DeviceMap





	return nil
}
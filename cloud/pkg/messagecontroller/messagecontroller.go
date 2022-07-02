package messagecontroller

import (
	"github.com/kubeedge/kubeedge/cloud/pkg/common/informers"
	"k8s.io/klog/v2"

	"github.com/kubeedge/beehive/pkg/core"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/modules"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/controller"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/cloudcore/v1alpha1"
)

type MessageController struct {
	config   v1alpha1.MessageController
	upstream *controller.UpstreamController
	downstream *controller.DownstreamController
}

func newMessageController(config *v1alpha1.MessageController) *MessageController {
	mc := &MessageController{config: *config}
	if !mc.Enable() {
		return mc
	}

	var err error
	mc.downstream, err = controller.NewDownstreamController(config)
	if err != nil {
		klog.Exitf("new downstream controller failed with error: %s", err)
	}

	mc.upstream, err = controller.NewUpstreamController(config, informers.GetInformersManager().GetK8sInformerFactory(), mc.downstream)
	if err != nil {
		klog.Exitf("new upstream controller failed with error: %s", err)
	}


	return mc
}

func Register(mc *v1alpha1.MessageController) {
	// sxy:需要InitConfigure吗
	core.Register(newMessageController(mc))
}

// Name of controller
func (mc *MessageController) Name() string {
	return modules.MessageControllerModuleName
}

// Group of controller
func (mc *MessageController) Group() string {
	return modules.MessageControllerModuleGroup
}

// Enable indicates whether enable this module
func (mc *MessageController) Enable() bool {
	return mc.config.Enable
}

// Start controller
func (mc *MessageController) Start() {
	if err := mc.upstream.Start(); err != nil {
		klog.Exitf("start upstream failed with error: %s", err)
	}

	if err := mc.downstream.Start(); err != nil {
		klog.Exitf("start upstream failed with error: %s", err)
	}
	// sxy: 关闭kafkaClient
}

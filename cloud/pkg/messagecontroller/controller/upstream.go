package controller

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"strconv"

	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sinformer "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/client"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/modules"
	messageconst "github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/constants"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/handler"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/kafkaclient"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/messagelayer"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/cloudcore/v1alpha1"
	"github.com/kubeedge/kubeedge/pkg/metaserver/util"
)

// UpstreamController subscribe messages from edge and sync to k8s api server
type UpstreamController struct {
	messageLayer messagelayer.MessageLayer
	kubeClient   kubernetes.Interface
	handler      *handler.Handler
	kafkaClient  *kafkaclient.KafkaClient
	handleWorker int

	// message channel
	queryNodeChan                chan model.Message
	queryConfigMapChan           chan model.Message
	querySecretChan              chan model.Message
	queryServiceAccountTokenChan chan model.Message

	nodeChan       chan model.Message
	nodeStatusChan chan model.Message
	podChan        chan model.Message
	podStatusChan  chan model.Message
	deviceStatusChan chan model.Message

	// lister
	podLister       corelisters.PodLister
	configMapLister corelisters.ConfigMapLister
	secretLister    corelisters.SecretLister
	serviceLister   corelisters.ServiceLister
	endpointLister  corelisters.EndpointsLister
	nodeLister      corelisters.NodeLister
}

func NewUpstreamController(config *v1alpha1.MessageController, factory k8sinformer.SharedInformerFactory, dc *DownstreamController) (*UpstreamController, error) {
	klog.Info("new upstream controller in message controller")
	uc := &UpstreamController{
		kubeClient:   client.GetKubeClient(),
		messageLayer: messagelayer.NewContextMessageLayer(config.Context),
		handleWorker: config.HandleWorker,
	}
	uc.queryNodeChan = make(chan model.Message, config.Buffer.Read)
	uc.queryConfigMapChan = make(chan model.Message, config.Buffer.Read)
	uc.querySecretChan = make(chan model.Message, config.Buffer.Read)
	uc.queryServiceAccountTokenChan = make(chan model.Message, config.Buffer.Read)

	uc.nodeChan = make(chan model.Message, config.Buffer.Write)
	uc.nodeStatusChan = make(chan model.Message, config.Buffer.Write)
	uc.podChan = make(chan model.Message, config.Buffer.Write)
	uc.podStatusChan = make(chan model.Message, config.Buffer.Write)
	uc.deviceStatusChan = make(chan model.Message, config.Buffer.Write)


	uc.nodeLister = factory.Core().V1().Nodes().Lister()
	uc.endpointLister = factory.Core().V1().Endpoints().Lister()
	uc.serviceLister = factory.Core().V1().Services().Lister()
	uc.podLister = factory.Core().V1().Pods().Lister()
	uc.configMapLister = factory.Core().V1().ConfigMaps().Lister()
	uc.secretLister = factory.Core().V1().Secrets().Lister()

	uc.handler = handler.NewHandler(uc.messageLayer, dc)

	var err error
	uc.kafkaClient, err = kafkaclient.NewKafkaClient(config)
	if err != nil {
		klog.Errorf("Failed to create kafka client: %v", err)
	}

	return uc, nil
}

// Start UpstreamController
func (uc *UpstreamController) Start() error {
	klog.Info("starting upstream in messageController")
	go uc.dispatchMessage()

	// query operation
	go uc.queryNode()
	go uc.querySecret()
	go uc.queryConfigMap()
	go uc.queryServiceAccountToken()

	// insert/update/delete operation
	go uc.updateNode()
	go uc.updateNodeStatus()
	go uc.deletePod()
	go uc.updatePodStatus()
	go uc.updateDeviceStatus()

	// consume msg from kafka
	go uc.consumeMsg()

	return nil
}

func (uc *UpstreamController) dispatchMessage() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Info("stop dispatchMessage")
			return
		default:
		}
		msg, err := uc.messageLayer.Receive()
		if err != nil {
			klog.Warningf("receive message failed, %s", err)
			continue
		}

		klog.V(5).Infof("message: %s, operation type is: %s", msg.GetID(), msg.GetOperation())
		resourceType, err := messagelayer.GetResourceType(msg)
		if err != nil {
			klog.Warningf("parse message: %s resource type with error: %s", msg.GetID(), err)
			continue
		}
		operation := msg.GetOperation()
		switch operation {
		case model.QueryOperation:
			// query Node, ConfigMap, Secret, ServiceAccountToken: handle directly
			switch resourceType {
			case model.ResourceTypeNode:
				uc.queryNodeChan <- msg
			case model.ResourceTypeSecret:
				uc.querySecretChan <- msg
			case model.ResourceTypeConfigmap:
				uc.queryConfigMapChan <- msg
			case model.ResourceTypeServiceAccountToken:
				uc.queryServiceAccountTokenChan <- msg
			default:
				err := stderrors.New("wrong resource type")
				klog.Error(err)
				continue
			}
		case model.InsertOperation, model.UpdateOperation, model.DeleteOperation:
			switch resourceType {
			case model.ResourceTypeNode:
				uc.nodeChan <- msg // update
			case model.ResourceTypeNodeStatus:
				uc.nodeStatusChan <- msg //insert update
			case model.ResourceTypePod:
				uc.podChan <- msg //delete
			case model.ResourceTypePodStatus:
				uc.podStatusChan <- msg // update
			case messageconst.ResourceTypeTwinEdgeUpdated:
				uc.deviceStatusChan <- msg // update
			default:
				err := stderrors.New("wrong resource type")
				klog.Error(err)
				continue
			}
		case model.ResponseOperation:
			// sxy:?
		default:
			klog.Errorf("message: %s, resource operation: %s unsupported", msg.GetID(), operation)
		}
	}
}


func queryInner(uc *UpstreamController, msg model.Message, queryType string) {
	klog.V(4).Infof("message: %s, operation is: %s, and resource is: %s", msg.GetID(), msg.GetOperation(), msg.GetResource())
	namespace, err := messagelayer.GetNamespace(msg)
	if err != nil {
		klog.Warningf("message: %s process failure, get namespace failed with error: %s", msg.GetID(), err)
		return
	}
	name, err := messagelayer.GetResourceName(msg)
	if err != nil {
		klog.Warningf("message: %s process failure, get resource name failed with error: %s", msg.GetID(), err)
		return
	}

	switch msg.GetOperation() {
	case model.QueryOperation:
		object, err := kubeClientGet(uc, namespace, name, queryType, msg)
		if errors.IsNotFound(err) {
			klog.Warningf("message: %s process failure, resource not found, namespace: %s, name: %s", msg.GetID(), namespace, name)
			return
		}
		if err != nil {
			klog.Warningf("message: %s process failure with error: %s, namespace: %s, name: %s", msg.GetID(), err, namespace, name)
			return
		}

		nodeID, err := messagelayer.GetNodeID(msg)
		if err != nil {
			klog.Warningf("message: %s process failure, get node id failed with error: %s", msg.GetID(), err)
			return
		}
		resource, err := messagelayer.BuildResource(nodeID, namespace, queryType, name)
		if err != nil {
			klog.Warningf("message: %s process failure, build message resource failed with error: %s", msg.GetID(), err)
			return
		}

		resMsg := model.NewMessage(msg.GetID()).
			SetResourceVersion(object.GetResourceVersion()).
			FillBody(object).
			BuildRouter(modules.MessageControllerModuleName, messageconst.GroupResource, resource, model.ResponseOperation)
		err = uc.messageLayer.Response(*resMsg)
		if err != nil {
			klog.Warningf("message: %s process failure, response failed with error: %s", msg.GetID(), err)
			return
		}
		klog.V(4).Infof("message: %s process successfully", msg.GetID())
	default:
		klog.Warningf("message: %s process failure, operation: %s unsupported", msg.GetID(), msg.GetOperation())
	}
}


func kubeClientGet(uc *UpstreamController, namespace string, name string, queryType string, msg model.Message) (metav1.Object, error) {
	var obj metav1.Object
	var err error
	switch queryType {
	case model.ResourceTypeConfigmap:
		obj, err = uc.configMapLister.ConfigMaps(namespace).Get(name)
	case model.ResourceTypeSecret:
		obj, err = uc.secretLister.Secrets(namespace).Get(name)
	case model.ResourceTypeNode:
		obj, err = uc.nodeLister.Get(name)
	case model.ResourceTypeServiceAccountToken:
		obj, err = uc.getServiceAccountToken(namespace, name, msg)
	default:
		err := stderrors.New("wrong query type")
		klog.Error(err)
		return nil, err
	}
	if err == nil && obj != nil {
		util.SetMetaType(obj.(runtime.Object))
	}
	return obj, err
}


func (uc *UpstreamController) getServiceAccountToken(namespace string, name string, msg model.Message) (metav1.Object, error) {
	data, err := msg.GetContentData()
	if err != nil {
		klog.Errorf("get message body failed err %v", err)
		return nil, err
	}

	tr := authenticationv1.TokenRequest{}
	if err := json.Unmarshal(data, &tr); err != nil {
		klog.Errorf("unmarshal token request failed err %v", err)
		return nil, err
	}

	tokenRequest, err := uc.kubeClient.CoreV1().ServiceAccounts(namespace).CreateToken(context.TODO(), name, &tr, metav1.CreateOptions{})
	if err != nil {
		klog.Errorf("apiserver get service account token failed: err %v", err)
		return nil, err
	}

	return tokenRequest, nil
}


func (uc *UpstreamController) queryNode() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop queryNode")
			return
		case msg := <-uc.queryNodeChan:
			queryInner(uc, msg, model.ResourceTypeNode)
		}
	}
}

func (uc *UpstreamController) queryConfigMap() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop queryConfigMap")
			return
		case msg := <-uc.queryConfigMapChan:
			queryInner(uc, msg, model.ResourceTypeConfigmap)
		}
	}
}


func (uc *UpstreamController) querySecret() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop querySecret")
			return
		case msg := <-uc.querySecretChan:
			queryInner(uc, msg, model.ResourceTypeSecret)
		}
	}
}

func (uc *UpstreamController) queryServiceAccountToken() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop process service account token")
			return
		case msg := <-uc.queryServiceAccountTokenChan:
			queryInner(uc, msg, model.ResourceTypeServiceAccountToken)
		}
	}
}


func (uc *UpstreamController) updateNode() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop updateNode")
			return
		case msg := <-uc.nodeChan:
			switch msg.GetOperation() {
			case model.UpdateOperation:
				nodeName, err := messagelayer.GetResourceName(msg)
				if err != nil {
					klog.Warningf("message: %s process failure, get resource name failed with error: %s", msg.GetID(), err)
					continue
				}
				topicName := uc.hashToTopic(nodeName, model.ResourceTypeNode)
				if topicName == "" {
					klog.Warningf("failed to hash to topic")
					continue
				}
				klog.Infof("[sxy] nodeName: %s, topicName: %d", nodeName, topicName)

				// send to kafka
				if err := uc.kafkaClient.PublicToKafka(msg, topicName, nodeName); err != nil {
					klog.Errorf("failed to publish msg to Kafka cluster, msg: %v, error: %v", msg, err)
				}
				klog.Infof("[sxy] message: %s process successfully", msg.GetID())
				klog.V(4).Infof("message: %s process successfully", msg.GetID())
			default:
				klog.Warningf("message: %s process failure, node operation: %s unsupported", msg.GetID(), msg.GetOperation())
				continue
			}
		}
	}
}

func (uc *UpstreamController) updateNodeStatus() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop updateNodeStatus")
			return
		case msg := <-uc.nodeStatusChan:
			nodeName, err := messagelayer.GetResourceName(msg)
			if err != nil {
				klog.Warningf("message: %s process failure, get resource name failed with error: %s", msg.GetID(), err)
				continue
			}
			topicName := uc.hashToTopic(nodeName, model.ResourceTypeNodeStatus)
			if topicName == "" {
				klog.Warningf("failed to hash to topic")
				continue
			}

			klog.Infof("[sxy] nodeName: %s, topicName: %d", nodeName, topicName)
			// send to kafka
			if err := uc.kafkaClient.PublicToKafka(msg, topicName, nodeName); err != nil {
				klog.Errorf("failed to publish msg to Kafka cluster, msg: %v, error: %v", msg, err)
			}
			klog.Infof("[sxy] message: %s process successfully", msg.GetID())
			klog.V(4).Infof("message: %s process successfully", msg.GetID())
		}
	}
}

func (uc *UpstreamController) updatePodStatus() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop updatePodStatus")
			return
		case msg := <-uc.podStatusChan:
			nodeName, err := messagelayer.GetResourceName(msg)
			if err != nil {
				klog.Warningf("message: %s process failure, get resource name failed with error: %s", msg.GetID(), err)
				continue
			}

			topicName := uc.hashToTopic(nodeName, model.ResourceTypePodStatus)
			if topicName == "" {
				klog.Warningf("failed to hash to topic")
				continue
			}

			klog.Infof("[sxy] nodeName: %s, topicName: %d", nodeName, topicName)
			// send to kafka
			if err := uc.kafkaClient.PublicToKafka(msg, topicName, nodeName); err != nil {
				klog.Errorf("failed to publish msg to Kafka cluster, msg: %v, error: %v", msg, err)
			}
			klog.Infof("[sxy] message: %s process successfully", msg.GetID())
			klog.V(4).Infof("message: %s process successfully", msg.GetID())
		}
	}
}

func (uc *UpstreamController) deletePod() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop deletePod")
			return
		case msg := <-uc.podChan:
			nodeName, err := messagelayer.GetResourceName(msg)
			if err != nil {
				klog.Warningf("message: %s process failure, get resource name failed with error: %s", msg.GetID(), err)
				continue
			}

			topicName := uc.hashToTopic(nodeName, model.ResourceTypePod)
			if topicName == "" {
				klog.Warningf("failed to hash to topic")
				continue
			}

			klog.Infof("[sxy] nodeName: %s, topicName: %d", nodeName, topicName)
			// send to kafka
			if err := uc.kafkaClient.PublicToKafka(msg, topicName, nodeName); err != nil {
				klog.Errorf("failed to publish msg to Kafka cluster, msg: %v, error: %v", msg, err)
			}
			klog.Infof("[sxy] message: %s process successfully", msg.GetID())
			klog.V(4).Infof("message: %s process successfully", msg.GetID())
		}
	}
}

func (uc *UpstreamController) updateDeviceStatus() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("stop update device status")
			return
		case msg := <-uc.deviceStatusChan:
			nodeName, err := messagelayer.GetResourceName(msg)
			if err != nil {
				klog.Warningf("message: %s process failure, get resource name failed with error: %s", msg.GetID(), err)
				continue
			}

			topicName := uc.hashToTopic(nodeName, messageconst.ResourceTypeTwinEdgeUpdated)
			if topicName == "" {
				klog.Warningf("failed to hash to topic")
				continue
			}

			klog.Infof("[sxy] nodeName: %s, topicName: %d", nodeName, topicName)
			// send to kafka
			if err := uc.kafkaClient.PublicToKafka(msg, topicName, nodeName); err != nil {
				klog.Errorf("failed to publish msg to Kafka cluster, msg: %v, error: %v", msg, err)
			}
			klog.Infof("[sxy] message: %s process successfully", msg.GetID())
			klog.V(4).Infof("message: %s process successfully", msg.GetID())
		}
	}
}


func (uc *UpstreamController) consumeMsg() {
	topicList := getTopicList()
	for _, topic := range topicList {
		// 创建一个消费者组
		go uc.kafkaClient.ConsumeFromKafka(topic, uc.handler)
	}
	klog.Infof("[sxy] func consumeMsg() 结束")
	return
}

func (uc *UpstreamController) hashToTopic(name string, resType string) string {
	var topicStr string
	switch resType {
	case model.ResourceTypeNode:
		topicStr = "TopicNode"
	case model.ResourceTypeNodeStatus:
		topicStr = "TopicNodeStatus"
	case model.ResourceTypePod:
		topicStr = "TopicPod"
	case model.ResourceTypePodStatus:
		topicStr = "TopicPodStatus"
	case messageconst.ResourceTypeTwinEdgeUpdated:
		topicStr = "TopicDeviceStatus"
	default:
		klog.Errorf("unsupported resource type")
		return ""
	}
	return topicStr + strconv.Itoa(uc.hashFunc(name))
}

func (uc *UpstreamController) hashFunc(str string) int {
	seed := 131 // 31 131 1313 13131 131313 etc..
	hash := 0
	for i := 0; i < len(str); i++ {
		hash = (hash * seed) + int(str[i])
	}
	return (hash & 0x7FFFFFFF) % uc.kafkaClient.GetTopicNum() // 10+10
}

func getTopicList() []string {
	// 从第三方获取
	// TODO:sxy 修改
	ans := []string{
		"TopicNodeStatus0",
		"TopicNodeStatus1",
		"TopicNodeStatus2",
		"TopicNodeStatus3",
		"TopicNodeStatus4",
		"TopicNodeStatus5",
		"TopicNodeStatus6",
		"TopicNodeStatus7",
		"TopicNodeStatus8",
		"TopicNodeStatus9",
	}
	return ans
}

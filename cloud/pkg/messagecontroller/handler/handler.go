package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/cloud/pkg/apis/devices/v1alpha2"
	crdClientset "github.com/kubeedge/kubeedge/cloud/pkg/client/clientset/versioned"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/client"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/modules"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/constants"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/controller"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/messagelayer"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/types"
	commonconst "github.com/kubeedge/kubeedge/common/constants"
	edgeapi "github.com/kubeedge/kubeedge/common/types"
	v1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/cri-api/pkg/errors"
	"k8s.io/klog/v2"
	"sort"
	"strconv"
	"time"
)

const (
	// MergePatchType is patch type
	MergePatchType = "application/merge-patch+json"
	// ResourceTypeDevices is plural of device resource in apiserver
	ResourceTypeDevices = "devices"
)

// DeviceStatus is structure to patch device status
type DeviceStatus struct {
	Status v1alpha2.DeviceStatus `json:"status"`
}

// SortedContainerStatuses define A type to help sort container statuses based on container names.
type SortedContainerStatuses []v1.ContainerStatus

func (s SortedContainerStatuses) Len() int      { return len(s) }
func (s SortedContainerStatuses) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s SortedContainerStatuses) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

// SortInitContainerStatuses ensures that statuses are in the order that their
// init container appears in the pod spec
func SortInitContainerStatuses(p *v1.Pod, statuses []v1.ContainerStatus) {
	containers := p.Spec.InitContainers
	current := 0
	for _, container := range containers {
		for j := current; j < len(statuses); j++ {
			if container.Name == statuses[j].Name {
				statuses[current], statuses[j] = statuses[j], statuses[current]
				current++
				break
			}
		}
	}
}

type Handler struct {
	kubeClient   kubernetes.Interface
	messageLayer messagelayer.MessageLayer
	crdClient    crdClientset.Interface

	dc *controller.DownstreamController
}

func NewHandler(messageLayer messagelayer.MessageLayer, dc *controller.DownstreamController) *Handler {
	return &Handler{
		kubeClient: client.GetKubeClient(),
		messageLayer: messageLayer,
		crdClient: client.GetCRDClient(),
		dc: dc,
	}
}

func (hd *Handler) UpdateNodeStatus(msg model.Message) {
	klog.V(5).Infof("message: %s, operation is: %s, and resource is %s", msg.GetID(), msg.GetOperation(), msg.GetResource())

	data, err := msg.GetContentData()
	if err != nil {
		klog.Warningf("message: %s process failure, get content data failed with error: %s", msg.GetID(), err)
		return
	}

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
	case model.InsertOperation:
		_, err := hd.kubeClient.CoreV1().Nodes().Get(context.Background(), name, metaV1.GetOptions{})
		if err == nil {
			klog.Infof("node: %s already exists, do nothing", name)
			//hd.nodeMsgResponse(name, namespace, common.MessageSuccessfulContent, msg)
			return
		}

		if !errors.IsNotFound(err) {
			errLog := fmt.Sprintf("get node %s info error: %v , register node failed", name, err)
			klog.Error(errLog)
			//hd.nodeMsgResponse(name, namespace, errLog, msg)
			return
		}

		node := &v1.Node{}
		err = json.Unmarshal(data, node)
		if err != nil {
			errLog := fmt.Sprintf("message: %s process failure, unmarshal marshaled message content with error: %s", msg.GetID(), err)
			klog.Error(errLog)
			//hd.nodeMsgResponse(name, namespace, errLog, msg)
			return
		}

		if _, err = hd.createNode(name, node); err != nil {
			errLog := fmt.Sprintf("create node %s error: %v , register node failed", name, err)
			klog.Error(errLog)
			//uc.nodeMsgResponse(name, namespace, errLog, msg)
			return
		}

		//hd.nodeMsgResponse(name, namespace, common.MessageSuccessfulContent, msg)
		klog.Infof("[SXY] insert node successful")

	case model.UpdateOperation:
		nodeStatusRequest := &edgeapi.NodeStatusRequest{}
		err := json.Unmarshal(data, nodeStatusRequest)
		if err != nil {
			klog.Warningf("message: %s process failure, unmarshal marshaled message content with error: %s", msg.GetID(), err)
			return
		}

		getNode, err := hd.kubeClient.CoreV1().Nodes().Get(context.Background(), name, metaV1.GetOptions{})
		if errors.IsNotFound(err) {
			klog.Warningf("message: %s process failure, node %s not found", msg.GetID(), name)
			return
		}

		if err != nil {
			klog.Warningf("message: %s process failure with error: %s, namespaces: %s name: %s", msg.GetID(), err, namespace, name)
			return
		}

		// TODO: comment below for test failure. Needs to decide whether to keep post troubleshoot
		// In case the status stored at metadata service is outdated, update the heartbeat automatically
		for i := range nodeStatusRequest.Status.Conditions {
			if time.Since(nodeStatusRequest.Status.Conditions[i].LastHeartbeatTime.Time) > time.Duration(10)*time.Second {
			//if time.Since(nodeStatusRequest.Status.Conditions[i].LastHeartbeatTime.Time) > time.Duration(uc.config.NodeUpdateFrequency)*time.Second {
				nodeStatusRequest.Status.Conditions[i].LastHeartbeatTime = metaV1.NewTime(time.Now())
			}
		}

		if getNode.Annotations == nil {
			getNode.Annotations = make(map[string]string)
		}
		for name, v := range nodeStatusRequest.ExtendResources {
			if name == constants.NvidiaGPUScalarResourceName {
				var gpuStatus []types.NvidiaGPUStatus
				for _, er := range v {
					gpuStatus = append(gpuStatus, types.NvidiaGPUStatus{ID: er.Name, Healthy: true})
				}
				if len(gpuStatus) > 0 {
					data, _ := json.Marshal(gpuStatus)
					getNode.Annotations[constants.NvidiaGPUStatusAnnotationKey] = string(data)
				}
			}
			data, err := json.Marshal(v)
			if err != nil {
				klog.Warningf("message: %s process failure, extend resource list marshal with error: %s", msg.GetID(), err)
				continue
			}
			getNode.Annotations[string(name)] = string(data)
		}

		// Keep the same "VolumesAttached" attribute with upstream,
		// since this value is maintained by kube-controller-manager.
		nodeStatusRequest.Status.VolumesAttached = getNode.Status.VolumesAttached
		if getNode.Status.DaemonEndpoints.KubeletEndpoint.Port != 0 {
			nodeStatusRequest.Status.DaemonEndpoints.KubeletEndpoint.Port = getNode.Status.DaemonEndpoints.KubeletEndpoint.Port
		}

		getNode.Status = nodeStatusRequest.Status

		_, err = hd.kubeClient.CoreV1().Nodes().UpdateStatus(context.Background(), getNode, metaV1.UpdateOptions{})
		if err != nil {
			klog.Warningf("message: %s process failure, update node failed with error: %s, namespace: %s, name: %s", msg.GetID(), err, getNode.Namespace, getNode.Name)
			return
		}

		//nodeID, err := messagelayer.GetNodeID(msg)
		//if err != nil {
		//	klog.Warningf("Message: %s process failure, get node id failed with error: %s", msg.GetID(), err)
		//	return
		//}
		//
		//resource, err := messagelayer.BuildResource(nodeID, namespace, model.ResourceTypeNode, name)
		//if err != nil {
		//	klog.Warningf("Message: %s process failure, build message resource failed with error: %s", msg.GetID(), err)
		//	return
		//}
		//
		//resMsg := model.NewMessage(msg.GetID()).
		//	SetResourceVersion(node.ResourceVersion).
		//	FillBody(common.MessageSuccessfulContent).
		//	BuildRouter(modules.EdgeControllerModuleName, constants.GroupResource, resource, model.ResponseOperation)
		//if err = hd.messageLayer.Response(*resMsg); err != nil {
		//	klog.Warningf("Message: %s process failure, response failed with error: %s", msg.GetID(), err)
		//	return
		//}

		klog.V(4).Infof("message: %s, update node status successfully, namespace: %s, name: %s", msg.GetID(), getNode.Namespace, getNode.Name)
		klog.Infof("[SXY] update node status successful")
	default:
		klog.Warningf("message: %s process failure, node status operation: %s unsupported", msg.GetID(), msg.GetOperation())
		return
	}
	klog.V(4).Infof("message: %s process successfully", msg.GetID())
}


// createNode create new edge node to kubernetes
func (hd *Handler) createNode(name string, node *v1.Node) (*v1.Node, error) {
	node.Name = name
	return hd.kubeClient.CoreV1().Nodes().Create(context.Background(), node, metaV1.CreateOptions{})
}

func (hd *Handler) UpdatePodStatus(msg model.Message) {
	klog.V(5).Infof("message: %s, operation is: %s, and resource is: %s", msg.GetID(), msg.GetOperation(), msg.GetResource())

	namespace, podStatuses := hd.unmarshalPodStatusMessage(msg)
	switch msg.GetOperation() {
	case model.UpdateOperation:
		for _, podStatus := range podStatuses {
			getPod, err := hd.kubeClient.CoreV1().Pods(namespace).Get(context.Background(), podStatus.Name, metaV1.GetOptions{})
			if errors.IsNotFound(err) {
				klog.Warningf("message: %s, pod not found, namespace: %s, name: %s", msg.GetID(), namespace, podStatus.Name)

				// Send request to delete this pod on edge side
				delMsg := model.NewMessage("")
				nodeID, err := messagelayer.GetNodeID(msg)
				if err != nil {
					klog.Warningf("Get node ID failed with error: %s", err)
					continue
				}
				resource, err := messagelayer.BuildResource(nodeID, namespace, model.ResourceTypePod, podStatus.Name)
				if err != nil {
					klog.Warningf("Built message resource failed with error: %s", err)
					continue
				}
				pod := &v1.Pod{}
				pod.Namespace, pod.Name = namespace, podStatus.Name
				delMsg.Content = pod
				delMsg.BuildRouter(modules.MessageControllerModuleName, constants.GroupResource, resource, model.DeleteOperation)
				if err := hd.messageLayer.Send(*delMsg); err != nil {
					klog.Warningf("Send message failed with error: %s, operation: %s, resource: %s", err, delMsg.GetOperation(), delMsg.GetResource())
				} else {
					klog.V(4).Infof("Send message successfully, operation: %s, resource: %s", delMsg.GetOperation(), delMsg.GetResource())
				}

				continue
			}
			if err != nil {
				klog.Warningf("message: %s, pod is nil, namespace: %s, name: %s, error: %s", msg.GetID(), namespace, podStatus.Name, err)
				continue
			}
			status := podStatus.Status
			oldStatus := getPod.Status
			// Set ReadyCondition.LastTransitionTime
			if readyCondition := hd.getPodCondition(&status, v1.PodReady); readyCondition != nil {
				// Need to set LastTransitionTime.
				lastTransitionTime := metaV1.Now()
				oldReadyCondition := hd.getPodCondition(&oldStatus, v1.PodReady)
				if oldReadyCondition != nil && readyCondition.Status == oldReadyCondition.Status {
					lastTransitionTime = oldReadyCondition.LastTransitionTime
				}
				readyCondition.LastTransitionTime = lastTransitionTime
			}

			// Set InitializedCondition.LastTransitionTime.
			if initCondition := hd.getPodCondition(&status, v1.PodInitialized); initCondition != nil {
				// Need to set LastTransitionTime.
				lastTransitionTime := metaV1.Now()
				oldInitCondition := hd.getPodCondition(&oldStatus, v1.PodInitialized)
				if oldInitCondition != nil && initCondition.Status == oldInitCondition.Status {
					lastTransitionTime = oldInitCondition.LastTransitionTime
				}
				initCondition.LastTransitionTime = lastTransitionTime
			}

			// ensure that the start time does not change across updates.
			if oldStatus.StartTime != nil && !oldStatus.StartTime.IsZero() {
				status.StartTime = oldStatus.StartTime
			} else if status.StartTime.IsZero() {
				// if the status has no start time, we need to set an initial time
				now := metaV1.Now()
				status.StartTime = &now
			}

			hd.normalizePodStatus(getPod, &status)
			getPod.Status = status

			if updatedPod, err := hd.kubeClient.CoreV1().Pods(getPod.Namespace).UpdateStatus(context.Background(), getPod, metaV1.UpdateOptions{}); err != nil {
				klog.Warningf("message: %s, update pod status failed with error: %s, namespace: %s, name: %s", msg.GetID(), err, getPod.Namespace, getPod.Name)
			} else {
				klog.V(5).Infof("message: %s, update pod status successfully, namespace: %s, name: %s", msg.GetID(), updatedPod.Namespace, updatedPod.Name)
				if updatedPod.DeletionTimestamp != nil && (status.Phase == v1.PodSucceeded || status.Phase == v1.PodFailed) {
					if hd.isPodNotRunning(status.ContainerStatuses) {
						if err := hd.kubeClient.CoreV1().Pods(updatedPod.Namespace).Delete(context.Background(), updatedPod.Name, *metaV1.NewDeleteOptions(0)); err != nil {
							klog.Warningf("message: %s, graceful delete pod failed with error: %s, namespace: %s, name: %s", msg.GetID(), err, updatedPod.Namespace, updatedPod.Name)
						} else {
							klog.Infof("message: %s, pod delete successfully, namespace: %s, name: %s", msg.GetID(), updatedPod.Namespace, updatedPod.Name)
						}
					}
				}
			}
		}

	default:
		klog.Warningf("message: %s process failure, pod status operation: %s unsupported", msg.GetID(), msg.GetOperation())
		return
	}
	klog.Infof("[SXY] update pod status successful")
	klog.V(4).Infof("message: %s process successfully", msg.GetID())
}

func (hd *Handler) unmarshalPodStatusMessage(msg model.Message) (ns string, podStatuses []edgeapi.PodStatusRequest) {
	ns, err := messagelayer.GetNamespace(msg)
	if err != nil {
		klog.Warningf("message: %s process failure, get namespace with error: %s", msg.GetID(), err)
		return
	}

	data, err := msg.GetContentData()
	if err != nil {
		klog.Warningf("message: %s process failure, get content data failed with error: %s", msg.GetID(), err)
		return
	}

	if name, _ := messagelayer.GetResourceName(msg); name == "" {
		// multi pod status in one message
		_ = json.Unmarshal(data, &podStatuses)
		return
	}

	// one pod status per message
	var status edgeapi.PodStatusRequest
	if err := json.Unmarshal(data, &status); err != nil {
		return
	}
	podStatuses = append(podStatuses, status)
	return
}

// GetPodCondition extracts the provided condition from the given status and returns that.
// Returns nil if the condition is not present, or return the located condition.
func (hd *Handler) getPodCondition(status *v1.PodStatus, conditionType v1.PodConditionType) *v1.PodCondition {
	if status == nil {
		return nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return &status.Conditions[i]
		}
	}
	return nil
}

func (hd *Handler) isPodNotRunning(statuses []v1.ContainerStatus) bool {
	for _, status := range statuses {
		if status.State.Terminated == nil && status.State.Waiting == nil {
			return false
		}
	}
	return true
}

// We add this function, because apiserver only supports *RFC3339* now, which means that the timestamp returned by
// apiserver has no nanosecond information. However, the timestamp returned by unversioned.Now() contains nanosecond,
// so when we do comparison between status from apiserver and cached status, isStatusEqual() will always return false.
// There is related issue #15262 and PR #15263 about this.
func (hd *Handler) normalizePodStatus(pod *v1.Pod, status *v1.PodStatus) *v1.PodStatus {
	normalizeTimeStamp := func(t *metaV1.Time) {
		*t = t.Rfc3339Copy()
	}
	normalizeContainerState := func(c *v1.ContainerState) {
		if c.Running != nil {
			normalizeTimeStamp(&c.Running.StartedAt)
		}
		if c.Terminated != nil {
			normalizeTimeStamp(&c.Terminated.StartedAt)
			normalizeTimeStamp(&c.Terminated.FinishedAt)
		}
	}

	if status.StartTime != nil {
		normalizeTimeStamp(status.StartTime)
	}
	for i := range status.Conditions {
		condition := &status.Conditions[i]
		normalizeTimeStamp(&condition.LastProbeTime)
		normalizeTimeStamp(&condition.LastTransitionTime)
	}

	// update container statuses
	for i := range status.ContainerStatuses {
		cstatus := &status.ContainerStatuses[i]
		normalizeContainerState(&cstatus.State)
		normalizeContainerState(&cstatus.LastTerminationState)
	}
	// Sort the container statuses, so that the order won't affect the result of comparison
	sort.Sort(SortedContainerStatuses(status.ContainerStatuses))

	// update init container statuses
	for i := range status.InitContainerStatuses {
		cstatus := &status.InitContainerStatuses[i]
		normalizeContainerState(&cstatus.State)
		normalizeContainerState(&cstatus.LastTerminationState)
	}
	// Sort the container statuses, so that the order won't affect the result of comparison
	SortInitContainerStatuses(pod, status.InitContainerStatuses)
	return status
}

func (hd *Handler) UpdateDeviceStatus(msg model.Message) {
	klog.Infof("Message: %s, operation is: %s, and resource is: %s", msg.GetID(), msg.GetOperation(), msg.GetResource())
	msgTwin, err := hd.unmarshalDeviceStatusMessage(msg)
	if err != nil {
		klog.Warningf("Unmarshall failed due to error %v", err)
		return
	}
	deviceID, err := messagelayer.GetDeviceID(msg.GetResource())
	if err != nil {
		klog.Warning("Failed to get device id")
		return
	}
	nodeID, err := messagelayer.GetDeviceNodeID(msg.GetResource())
	if err != nil {
		klog.Warning("Failed to get node id")
		return
	}

	var device interface{}
	var ok bool
	device, ok = hd.dc.Device.Load(deviceID)
	if !ok {
		//klog.Warningf("Device %s does not exist in downstream controller", deviceID)
		//return
		device, err = hd.getDeviceFromRedis(nodeID, deviceID)
		if err != nil {
			klog.Warningf("Can't get Device %s in redis", deviceID)
			return
		}
	}
	cacheDevice, ok := device.(*v1alpha2.Device)
	if !ok {
		klog.Warning("Failed to assert to CacheDevice type")
		return
	}
	deviceStatus := &DeviceStatus{Status: cacheDevice.Status}
	for twinName, twin := range msgTwin.Twin {
		for i, cacheTwin := range deviceStatus.Status.Twins {
			if twinName == cacheTwin.PropertyName && twin.Actual != nil && twin.Actual.Value != nil {
				reported := v1alpha2.TwinProperty{}
				reported.Value = *twin.Actual.Value
				reported.Metadata = make(map[string]string)
				if twin.Actual.Metadata != nil {
					reported.Metadata["timestamp"] = strconv.FormatInt(twin.Actual.Metadata.Timestamp, 10)
				}
				if twin.Metadata != nil {
					reported.Metadata["type"] = twin.Metadata.Type
				}
				deviceStatus.Status.Twins[i].Reported = reported
				break
			}
		}
	}

	// Store the status in cache so that when update is received by informer, it is not processed by downstream controller
	cacheDevice.Status = deviceStatus.Status
	hd.dc.Device.Store(deviceID, cacheDevice)

	body, err := json.Marshal(deviceStatus)
	if err != nil {
		klog.Errorf("Failed to marshal device status %v", deviceStatus)
		return
	}
	err = hd.crdClient.DevicesV1alpha2().RESTClient().Patch(MergePatchType).Namespace(cacheDevice.Namespace).Resource(ResourceTypeDevices).Name(deviceID).Body(body).Do(context.Background()).Error()
	if err != nil {
		klog.Errorf("Failed to patch device status %v of device %v in namespace %v, err: %v", deviceStatus, deviceID, cacheDevice.Namespace, err)
		return
	}
	//send confirm message to edge twin
	resMsg := model.NewMessage(msg.GetID())
	nodeID, err = messagelayer.GetDeviceNodeID(msg)
	if err != nil {
		klog.Warningf("Message: %s process failure, get node id failed with error: %s", msg.GetID(), err)
		return
	}
	resource, err := messagelayer.BuildDeviceResource(nodeID, "twin", "")
	if err != nil {
		klog.Warningf("Message: %s process failure, build message resource failed with error: %s", msg.GetID(), err)
		return
	}
	resMsg.BuildRouter(modules.DeviceControllerModuleName, constants.GroupTwin, resource, model.ResponseOperation)
	resMsg.Content = commonconst.MessageSuccessfulContent
	err = hd.messageLayer.Response(*resMsg)
	if err != nil {
		klog.Warningf("Message: %s process failure, response failed with error: %s", msg.GetID(), err)
		return
	}
	klog.Infof("Message: %s process successfully", msg.GetID())
}


func (hd *Handler) unmarshalDeviceStatusMessage(msg model.Message) (*types.DeviceTwinUpdate, error) {
	contentData, err := msg.GetContentData()
	if err != nil {
		return nil, err
	}

	twinUpdate := &types.DeviceTwinUpdate{}
	if err := json.Unmarshal(contentData, twinUpdate); err != nil {
		return nil, err
	}
	return twinUpdate, nil
}

func (hd *Handler) getDeviceFromRedis(nodeID string, deviceID string) (interface{}, error) {
	//读取结构
	deviceClient := hd.dc.GetRedisClient().DeviceDBClient
	doctorResult, _ := deviceClient.Get(nodeID+"/"+deviceID).Result()
	var device *v1alpha2.Device
	//反序列化
	err := json.Unmarshal([]byte(doctorResult), &device)
	return device, err
}
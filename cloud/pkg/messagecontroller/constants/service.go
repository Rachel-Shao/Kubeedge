package constants

// Service level constants
const (
	ResourceNodeIDIndex       = 1
	ResourceNamespaceIndex    = 2
	ResourceResourceTypeIndex = 3
	ResourceResourceNameIndex = 4

	ResourceNode = "node"
	ResourceTypeTwinEdgeUpdated = "twin/edge_updated"

	// Group
	GroupResource = "resource"
	GroupTwin = "twin"

	// Topic
	TopicNodeStatus = "TopicNodeStatus"
	TopicNode = "TopicNode"
	TopicPod = "TopicPod"
	TopicPodStatus = "TopicPodStatus"
	TopicDeviceStatus = "TopicDeviceStatus"

	// Nvidia Constants
	// NvidiaGPUStatusAnnotationKey is the key of the node annotation for GPU status
	NvidiaGPUStatusAnnotationKey = "huawei.com/gpu-status"
	// NvidiaGPUScalarResourceName is the device plugin resource name used for special handling
	NvidiaGPUScalarResourceName = "nvidia.com/gpu"
)

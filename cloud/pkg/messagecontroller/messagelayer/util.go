package messagelayer

import (
	"errors"
	"fmt"
	"github.com/kubeedge/beehive/pkg/core/model"
	deviceconstants "github.com/kubeedge/kubeedge/cloud/pkg/devicecontroller/constants"
	controller "github.com/kubeedge/kubeedge/cloud/pkg/edgecontroller/constants"
	"github.com/kubeedge/kubeedge/common/constants"
	pkgutil "github.com/kubeedge/kubeedge/pkg/util"
	"k8s.io/klog/v2"
	"strings"
)

// BuildResource return a string as "beehive/pkg/core/model".Message.Router.Resource
func BuildResource(nodeID, namespace, resourceType, resourceID string) (resource string, err error) {
	if namespace == "" || resourceType == "" || nodeID == "" {
		err = fmt.Errorf("required parameter are not set (node id, namespace or resource type)")
		return
	}

	resource = pkgutil.ConcatStrings(controller.ResourceNode, constants.ResourceSep, nodeID, constants.ResourceSep, namespace, constants.ResourceSep, resourceType)
	if resourceID != "" {
		resource += pkgutil.ConcatStrings(constants.ResourceSep, resourceID)
	}
	return
}

// BuildResourceForRouter return a string as "beehive/pkg/core/model".Message.Router.Resource
func BuildResourceForRouter(resourceType, resourceID string) (string, error) {
	if resourceID == "" || resourceType == "" {
		return "", fmt.Errorf("required parameter are not set (resourceID or resource type)")
	}
	return pkgutil.ConcatStrings(resourceType, constants.ResourceSep, resourceID), nil
}

// getElementByIndex returns a string from "beehive/pkg/core/model".Message.Router.Resource by index
func getElementByIndex(msg model.Message, index int) string {
	sli := strings.Split(msg.GetResource(), constants.ResourceSep)
	if len(sli) <= index {
		return ""
	}
	return sli[index]
}

// GetNodeID from "beehive/pkg/core/model".Message.Router.Resource
func GetNodeID(msg model.Message) (string, error) {
	res := getElementByIndex(msg, controller.ResourceNodeIDIndex)
	if res == "" {
		return "", fmt.Errorf("node id not found")
	}
	klog.V(4).Infof("The node id %s, %d", res, controller.ResourceNodeIDIndex)
	return res, nil
}

// GetNamespace from "beehive/pkg/core/model".Model.Router.Resource
func GetNamespace(msg model.Message) (string, error) {
	res := getElementByIndex(msg, controller.ResourceNamespaceIndex)
	if res == "" {
		return "", fmt.Errorf("namespace not found")
	}
	klog.V(4).Infof("The namespace %s, %d", res, controller.ResourceNamespaceIndex)
	return res, nil
}

// GetResourceType from "beehive/pkg/core/model".Model.Router.Resource
func GetResourceType(msg model.Message) (string, error) {
	res := getElementByIndex(msg, controller.ResourceResourceTypeIndex)
	if res == "" {
		return "", fmt.Errorf("resource type not found")
	}
	klog.V(4).Infof("The resource type is %s, %d", res, controller.ResourceResourceTypeIndex)
	return res, nil
}

// GetResourceName from "beehive/pkg/core/model".Model.Router.Resource
func GetResourceName(msg model.Message) (string, error) {
	res := getElementByIndex(msg, controller.ResourceResourceNameIndex)
	if res == "" {
		return "", fmt.Errorf("resource name not found")
	}
	klog.V(4).Infof("The resource name is %s, %d", res, controller.ResourceResourceNameIndex)
	return res, nil
}

// Device Part
// BuildResource return a string as "beehive/pkg/core/model".Message.Router.Resource
func BuildDeviceResource(nodeID, resourceType, resourceID string) (resource string, err error) {
	if nodeID == "" || resourceType == "" {
		err = fmt.Errorf("required parameter are not set (node id, namespace or resource type)")
		return
	}
	resource = fmt.Sprintf("%s%s%s%s%s", deviceconstants.ResourceNode, constants.ResourceSep, nodeID, constants.ResourceSep, resourceType)
	if resourceID != "" {
		resource += fmt.Sprintf("%s%s", constants.ResourceSep, resourceID)
	}
	return
}

// GetDeviceID returns the ID of the device
func GetDeviceID(resource string) (string, error) {
	res := strings.Split(resource, "/")
	if len(res) >= deviceconstants.ResourceDeviceIDIndex+1 && res[deviceconstants.ResourceDeviceIndex] == deviceconstants.ResourceDevice {
		return res[deviceconstants.ResourceDeviceIDIndex], nil
	}
	return "", errors.New("failed to get device id")
}

// GetResourceType returns the resourceType of message received from edge
func GetDeviceResourceType(resource string) (string, error) {
	if strings.Contains(resource, deviceconstants.ResourceTypeTwinEdgeUpdated) {
		return deviceconstants.ResourceTypeTwinEdgeUpdated, nil
	} else if strings.Contains(resource, deviceconstants.ResourceTypeMembershipDetail) {
		return deviceconstants.ResourceTypeMembershipDetail, nil
	}

	return "", fmt.Errorf("unknown resource, found: %s", resource)
}

// GetNodeID from "beehive/pkg/core/model".Message.Router.Resource
func GetDeviceNodeID(msg model.Message) (string, error) {
	sli := strings.Split(msg.GetResource(), constants.ResourceSep)
	if len(sli) <= deviceconstants.ResourceNodeIDIndex {
		return "", fmt.Errorf("node id not found")
	}
	return sli[deviceconstants.ResourceNodeIDIndex], nil
}
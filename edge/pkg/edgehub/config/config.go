package config

import (
	"strings"
	"sync"

	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
)

var Config Configure
var once sync.Once

type Configure struct {
	v1alpha1.EdgeHub
	WebSocketURL []string
	NodeName     string
}

func InitConfigure(eh *v1alpha1.EdgeHub, nodeName string) {
	once.Do(func() {
		var webURLs []string
		for _, v := range eh.WebSocket.Server {
			wbURL := strings.Join([]string{"wss:/", v, eh.ProjectID, nodeName, "events"}, "/")
			webURLs = append(webURLs, wbURL)
		}

		klog.Infof("[sxy] the webURL is %v", webURLs)
		Config = Configure{
			EdgeHub:      *eh,
			WebSocketURL: webURLs,
			NodeName:     nodeName,
		}
	})
}

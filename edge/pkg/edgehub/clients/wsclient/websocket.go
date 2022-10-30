package wsclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/config"
	"github.com/kubeedge/viaduct/pkg/api"
	wsclient "github.com/kubeedge/viaduct/pkg/client"
	"github.com/kubeedge/viaduct/pkg/conn"
)

const (
	retryCount       = 5
	cloudAccessSleep = 5 * time.Second
)

// WebSocketClient a websocket client
type WebSocketClient struct {
	config     *WebSocketConfig
	clients    map[string]*wsclient.Client
	connection conn.Connection
}

// WebSocketConfig config for websocket
type WebSocketConfig struct {
	URL              []string
	CertFilePath     string
	KeyFilePath      string
	HandshakeTimeout time.Duration
	ReadDeadline     time.Duration
	WriteDeadline    time.Duration
	NodeID           string
	ProjectID        string
}

// NewWebSocketClient initializes a new websocket client instance
func NewWebSocketClient(conf *WebSocketConfig) *WebSocketClient {
	return &WebSocketClient{
		config: conf,
		clients: make(map[string]*wsclient.Client),
	}
}

// Init initializes websocket client
func (wsc *WebSocketClient) Init() error {
	klog.Infof("Websocket start to connect Access")
	cert, err := tls.LoadX509KeyPair(wsc.config.CertFilePath, wsc.config.KeyFilePath)
	if err != nil {
		klog.Errorf("Failed to load x509 key pair: %v", err)
		return fmt.Errorf("failed to load x509 key pair, error: %v", err)
	}

	caCert, err := ioutil.ReadFile(config.Config.TLSCAFile)
	if err != nil {
		return err
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(caCert); !ok {
		return fmt.Errorf("cannot parse the certificates")
	}

	tlsConfig := &tls.Config{
		RootCAs:            pool,
		Certificates:       []tls.Certificate{cert},
		//InsecureSkipVerify: false,
		InsecureSkipVerify: true,
		ClientAuth: tls.NoClientCert,
	}

	var currentClient *wsclient.Client
	klog.Infof("[sxy] 开始创建所有client")
	for i, v := range wsc.config.URL {
		klog.Infof("[sxy] 处理： %s", v)
		option := wsclient.Options{
			HandshakeTimeout: wsc.config.HandshakeTimeout,
			TLSConfig:        tlsConfig,
			Type:             api.ProtocolTypeWS,
			Addr:             v,
			AutoRoute:        false,
			ConnUse:          api.UseTypeMessage,
		}
		exOpts := api.WSClientOption{Header: make(http.Header)}
		exOpts.Header.Set("node_id", wsc.config.NodeID)
		exOpts.Header.Set("project_id", wsc.config.ProjectID)
		client := &wsclient.Client{Options: option, ExOpts: exOpts}

		url := strings.Split(v, "/")
		klog.Infof("[sxy] ip:port is %s", url[2])
		wsc.clients[url[2]] = client

		if i == 0 {
			klog.Infof("[sxy] 连接默认的CloudCore：%s", url[2])
			currentClient = client
		}
	}

	if currentClient == nil {
		for _, v := range wsc.clients {
			if v != nil {
				klog.Infof("[sxy] 连接默认的CloudCore：%s", v)
				currentClient = v
				break
			}
		}
	}

	for i := 0; i < retryCount; i++ {
		if currentClient == nil {
			klog.Errorf("current Client is nil")
			return nil
		}
		connection, err := currentClient.Connect()
		if err != nil {
			klog.Errorf("Init websocket connection failed %s", err.Error())
			klog.Infof("[sxy] 默认连接失败")
		} else {
			wsc.connection = connection
			klog.Infof("Websocket connect to cloud access successful")
			klog.Infof("[sxy] 默认连接成功")
			return nil
		}
		time.Sleep(cloudAccessSleep)
	}
	return errors.New("max retry count reached when connecting to cloud")
}

//UnInit closes the websocket connection
func (wsc *WebSocketClient) UnInit() {
	wsc.connection.Close()
}

//Send sends the message as JSON object through the connection
func (wsc *WebSocketClient) Send(message model.Message) error {
	err := wsc.connection.SetWriteDeadline(time.Now().Add(wsc.config.WriteDeadline))
	if err != nil {
		return err
	}
	return wsc.connection.WriteMessageAsync(&message)
}

//Receive reads the binary message through the connection
func (wsc *WebSocketClient) Receive() (model.Message, error) {
	message := model.Message{}
	err := wsc.connection.ReadMessage(&message)
	return message, err
}

//Notify logs info
func (wsc *WebSocketClient) Notify(authInfo map[string]string) {
	klog.Infof("no op")
}

//Exchange the connect
func (wsc *WebSocketClient) Exchange(addr string) error {
	klog.Infof("[sxy] 执行交换")
	//var currentConn conn.Connection

	// 获取证书

	newClient := wsc.clients[addr]
	if newClient == nil {
		return errors.New("can not find the client for addr")
	}
	for i := 0; i < retryCount; i++ {
		connection, err := newClient.Connect()
		if err != nil {
			klog.Errorf("Exchange websocket connection failed %s", err.Error())
		} else {
			//currentConn = wsc.connection
			wsc.connection = connection
			klog.Infof("[sxy] Exchange Successful")
			klog.Infof("Websocket exchange connect to cloud access successful")

			//klog.Infof("[sxy] 等待60s关闭原链接")
			//time.Sleep(time.Second*60)
			//currentConn.Close()
			//klog.Infof("[sxy] 关闭原链接")
			return nil
		}
		time.Sleep(cloudAccessSleep)
	}

	return errors.New("max retry count reached when connecting to cloud")
}

func (wsc *WebSocketClient) GetRemoteAddr() string {
	return wsc.connection.RemoteAddr().String()
}
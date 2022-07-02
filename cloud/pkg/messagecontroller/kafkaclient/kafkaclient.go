package kafkaclient

import (
	"encoding/json"
	errs "errors"
	"github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/handler"
	v1 "k8s.io/api/core/v1"

	"github.com/Shopify/sarama"
	"k8s.io/klog/v2"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	messageconst "github.com/kubeedge/kubeedge/cloud/pkg/messagecontroller/constants"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/cloudcore/v1alpha1"
)

type SortedContainerStatuses []v1.ContainerStatus

func (s SortedContainerStatuses) Len() int      { return len(s) }
func (s SortedContainerStatuses) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s SortedContainerStatuses) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

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

type KafkaClient struct {
	address      string
	topicNum     int
	partitionNum int
	client       sarama.Client
	producer     sarama.AsyncProducer
	consumer     sarama.Consumer
}

func NewKafkaClient(c *v1alpha1.MessageController) (*KafkaClient, error) {
	kafkaClient := &KafkaClient{}
	kafkaClient.address = c.KafkaConfig.Address + ":" + c.KafkaConfig.Port
	kafkaClient.topicNum = int(c.KafkaConfig.TopicNum)
	kafkaClient.partitionNum = int(c.KafkaConfig.PartitionNum)

	klog.Info("[sxy] create a kafka client: %s, topicNum: %v, partitionNum: %v", kafkaClient.address, kafkaClient.topicNum, kafkaClient.partitionNum)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Consumer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	client, err := sarama.NewClient([]string{kafkaClient.address}, config)
	//defer client.Close() // sxy:记得关闭
	if err != nil {
		klog.Errorf("Failed to connect to the kafka cluster(%s) error: %v", kafkaClient.address, err)
		panic(err)
	}

	// sxy: 记得创建topic，查找topic
	//admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	//if err != nil {
	//
	//}

	// create producer
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	// create consumer
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}

	kafkaClient.client = client
	kafkaClient.producer = producer
	kafkaClient.consumer = consumer

	klog.Infof("Connect to Kafka Cluster: %s successfully.", kafkaClient.address)
	return kafkaClient, nil
}

func (kc KafkaClient) GetTopicNum() int {
	return kc.topicNum
}

func (kc KafkaClient) GetPartitionNum() int {
	return kc.partitionNum
}

func (kc KafkaClient) Reconnect() error {
	var retry = 3
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Consumer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	for i:=0;i<retry;i++ {
		cl, err := sarama.NewClient([]string{kc.address}, config)
		//defer client.Close() // sxy:记得关闭
		if err != nil {
			klog.Errorf("Failed to connect to the kafka cluster(%s) error: %v", kc.address, err)
			continue
		}

		producer, err := sarama.NewAsyncProducerFromClient(cl)
		if err != nil {
			continue
		}
		consumer, err := sarama.NewConsumerFromClient(cl)
		if err != nil {
			continue
		}

		kc.client =  cl
		kc.producer = producer
		kc.consumer = consumer
		return nil
	}
	return errs.New("failed to connect to kafka client")
}

func (kc KafkaClient) PublicToKafka(message model.Message, topic string, nodeName string) error {
	// check kafkaClient
	if kc.client == nil {
		// reconnect sxy: 需要吗？
		// kc.Reconnect()
	}
	msgByte, err := json.Marshal(message) // sxy:应该消耗时间不多吧？
	if err != nil {
		klog.Errorf("failed to marshal")
		return err
	}
	kc.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(nodeName),
		Value: sarama.ByteEncoder(msgByte),
	}

	select {
	case suc := <-kc.producer.Successes():
		klog.Infof("partition: %d, offset: %d", suc.Partition, suc.Offset)
		return nil
	case fail := <-kc.producer.Errors():
		return fail.Err
	}
}

func (kc KafkaClient) ConsumeFromKafka(topic string, handler *handler.Handler) {
	partitionList, err := kc.consumer.Partitions(topic)
	if err != nil {
		klog.Errorf("failed to start consumer partition, err: %v", err)
		return
	}
	topicType := getTopicType(topic)

	for _, partition := range partitionList {
		klog.Infof("[sxy] 为partition %v 创建消费者并消费", partition)
		partitionConsumer, err := kc.consumer.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			klog.Errorf("failed to start consumer partition, err: %v", err)
			return
		}
		// sxy: 需要关闭
		//defer partitionConsumer.Close()
		go func(p int32) {
			for {
				select {
				case <-beehiveContext.Done():
					klog.Warning("MetaManager main loop stop")
					return
				default:
				}

				select {
				case msg := <-partitionConsumer.Messages():
					klog.Infof("[sxy] 为partition %v 处理消息", p)
					klog.Infof("msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
						msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))

					//直接消费
					message := &model.Message{}
					err = json.Unmarshal(msg.Value, message)
					if err != nil {
						klog.Errorf("failed to unmarshal msg")
						continue
					}

					switch topicType {
					case messageconst.TopicNode:
						// sxy: jump
					case messageconst.TopicNodeStatus:
						handler.UpdateNodeStatus(*message)
					case messageconst.TopicPod:
						// sxy: jump
					case messageconst.TopicPodStatus:
						handler.UpdatePodStatus(*message)
					case messageconst.TopicDeviceStatus:
						handler.UpdateDeviceStatus(*message)
					default:
						klog.Infof("unsupported topic type")
						continue
					}
				case err := <-partitionConsumer.Errors():
					klog.Infof("[sxy] failed to get msg from partitions")
					klog.Infof("err :%s\n", err.Error())
				}
			}
		}(partition)
	}

	//for {
	//	select {
	//	case <- beehiveContext.Done():
	//		return
	//	default:
	//		continue
	//	}
	//}
}

func getTopicType(topicName string) string {
	i := len(topicName)
	for {
		if i<0 {
			break
		}
		if topicName[i] >= '0' && topicName[i] <= '9' {
			i--
		}else{
			break
		}
	}
	return topicName[:i+1]
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	_ "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/go-yaml/yaml"
)

type labels struct {
	ClusterArn   string `yaml:"cluster_arn"`
	ClusterName  string `yaml:"cluster_name"`
	KafkaVersion string `yaml:"kafka_version"`
	Exporter     string `yaml:"exporter"`
	MetricsPath  string `yaml:"__metrics_path__,omitempty"`
	Scheme       string `yaml:"__scheme__,omitempty"`
}

var cluster = flag.String("config.cluster", "", "arn of the MSK cluster to scrape")
var outFile = flag.String("config.write-to", "msk_file_sd.yml", "path of file to write MSK service discovery information to")
var interval = flag.Duration("config.scrape-interval", 60*time.Second, "interval at which to scrape the AWS API for MSK service discovery information")
var times = flag.Int("config.scrape-times", 0, "how many times to scrape before exiting (0 = infinite)")
var roleArn = flag.String("config.role-arn", "", "arn of the role to assume when scraping the AWS API (optional)")
var jmxMetrics = flag.Bool("config.jmx-metrics", true, "add the jmx metrics to the out file (default = true)")
var nodeMetrics = flag.Bool("config.node-metrics", true, "add the jmx metrics to the out file (default = true)")

var prometheusJmxPort = 11001
var prometheusNodePort = 11002

// PrometheusKafkaInfo is the final structure that will be
// output as a Prometheus file service discovery config.
type PrometheusKafkaInfo struct {
	Targets []string `yaml:"targets"`
	Labels  labels   `yaml:"labels"`
}

// logError is a convenience function that decodes all possible aws
// errors and displays them to standard error.
func logError(err error) {
	if err != nil {
		var oe *smithy.OperationError
		if errors.As(err, &oe) {
			log.Printf("failed to call service: %s, operation: %s, error: %v", oe.Service(), oe.Operation(), oe.Unwrap())
		} else {
			log.Println(err.Error())
		}
	}
}

// GetClusters retrieves a list of *ClusterArns from Amazon MSK,
// dealing with the mandatory pagination as needed.
func GetClusters(client *kafka.Client) (*kafka.ListClustersV2Output, error) {
	input := &kafka.ListClustersV2Input{}
	output := &kafka.ListClustersV2Output{}
	for {
		myoutput, err := client.ListClustersV2(context.Background(), input)
		if err != nil {
			return nil, err
		}
		output.ClusterInfoList = append(output.ClusterInfoList, myoutput.ClusterInfoList...)
		if myoutput.NextToken == nil {
			break
		}
		input.NextToken = myoutput.NextToken
	}
	return output, nil
}

// TransformUrlsForPort transforms kafka broker string into a list of urls for prometheus
func TransformUrlsForPort(brokerString *string, port *int) []string {
	var finalUrls []string
	splitUrls := strings.Split(*brokerString, ",")

	for _, element := range splitUrls {
		dnsAndPort := strings.Split(element, ":")
		finalUrls = append(finalUrls, fmt.Sprintf("%s:%d", dnsAndPort[0], *port))
	}

	return finalUrls
}

func main() {
	flag.Parse()

	conf, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logError(err)
		return
	}

	if !*jmxMetrics && !*nodeMetrics {
		log.Print("no metrics enabled, exiting. see conf.jmx-metrics and conf.node-metrics and enable one of them")
		return
	}

	if *roleArn != "" {
		// Assume role
		stsSvc := sts.NewFromConfig(conf)
		conf.Credentials = stscreds.NewAssumeRoleProvider(stsSvc, *roleArn)
	}

	// Initialise AWS Service clients
	msk := kafka.NewFromConfig(conf)

	work := func() {
		var clusters *kafka.ListClustersV2Output

		if *cluster != "" {
			res, err := msk.DescribeClusterV2(context.Background(), &kafka.DescribeClusterV2Input{
				ClusterArn: cluster,
			})
			if err != nil {
				logError(err)
				return
			}

			if res.ClusterInfo == nil {
				logError(fmt.Errorf("%s cluster not found", *cluster))
				return
			}

			clusters = &kafka.ListClustersV2Output{
				ClusterInfoList: []types.Cluster{*res.ClusterInfo},
			}
		} else {
			c, err := GetClusters(msk)
			if err != nil {
				logError(err)
				return
			}
			clusters = c
		}

		var promKafkaInfo []*PrometheusKafkaInfo

		for _, cluster := range clusters.ClusterInfoList {
			description, err := msk.GetBootstrapBrokers(context.Background(), &kafka.GetBootstrapBrokersInput{
				ClusterArn: cluster.ClusterArn,
			})
			if err != nil {
				logError(err)
				return
			}

			var brokerString *string

			// fetch the broker urls
			if description.BootstrapBrokerString != nil {
				brokerString = description.BootstrapBrokerString
			} else if description.BootstrapBrokerStringTls != nil {
				brokerString = description.BootstrapBrokerStringTls
			} else if description.BootstrapBrokerStringSaslScram != nil {
				brokerString = description.BootstrapBrokerStringSaslScram
			} else if description.BootstrapBrokerStringSaslIam != nil {
				brokerString = description.BootstrapBrokerStringSaslIam
			} else {
				log.Printf("[%s] failed to obtain broker string, skipping", *cluster.ClusterArn)
			}

			labels := labels{
				ClusterArn:   *cluster.ClusterArn,
				ClusterName:  *cluster.ClusterName,
				KafkaVersion: *cluster.CurrentVersion,
			}

			// transform urls
			if *jmxMetrics {
				labels.Exporter = "jmx"
				urls := TransformUrlsForPort(brokerString, &prometheusJmxPort)
				promKafkaInfo = append(promKafkaInfo,
					&PrometheusKafkaInfo{urls, labels})
			}

			if *nodeMetrics {
				labels.Exporter = "node"
				urls := TransformUrlsForPort(brokerString, &prometheusNodePort)
				promKafkaInfo = append(promKafkaInfo,
					&PrometheusKafkaInfo{urls, labels})
			}
		}

		m, err := yaml.Marshal(promKafkaInfo)
		if err != nil {
			logError(err)
			return
		}

		log.Printf("Writing %d discovered exporters to %s", len(promKafkaInfo), *outFile)
		err = ioutil.WriteFile(*outFile, m, 0644)
		if err != nil {
			logError(err)
			return
		}

	}

	// Run the worker according to the parameters set
	s := time.NewTimer(1 * time.Millisecond)
	t := time.NewTicker(*interval)
	n := *times
	for {
		select {
		case <-s.C:
		case <-t.C:
		}
		work()
		n = n - 1
		if *times > 0 && n == 0 {
			break
		}
	}
}

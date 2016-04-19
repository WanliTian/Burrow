package main

import (
	"encoding/json"
	log "github.com/cihub/seelog"
	"reflect"
	"sort"
	"time"
)

type LagMonitor struct {
	closec   chan struct{}
	clusters map[string][]*ClusterLagMonitor
	groups   map[string][]string
}

type ClusterLagMonitor struct {
	app     *ApplicationContext
	closec  chan struct{}
	cluster string
	group   string
}

func NewLagMonitor(context *ApplicationContext) *LagMonitor {
	lm := &LagMonitor{
		closec:   make(chan struct{}),
		clusters: make(map[string][]*ClusterLagMonitor),
		groups:   make(map[string][]string),
	}

	for cluster, _ := range context.Config.Kafka {
		lm.groups[cluster] = getConsumers(context, cluster)
		length := len(lm.groups[cluster])
		lm.clusters[cluster] = make([]*ClusterLagMonitor, length)
		for index, group := range lm.groups[cluster] {
			lm.clusters[cluster][index] = NewClusterLagMonitor(cluster, group, context)
		}
	}

	go func() {
		tick := time.NewTimer(30 * time.Second)
		for {
			select {
			case <-lm.closec:
				for _, entry := range lm.clusters {
					for _, each := range entry {
						each.Close()
					}
				}
				return
			case <-tick.C:
				for cluster, groups := range lm.groups {
					newGroups := getConsumers(context, cluster)
					if !reflect.DeepEqual(groups, newGroups) {
						for _, ngroup := range newGroups {
							if !contains(ngroup, groups) {
								lm.clusters[cluster] = append(lm.clusters[cluster], NewClusterLagMonitor(cluster, ngroup, context))
							}
						}
						lm.groups[cluster] = newGroups
					}
				}
				tick.Reset(30 * time.Second)
			}
		}
	}()

	return lm
}

func (this *LagMonitor) Close() {
	this.closec <- struct{}{}
}

func NewClusterLagMonitor(cluster, group string, context *ApplicationContext) *ClusterLagMonitor {
	clm := &ClusterLagMonitor{
		closec:  make(chan struct{}),
		cluster: cluster,
		group:   group,
		app:     context,
	}

	go func() {
		tick := time.NewTimer(time.Duration(clm.app.Config.Tickers.BrokerOffsets) * time.Second)
		for {
			select {
			case <-clm.closec:
				return
			case <-tick.C:
				tick.Reset(time.Duration(clm.app.Config.Tickers.BrokerOffsets) * time.Second)
				storageRequest := &RequestConsumerStatus{Result: make(chan *ConsumerGroupStatus), Cluster: clm.cluster, Group: clm.group, Showall: true}
				clm.app.Storage.requestChannel <- storageRequest
				result := <-storageRequest.Result
				if result.Status != StatusOK {
					jStr, _ := json.Marshal(result)
					log.Warnf("%s", jStr)
				}
			}
		}
	}()
	return clm
}

func (this *ClusterLagMonitor) Close() {
	this.closec <- struct{}{}
}

func getConsumers(app *ApplicationContext, cluster string) []string {
	storageRequest := &RequestConsumerList{Result: make(chan []string), Cluster: cluster}
	app.Storage.requestChannel <- storageRequest
	consumerList := <-storageRequest.Result
	sort.Strings(consumerList)
	return consumerList
}

func contains(ele string, slice []string) bool {
	for _, val := range slice {
		if val == ele {
			return true
		}
	}
	return false
}

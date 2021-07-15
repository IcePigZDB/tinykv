// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. get all suitable stores and then sort them according to their region size.
	// Suitable stores: use cluster.GetMaxStoreDownTime() to filter stores
	// In short, a suitable store should be up and the down time cannot be longer
	// than MaxStoreDownTime of the cluster, which you can get through cluster.GetMaxStoreDownTime().
	// log.Infof("++++Schedule")
	stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		// log.Infof("all store :%v", store.GetID())
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
			// log.Infof("suitable store :%v", store.GetID())
		}
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	// 2. pick source store and region
	// Scheduler tries to find regions to move from the store with the biggest region size
	// First, it will try to select a pending region because pending may mean the disk is overloaded.
	// If there isn’t a pending region, it will try to find a follower region.
	// If it still cannot pick out one region, it will try to pick leader regions.
	// Finally, it will select out the region to move, or the Scheduler will try the next store which has
	// a smaller region size until all stores will have been tried.
	var region *core.RegionInfo
	var srcStore, dstStore *core.StoreInfo
	for i, store := range stores {
		var regions core.RegionsContainer
		storeId := store.GetID()
		cluster.GetPendingRegionsWithLock(storeId, func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			srcStore = stores[i]
			// log.Infof("pending region.id:%d of store:%d", region.GetID(), srcStore.GetID())
			break
		}
		cluster.GetFollowersWithLock(storeId, func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			srcStore = stores[i]
			// log.Infof("follower region.id:%d of store:%d", region.GetID(), srcStore.GetID())
			break
		}
		cluster.GetLeadersWithLock(storeId, func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			srcStore = stores[i]
			// log.Infof("leader region.id:%d of store:%d", region.GetID(), srcStore.GetID())
			break
		}
	}
	if region == nil {
		return nil
	}
	storeIds := region.GetStoreIds()
	// log.Infof("storeIds:%v", storeIds)
	// no need to move if replicas num < MaxReplicas
	if len(storeIds) < cluster.GetMaxReplicas() {
		// log.Infof("no need to mv: replicas num %d < MaxReplicas %d", len(storeIds), cluster.GetMaxReplicas())
		return nil
	}

	// 3. pick target store to move
	// Actually, the Scheduler will select the store with the smallest region size.
	// no need to move if dstStore has this region already including srcStore==dstStore
	for i := range stores {
		store := stores[len(stores)-i-1]
		// log.Infof("storeId:%v", store.GetID())
		if _, ok := storeIds[store.GetID()]; !ok {
			dstStore = store
			// log.Infof("dstStore:%d", dstStore.GetID())
			break
		}
	}

	if dstStore == nil {
		// log.Infof("no enough store to set dstStore")
		return nil
	}
	// 4. Then the Scheduler will judge whether this movement is valuable,
	// by checking the difference between region sizes of the original store and
	// the target store. If the difference is big enough, the Scheduler should
	// allocate a new peer on the target store and create a move peer operator.
	if srcStore.GetRegionSize()-dstStore.GetRegionSize() < 2*region.GetApproximateSize() {
		return nil
	}

	newPeer, err := cluster.AllocPeer(dstStore.GetID())
	if err != nil {
		return nil
	}
	desc := fmt.Sprintf("mv region %d's peer: store %v to %v", region.GetID(), srcStore.GetID(), dstStore.GetID())
	op, err := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance,
		srcStore.GetID(), dstStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	// log.Infof("return op.desc:%s", op.Desc())
	return op
}

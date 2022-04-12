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
	// 选出适合移动的store
	tempStores := cluster.GetStores()
	stores := make([]*core.StoreInfo, 0)
	for _, store := range tempStores {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	// 不能移动
	if len(stores) <= 1 {
		return nil
	}
	// 按照 regionSize 从大到小进行排序
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
	// 从 regionSize 最大的开始遍历
	var regionInfo *core.RegionInfo
	var fromStore, toStore *core.StoreInfo
	for i, st := range stores {
		// 先尝试选择一个挂起的区域，因为挂起可能意味着磁盘过载（？）
		cluster.GetPendingRegionsWithLock(st.GetID(), func(rc core.RegionsContainer) {
			regionInfo = rc.RandomRegion(nil, nil) // 选择随机区域
		})
		if regionInfo != nil {
			fromStore = stores[i]
			break
		}
		// 否则，再尝试找到一个 follower 区域
		cluster.GetFollowersWithLock(st.GetID(), func(rc core.RegionsContainer) {
			regionInfo = rc.RandomRegion(nil, nil) // 选择随机区域
		})
		if regionInfo != nil {
			fromStore = stores[i]
			break
		}
		// 否则，最后尝试挑选 leader 区域
		cluster.GetLeadersWithLock(st.GetID(), func(rc core.RegionsContainer) {
			regionInfo = rc.RandomRegion(nil, nil) // 选择随机区域
		})
		if regionInfo != nil {
			fromStore = stores[i]
			break
		}
	}
	// 如果找不到目标 region，直接放弃本次操作
	if regionInfo == nil {
		return nil
	}
	// 判断目标 region 的 store 数量, 如果小于 cluster.GetMaxReplicas()，直接放弃本次操作
	storeIds := regionInfo.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}
	// 从 regionSize 最小的开始遍历，选出一个目标 store
	regionStores := cluster.GetRegionStores(regionInfo) // 已有该region的store
	for i := range stores {
		st := stores[len(stores)-i-1]
		// 目标 store 不能和原 store 为一个store
		if st.GetID() != fromStore.GetID() {
			// 目标 store 不能在原来的 region 里面
			isInRegionStores := false
			for _, local := range regionStores {
				if local.GetID() == st.GetID() {
					isInRegionStores = true
					break
				}
			}
			if !isInRegionStores {
				toStore = st
				break
			}
		}
	}
	// 如果目标 store 找不到，直接放弃
	if toStore == nil {
		return nil
	}
	// 判断两个 store 的 regionSize 是否小于等于 2*ApproximateSize
	if fromStore.GetRegionSize()-toStore.GetRegionSize() <= 2*regionInfo.GetApproximateSize() {
		return nil
	}
	// 在目标 store 中创建新的 peer
	newPeer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		return nil
	}
	// 创建一个移动 peer 的 operator
	peerOperator, err := operator.CreateMovePeerOperator("balance-region", cluster, regionInfo, operator.OpBalance, fromStore.GetID(), toStore.GetID(), newPeer.Id)
	if err != nil {
		return nil
	}
	return peerOperator
}

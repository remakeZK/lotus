package sync

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"

	"github.com/raulk/clock"
	"github.com/stretchr/testify/require"
)

type syncstate struct {
	err  error
	data *api.SyncState
}

type chainhead struct {
	err  error
	data *types.TipSet
}

type testapi struct {
	syncstate chan syncstate
	chainhead chan chainhead
}

func (a *testapi) SyncState(context.Context) (*api.SyncState, error) {
	v := <-a.syncstate
	if v.err != nil {
		return nil, v.err
	}

	return v.data, nil
}

func (a *testapi) ChainHead(context.Context) (*types.TipSet, error) {
	v := <-a.chainhead
	if v.err != nil {
		return nil, v.err
	}

	return v.data, nil
}

func TestSyncWait(t *testing.T) {
	/*
		build.Clock = clock.NewMock()

		ta := &testapi{}
		ta.syncstate = make(chan syncstate)
		ta.chainhead = make(chan chainhead)

		go func() {
			ta.syncstate <- syncstate{
				err: nil,
				data: &api.SyncState{
					ActiveSyncs: []api.ActiveSync{api.ActiveSync{
						WorkerID: 1000,
						Base:     nil,
						Target:   nil,
						Stage:    api.StageSyncComplete,
						Height:   abi.ChainEpoch(2880),
						Start:    build.Clock.Now().Add(-1 * 30 * time.Second),
						End:      build.Clock.Now(),
						Message:  "whynot",
					}},
				},
			}
		}()

		require.Nil(t, SyncWait(context.Background(), ta))
	*/
}

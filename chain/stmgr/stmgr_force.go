package stmgr

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
)

func (sm *StateManager) ValidateChainFromSpecialHeight(ctx context.Context, ts *types.TipSet,
	shutdown chan struct{},
	height, empty int) error {
	defer close(shutdown)

	tschain := []*types.TipSet{ts}
	for ts.Height() != 0 {
		if ts.Height() < abi.ChainEpoch(empty) {
			break
		}
		next, err := sm.cs.LoadTipSet(ts.Parents())
		if err != nil {
			return err
		}

		tschain = append(tschain, next)
		ts = next
	}

	flag := true
	lastState := tschain[len(tschain)-1].ParentState()
	for i := len(tschain) - 1; i >= 0; i-- {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		cur := tschain[i]
		if cur.Height() < abi.ChainEpoch(height) {
			continue
		}
		if flag {
			lastState = cur.ParentState()
			flag = false
		}
		log.Infof("computing state (height: %d, ts=%s)", cur.Height(), cur.Cids())
		if cur.ParentState() != lastState {
			return fmt.Errorf("tipset chain had state mismatch at height %d, cur.ParentState %s lastState %s", cur.Height(), cur.ParentState(), lastState)
		}
		st, _, err := sm.TipSetState(ctx, cur)
		if err != nil {
			return err
		}
		lastState = st
	}

	return nil
}

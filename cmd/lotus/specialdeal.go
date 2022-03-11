package main

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/consensus/filcns"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/journal/fsjournal"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
)

var SpecialCmd = &cli.Command{
	Name:  "validate-special",
	Usage: "validate a chain from special height, only verify one height",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "cid",
			Usage:    "tipsets want calculate",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		r, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return fmt.Errorf("opening fs repo: %w", err)
		}

		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return fmt.Errorf("repo init error: %w", err)
		}

		return ValidateTipset(cctx.Context, r, cctx.String("cid"))
	},
}

func ValidateTipset(ctx context.Context, r repo.Repo, cidString string) error {

	lr, err := r.Lock(repo.FullNode)
	if err != nil {
		return err
	}
	defer lr.Close() //nolint:errcheck

	bs, err := lr.Blockstore(ctx, repo.UniversalBlockstore)
	if err != nil {
		return fmt.Errorf("failed to open blockstore: %w", err)
	}

	mds, err := lr.Datastore(context.TODO(), "/metadata")
	if err != nil {
		return err
	}

	j, err := fsjournal.OpenFSJournal(lr, journal.EnvDisabledEvents())
	if err != nil {
		return fmt.Errorf("failed to open journal: %w", err)
	}
	cst := store.NewChainStore(bs, bs, mds, filcns.Weight, j)

	b, err := modules.RandomSchedule(modules.RandomBeaconParams{
		Cs:          cst,
		DrandConfig: modules.BuiltinDrandConfig(),
	}, dtypes.AfterGenesisSet{})
	if err != nil {
		return err
	}
	stm, err := stmgr.NewStateManager(cst, filcns.NewTipSetExecutor(), vm.Syscalls(ffiwrapper.ProofVerifier), filcns.DefaultUpgradeSchedule(), b)

	cids, err := lcli.ParseTipSetString(cidString)
	if err != nil {
		return err
	}
	ts, err := cst.LoadTipSet(types.NewTipSetKey(cids...))
	if err != nil {
		return fmt.Errorf("importing chain failed: %w", err)
	}

	tss, err := cst.LoadTipSet(ts.Parents())
	if err != nil {
		return fmt.Errorf("load tipset failed: %w", err)
	}
	st, _, err := stm.TipSetState(ctx, tss)
	if err != nil {
		return fmt.Errorf("bad tipset calculating: %w", err)
	}
	fmt.Println("tipset state", st, ts.ParentState())
	return nil
}

var SpecialChainDataCmd = &cli.Command{
	Name:  "validate-special-data",
	Usage: "validate a chain from special height, only verify one height",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "cid",
			Usage:    "tipsets want calculate",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		r, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return fmt.Errorf("opening fs repo: %w", err)
		}

		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return fmt.Errorf("repo init error: %w", err)
		}

		return ValidateChainData(cctx.Context, r, cctx.String("cid"))
	},
}

func ValidateChainData(ctx context.Context, r repo.Repo, cidString string) error {

	lr, err := r.Lock(repo.FullNode)
	if err != nil {
		return err
	}
	defer lr.Close() //nolint:errcheck

	bs, err := lr.Blockstore(ctx, repo.UniversalBlockstore)
	if err != nil {
		return fmt.Errorf("failed to open blockstore: %w", err)
	}

	mds, err := lr.Datastore(context.TODO(), "/metadata")
	if err != nil {
		return err
	}

	j, err := fsjournal.OpenFSJournal(lr, journal.EnvDisabledEvents())
	if err != nil {
		return fmt.Errorf("failed to open journal: %w", err)
	}
	cst := store.NewChainStore(bs, bs, mds, filcns.Weight, j)

	//b, err := modules.RandomSchedule(modules.RandomBeaconParams{
	//	Cs:          cst,
	//	DrandConfig: modules.BuiltinDrandConfig(),
	//}, dtypes.AfterGenesisSet{})
	//if err != nil {
	//	return err
	//}
	//stm, err := stmgr.NewStateManager(cst, filcns.NewTipSetExecutor(), vm.Syscalls(ffiwrapper.ProofVerifier), filcns.DefaultUpgradeSchedule(), b)

	cids, err := lcli.ParseTipSetString(cidString)
	if err != nil {
		return err
	}
	ts, err := cst.LoadTipSet(types.NewTipSetKey(cids...))
	if err != nil {
		return fmt.Errorf("importing chain failed: %w", err)
	}

	tss, err := cst.LoadTipSet(ts.Parents())
	if err != nil {
		return fmt.Errorf("load tipset failed: %w", err)
	}
	//st, _, err := stm.TipSetState(ctx, tss)
	//if err != nil {
	//	return fmt.Errorf("bad tipset calculating: %w", err)
	//}
	//fmt.Println("tipset state", st, ts.ParentState())
	//bh := tss.Blocks()[0]
	for {
		tss, err = cst.LoadTipSet(tss.Parents())
		if err == blockstore.ErrNotFound {
			fmt.Printf("chain end at %d\n", tss.Height())
			return nil
		} else if err != nil {
			panic(err)
		}

		fmt.Printf("loaded chain at %d\n", tss.Height())
	}
}

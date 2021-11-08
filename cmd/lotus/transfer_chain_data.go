package main

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/consensus/filcns"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/journal/fsjournal"
	"github.com/filecoin-project/lotus/node/repo"
)

var Transfermd = &cli.Command{
	Name:  "transfer-chaindata",
	Usage: "transfer a range chain data to dst repo",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "src-repo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dst-repo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "src-tail-ts",
			Required: true,
			Usage:    "the dst-tipset in src repo",
		},
		&cli.IntFlag{
			Name:     "start-height",
			Required: true,
		},
		&cli.IntFlag{
			Name:     "end-height",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		r, err := repo.NewFS(cctx.String("src-repo"))
		if err != nil {
			return fmt.Errorf("opening fs repo: %w", err)
		}

		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return fmt.Errorf("src repo error: %w", err)
		}

		rd, err := repo.NewFS(cctx.String("dst-repo"))
		if err != nil {
			return fmt.Errorf("opening fs repo: %w", err)
		}

		err = rd.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return fmt.Errorf("dst repo error: %w", err)
		}

		lr, err := r.Lock(repo.FullNode)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck

		bs, err := lr.Blockstore(cctx.Context, repo.UniversalBlockstore)
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
		cids, err := lcli.ParseTipSetString(cctx.String("src-tail-ts"))
		if err != nil {
			return err
		}

		ts, err := cst.LoadTipSet(types.NewTipSetKey(cids...))
		if err != nil {
			return fmt.Errorf("importing chain failed: %w", err)
		}

		start := cctx.Int("start-height")
		end := cctx.Int("end-height")

		log.Info("start, end height", start, end)

		lr1, err := rd.Lock(repo.FullNode)
		if err != nil {
			return err
		}
		defer lr1.Close() //nolint:errcheck

		bs1, err := lr1.Blockstore(cctx.Context, repo.UniversalBlockstore)
		if err != nil {
			return fmt.Errorf("failed to open blockstore: %w", err)
		}

		mds1, err := lr1.Datastore(context.TODO(), "/metadata")
		if err != nil {
			return err
		}

		unionBs := blockstore.Union(cst.ChainBlockstore(), cst.StateBlockstore())
		cst1 := store.NewChainStore(bs1, bs1, mds1, filcns.Weight, j)

		return cst.ForceChainExport(cctx.Context, ts, abi.ChainEpoch(start), abi.ChainEpoch(end), func(c cid.Cid) error {
			blk, err := unionBs.Get(c)
			if err != nil {
				return fmt.Errorf("get block from unionBs failed: %w", err)
			}
			if err = cst1.ChainBlockstore().Put(blk); err != nil {
				return fmt.Errorf("put blockstore failed: %w", err)
			}
			return nil
		})
	},
}

var Transfermd1 = &cli.Command{
	Name:  "transfer-chaindata1",
	Usage: "transfer a range chain data to dst repo",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "src-repo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dst-repo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "src-tail-ts",
			Required: true,
			Usage:    "the dst-tipset in src repo",
		},
	},
	Action: func(cctx *cli.Context) error {
		r, err := repo.NewFS(cctx.String("src-repo"))
		if err != nil {
			return fmt.Errorf("opening fs repo: %w", err)
		}

		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return fmt.Errorf("src repo error: %w", err)
		}

		rd, err := repo.NewFS(cctx.String("dst-repo"))
		if err != nil {
			return fmt.Errorf("opening fs repo: %w", err)
		}

		err = rd.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return fmt.Errorf("dst repo error: %w", err)
		}

		lr, err := r.Lock(repo.FullNode)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck

		bs, err := lr.Blockstore(cctx.Context, repo.UniversalBlockstore)
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
		cids, err := lcli.ParseTipSetString(cctx.String("src-tail-ts"))
		if err != nil {
			return err
		}

		lr1, err := rd.Lock(repo.FullNode)
		if err != nil {
			return err
		}
		defer lr1.Close() //nolint:errcheck

		bs1, err := lr1.Blockstore(cctx.Context, repo.UniversalBlockstore)
		if err != nil {
			return fmt.Errorf("failed to open blockstore: %w", err)
		}

		mds1, err := lr1.Datastore(context.TODO(), "/metadata")
		if err != nil {
			return err
		}

		unionBs := blockstore.Union(cst.ChainBlockstore(), cst.StateBlockstore())
		cst1 := store.NewChainStore(bs1, bs1, mds1, filcns.Weight, j)
		blk, err := unionBs.Get(cids[0])
		if err != nil {
			return fmt.Errorf("get blk failed in unionBs: %w", err)
		}
		err = cst1.ChainBlockstore().Put(blk)
		if err != nil {
			return fmt.Errorf("put blk failed into cst1: %w", err)
		}

		return nil
	},
}

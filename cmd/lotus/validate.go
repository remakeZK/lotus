package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/consensus/filcns"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/journal/fsjournal"
	"github.com/filecoin-project/lotus/node/repo"
	lru "github.com/hashicorp/golang-lru"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

// ValidateCmd is the command compute date already exist in store
var ValidateCmd = &cli.Command{
	Name:  "validate",
	Usage: "validate a chain from special height",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:     "start-height",
			Usage:    "the tipset height you want to start",
			Required: true,
		},
		&cli.IntFlag{
			Name:     "empty-height",
			Usage:    "from which height we don't need flush all data",
			Required: true,
			Value:    0,
		},
		&cli.StringFlag{
			Name:     "snapshot",
			Usage:    "the file path of snapshot",
			Required: true,
		},
		&cli.Int64Flag{
			Name:  "lru-cache",
			Usage: "the size of lru cache",
			Value: 1 << 20,
		},
	},
	Action: func(cctx *cli.Context) error {
		r, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return xerrors.Errorf("opening fs repo: %w", err)
		}

		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return xerrors.Errorf("repo init error: %w", err)
		}

		return ValidateChain(cctx.Context, r, cctx.String("snapshot"), cctx.Int("start-height"), cctx.Int("empty-height"), cctx.Int("lru-cache"))
	},
}

func ValidateChain(ctx context.Context, r repo.Repo, fname string, startHeight, emptyHeight, lruSize int) error {
	var rd io.Reader
	var err error
	fname, err = homedir.Expand(fname)
	if err != nil {
		return err
	}

	fi, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer fi.Close() //nolint:errcheck

	// st, err := os.Stat(fname)
	// if err != nil {
	//     return err
	// }

	rd = fi
	lr, err := r.Lock(repo.FullNode)
	if err != nil {
		return err
	}
	defer lr.Close() //nolint:errcheck

	bs, err := lr.Blockstore(ctx, repo.UniversalBlockstore)
	if err != nil {
		return xerrors.Errorf("failed to open blockstore: %w", err)
	}

	mds, err := lr.Datastore(context.TODO(), "/metadata")
	if err != nil {
		return err
	}

	j, err := fsjournal.OpenFSJournal(lr, journal.EnvDisabledEvents())
	if err != nil {
		return xerrors.Errorf("failed to open journal: %w", err)
	}

	cache, err := lru.NewARC(lruSize)
	if err != nil {
		return fmt.Errorf("failed to new lru arc cache：%w", err)
	}
	stateBs := &CacheBlockStore{
		Blockstore: bs,
		cache:      cache,
	}

	cst := store.NewChainStore(bs, stateBs, mds, filcns.Weight, j)

	log.Infof("importing chain from %s...", fname)

	rh, err := car.NewCarReader(rd)
	if err != nil {
		return fmt.Errorf("new car reader failed: %w", err)
	}
	ts, err := cst.LoadTipSet(types.NewTipSetKey(rh.Header.Roots...))
	if err != nil {
		return xerrors.Errorf("importing chain failed: %w", err)
	}

	// if err := cst.FlushValidationCache(); err != nil {
	//     return xerrors.Errorf("flushing validation cache failed: %w", err)
	// }

	gb, err := cst.GetTipsetByHeight(ctx, 0, ts, true)
	if err != nil {
		return err
	}

	err = cst.SetGenesis(gb.Blocks()[0])
	if err != nil {
		return err
	}

	// TODO: We need to supply the actual beacon after v14
	stm, err := stmgr.NewStateManager(cst, filcns.NewTipSetExecutor(), vm.Syscalls(ffiwrapper.ProofVerifier), filcns.DefaultUpgradeSchedule(), nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	shutdown := make(chan struct{})

	sigCh := make(chan os.Signal, 2)

	out := make(chan struct{})
	go func() {
		select {
		case sig := <-sigCh:
			log.Warnw("received shutdown", "signal", sig)
		}

		log.Warn("Shutting down...")

		// Call all the handlers, logging on failure and success.
		cancel()
		log.Warn("Graceful shutdown successful")

		<-shutdown
		cst.Close()
		close(out)
	}()

	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	log.Infof("validating imported chain...")
	if err := stm.ValidateChainFromSpecialHeight(ctx, ts, shutdown, startHeight, emptyHeight); err != nil {
		return xerrors.Errorf("chain validation failed: %w", err)
	}

	<-out
	// log.Infof("accepting %s as new head", ts.Cids())
	// if err := cst.ForceHeadSilent(ctx, ts); err != nil {
	//     return err
	// }

	return nil
}

type CacheBlockStore struct {
	blockstore.Blockstore
	cache *lru.ARCCache
}

func (cb *CacheBlockStore) Get(c cid.Cid) (blocks.Block, error) {
	if b, ok := cb.cache.Get(c); ok {
		return b.(blocks.Block), nil
	}
	return cb.Blockstore.Get(c)
}

func (cb *CacheBlockStore) Has(c cid.Cid) (bool, error) {
	if ok := cb.cache.Contains(c); ok {
		return ok, nil
	}
	return cb.Blockstore.Has(c)
}

func (cb *CacheBlockStore) Put(b blocks.Block) error {
	cb.cache.Add(b.Cid(), b)
	return cb.Blockstore.Put(b)
}

func (cb *CacheBlockStore) PutMany(b []blocks.Block) error {
	for i := range b {
		cb.cache.Add(b[i].Cid(), b[i])
	}
	return cb.Blockstore.PutMany(b)
}

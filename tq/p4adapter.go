package tq

import (
	"fmt"
	"github.com/git-lfs/git-lfs/fs"
	"github.com/rubyist/tracerx"
	"time"
)


type p4Adapter struct {
	*adapterBase
	path                string
	args                string
	concurrent          bool
	originalConcurrency int
	standalone          bool
}

type p4AdapterWorkerContext struct {
	workerNum   int
}


func configureP4Adapter(git Env, m *Manifest) {

	var name = "lfs-p4"
    // retrieve other values
	tracerx.Printf("======================== REGISTERING P4ADAPTER")

	// Separate closure for each since we need to capture vars above
	newfunc := func(name string, dir Direction) Adapter {
		return newP4Adapter(
			m.fs,
			name,
			dir,
			"",
			"",
			false,
			false,
		)
	}
	m.RegisterNewAdapterFunc(name, Download, newfunc)
	m.RegisterNewAdapterFunc(name, Upload, newfunc)
}

func newP4Adapter(f *fs.Filesystem, name string, dir Direction, path string, args string, concurrent bool, standalone bool) *p4Adapter {
	tracerx.Printf("======================== CREATING P4ADAPTER")
	p := &p4Adapter{newAdapterBase(f, name, dir, nil), path, args, concurrent, 3, standalone}
	// self implements impl
	p.transferImpl = p
	return p
}

//func (j *p4Adapter) Done(err error) {
//	tracerx.Printf("======== P4ADAPTER DONE")
//}

//func (a *p4Adapter) End() {
//	tracerx.Printf("======== P4ADAPTER END")
//}

func (a *p4Adapter) Begin(cfg AdapterConfig, cb ProgressCallback) error {
	tracerx.Printf("======== P4ADAPTER BEGIN")
	// If config says not to launch multiple processes, downgrade incoming value
	return a.adapterBase.Begin(&p4AdapterConfig{AdapterConfig: cfg}, cb)
}

func (a *p4Adapter) Add(transfers ...*Transfer) <-chan TransferResult {
	for _, t := range transfers {
		tracerx.Printf("======== P4ADAPTER will add job for %q %q", t.Oid, t.Name)
	}
	return a.adapterBase.Add(transfers...)
}

func (a *p4Adapter) ClearTempStorage() error {
	tracerx.Printf("======== P4ADAPTER CLEARTEMPSTORAGE")
	// no action required
	return nil
}

func (a *p4Adapter) WorkerStarting(workerNum int) (interface{}, error) {
	tracerx.Printf("======== P4ADAPTER WORKERSTARTING [%d]",  workerNum)
	if workerNum == 0 { a.authWait.Done() } // FIXME
	ctx := &p4AdapterWorkerContext{workerNum}
	return ctx, nil
}

func (a *p4Adapter) WorkerEnding(workerNum int, ctx interface{}) {
	tracerx.Printf("======== P4ADAPTER WORKERENDING [%d]", workerNum)
}

func (a *p4Adapter) DoTransfer(ctx interface{}, t *Transfer, cb ProgressCallback, authOkFunc func()) error {
	p4Ctx, ok := ctx.(*p4AdapterWorkerContext)
	if !ok {
		return fmt.Errorf("context object for custom transfer %q was of the wrong type", a.name)
	}
	tracerx.Printf("======== DOTRANSFER [%d] %q %q", p4Ctx.workerNum, t.Oid, t.Name)
	time.Sleep(3 * time.Second)
	return nil
}

// worker function, many of these run per adapter
func (a *p4Adapter) worker(workerNum int, ctx interface{}) {

	a.Trace("xfer: adapter %q worker %d starting", a.Name(), workerNum)
	p4Ctx, ok := ctx.(*p4AdapterWorkerContext)
	if !ok {
		fmt.Errorf("context object for custom transfer %q was of the wrong type", a.name)
		a.transferImpl.WorkerEnding(workerNum, ctx)
		a.workerWait.Done()
		return
	}
	tracerx.Printf("$$$$$$ P4ADAPTER [%d] WORKER", p4Ctx.workerNum)

	for job := range a.jobChan {
		t := job.T
		tracerx.Printf("$$$$$$ P4ADAPTER [%d] DOWNLOAD %q %q", p4Ctx.workerNum, t.Oid, t.Name)
	}
	tracerx.Printf("$$$$$$ P4ADAPTER [%d] P4 SYNC NOW", p4Ctx.workerNum)
	for job := range a.jobChan {
		job.Done(nil)
	}
	tracerx.Printf("$$$$$$ P4ADAPTER [%d] DONE", p4Ctx.workerNum)
	a.transferImpl.WorkerEnding(workerNum, ctx)
	a.workerWait.Done()
}

type p4AdapterConfig struct {
	AdapterConfig
}

func (c *p4AdapterConfig) ConcurrentTransfers() int {
	return 2
}

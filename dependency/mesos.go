package dependency

import (
	"fmt"
	"log"

	"github.com/mesosphere/go-mesos-operator/mesos"
	"github.com/pkg/errors"
)

type MesosQuery struct {
	stopCh chan struct{}

	receive chan payload
	addr    string
}

type payload struct {
	snap mesos.FrameworkSnapshot
	err  error
}

func NewMesosQuery(addr string, prot mesos.Protocol) (*MesosQuery, error) {
	stop := make(chan struct{})
	rec := make(chan payload)

	handleUpdate := func(snapshot mesos.Snapshot, err error) {
		p := payload{
			snap: snapshot.(mesos.FrameworkSnapshot),
			err:  err,
		}
		rec <- p
	}

	go mesos.NewFrameworkListener(addr, prot, handleUpdate)

	mq := MesosQuery{
		stopCh:  stop,
		receive: rec,
		addr:    addr,
	}

	return &mq, nil
}

func (d *MesosQuery) Fetch(clients *ClientSet, opts *QueryOptions) (interface{}, *ResponseMetadata, error) {
	log.Printf("[TRACE] %s: FETCH %s", d, d.addr)

	select {
	case <-d.stopCh:
		log.Printf("[TRACE] %s: stopped", d)
		return "", nil, ErrStopped
	case p := <-d.receive:
		log.Printf("[TRACE] %s: reported change", d)

		if p.err != nil {
			return "", nil, errors.Wrap(p.err, d.String())
		}

		return respWithMetadata(p.snap)
	}
}

// CanShare returns a boolean if this dependency is shareable.
func (d *MesosQuery) CanShare() bool {
	return false
}

// Stop halts the dependency's fetch function.
func (d *MesosQuery) Stop() {
	close(d.stopCh)
}

// String returns the human-friendly version of this dependency.
func (d *MesosQuery) String() string {
	return fmt.Sprintf("%v", d)
}

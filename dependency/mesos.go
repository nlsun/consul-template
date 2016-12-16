package dependency

import (
	"fmt"
	"log"
	"time"

	//"github.com/mesosphere/go-mesos-operator/mesos"
	"github.com/pkg/errors"
)

const MesosQuerySleepTime time.Duration = 2 * time.Second

type MesosQuery struct {
	stopCh chan struct{}

	id int
}

func NewMesosQuery() *MesosQuery {
	stop := make(chan struct{})

	mq := MesosQuery{
		stopCh: stop,
	}

	return &mq
}

func (d *MesosQuery) Fetch(clients *ClientSet, opts *QueryOptions) (interface{}, *ResponseMetadata, error) {
	log.Printf("[TRACE] %s: FETCH %s", d, d.id)

	select {
	case <-d.stopCh:
		log.Printf("[TRACE] %s: stopped", d)
		return "", nil, ErrStopped
	case p := <-d.watch(d.id, clients):
		log.Printf("[TRACE] %s: reported change", d)

		d.id = p.id

		if p.Err != nil {
			return "", nil, errors.Wrap(p.Err, d.String())
		}

		return respWithMetadata(p.Snap)
	}
}

func (d *MesosQuery) watch(lastId int, clients *ClientSet) <-chan MesosPayload {
	watchCh := make(chan MesosPayload, 1)

	go func(li int, c *ClientSet, wCh chan MesosPayload) {
		for {
			payload := c.mesos.read()
			if payload.id != li {
				select {
				case <-d.stopCh:
					return
				case wCh <- payload:
				}
			}
			time.Sleep(MesosQuerySleepTime)
		}
	}(lastId, clients, watchCh)
	return watchCh
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
	return fmt.Sprintf("mesos %d", d.id)
}

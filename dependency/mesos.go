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

	id int // this is used to track changes in the query

	uuid string // this labels each query
}

func NewMesosQuery(uuid string) *MesosQuery {
	stop := make(chan struct{})

	mq := MesosQuery{
		stopCh: stop,
		uuid:   uuid,
	}
	log.Printf("[DEBUG] new mesosquery-%s", mq.uuid)

	return &mq
}

func (d *MesosQuery) Fetch(clients *ClientSet, opts *QueryOptions) (interface{}, *ResponseMetadata, error) {
	log.Printf("[DEBUG] mesosquery-%s: FETCH %d", d.uuid, d.id)
	log.Printf("[TRACE] mesosquery-%s: FETCH %d", d.uuid, d.id)

	select {
	case <-d.stopCh:
		log.Printf("[DEBUG] mesosquery-%s: stopped", d.uuid)
		log.Printf("[TRACE] mesosquery-%s: stopped", d.uuid)
		return "", nil, ErrStopped
	case p := <-d.watch(d.id, clients):
		log.Printf("[DEBUG] mesosquery-%s: reported change", d.uuid)
		log.Printf("[TRACE] mesosquery-%s: reported change", d.uuid)

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
			//log.Printf("[DEBUG] mesosquery-%s: checking payload <%d:%d>", d.uuid, payload.id, li)
			if payload.id != li {
				select {
				case <-d.stopCh:
					return
				case wCh <- payload:
					log.Printf("[DEBUG] mesosquery-%s: sent payload", d.uuid)
				}
			}
			time.Sleep(MesosQuerySleepTime)
		}
	}(lastId, clients, watchCh)
	log.Printf("[DEBUG] mesosquery-%s: started watch", d.uuid)
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
	return fmt.Sprintf("mesosquery-%s", d.uuid)
}

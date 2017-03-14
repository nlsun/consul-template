package dependency

import (
	"fmt"
	"log"
	"time"

	//mesos_v1 "github.com/mesosphere/go-mesos-operator/include/mesos/v1"
	"github.com/mesosphere/go-mesos-operator/mesos"
	"github.com/pkg/errors"
)

// XXX It's been a while since I wrote this code, so I don't remember why I
//   didn't go with a channel. I remember that the reason for polling is
//   because I copied the implementation in file.go, but I feel like
//   we could do a channel in here.

type MesosTask struct {
	Task  *mesos.Task
	Agent *mesos.Agent
}

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
	log.Printf("[DEBUG] (mesos) new mesosquery-%s", mq.uuid)

	return &mq
}

func (d *MesosQuery) Fetch(clients *ClientSet, opts *QueryOptions) (interface{}, *ResponseMetadata, error) {
	log.Printf("[DEBUG] (mesos) mesosquery-%s: FETCH %d", d.uuid, d.id)
	log.Printf("[TRACE] (mesos) mesosquery-%s: FETCH %d", d.uuid, d.id)

	select {
	case <-d.stopCh:
		log.Printf("[DEBUG] (mesos) mesosquery-%s: stopped", d.uuid)
		log.Printf("[TRACE] (mesos) mesosquery-%s: stopped", d.uuid)
		return "", nil, ErrStopped
	case p := <-d.watch(d.id, clients):
		log.Printf("[DEBUG] (mesos) mesosquery-%s: reported change", d.uuid)
		log.Printf("[TRACE] (mesos) mesosquery-%s: reported change", d.uuid)

		d.id = p.id

		if p.Err != nil {
			return "", nil, errors.Wrap(p.Err, d.String())
		}

		// Return entire payload instead of just the snapshot because
		// consul-template does some strange equality check? Possibly
		// because the snapshot itself is a pointer, if that doesn't
		// change, it seems they don't treat this as an update? Since
		// the id in here changes every time, perhaps it's enough to make
		// the equality check think that it's unequal?
		return respWithMetadata(p)
	}
}

func (d *MesosQuery) watch(lastId int, clients *ClientSet) <-chan MesosPayload {
	// Buffer so that the goroutine may immediately exit due to writing to
	// buffer, instead of having to wait for the reader to also read it before
	// exiting.
	watchCh := make(chan MesosPayload, 1)

	go func(li int, c *ClientSet, wCh chan MesosPayload) {
		defer log.Printf("[DEBUG] (mesos) mesosquery-%s: watch terminated", d.uuid)
		for {
			payload := c.mesos.read()
			//log.Printf("[DEBUG] (mesos) mesosquery-%s: checking payload <%d:%d>", d.uuid, payload.id, li)
			if payload.id != li {
				select {
				case <-d.stopCh:
					return
				case wCh <- payload:
					log.Printf("[DEBUG] (mesos) mesosquery-%s: sent payload", d.uuid)
					return
				}
			}
			time.Sleep(MesosQuerySleepTime)
		}
	}(lastId, clients, watchCh)
	log.Printf("[DEBUG] (mesos) mesosquery-%s: started watch", d.uuid)
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
	// This function is the one that's used to track the task! if this changes
	// then this will be killed!
	return fmt.Sprintf("mesosquery-%s", d.uuid)
}

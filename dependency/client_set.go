package dependency

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-cleanhttp"
	rootcerts "github.com/hashicorp/go-rootcerts"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/mesosphere/go-mesos-operator/mesos"
)

// XXX Currently we have a set of timers associated with tasks in order
//   to keep track of which tasks to keep alive. In addition, we keep
//   around any frameworks/agents that are associated with these tasks.
//   This approach basically keeps around a fake view of cluster state.
//   An alternative approach is to maintain 2 completely separate state,
//   the current one, and one that is completely made up of timers. This
//   approach has less logic when it comes to maintaining state, and instead
//   pushes the complexity to template render time, which is when we
//   reconcile the 2 states.

// XXX Another thing to consider is that we might be able to wait for
//   an AGENT_REMOVED message instead of using a timeout. The issue with
//   that is that we'd need some signal that we recieve that message
//   from go-mesos-operater, which currently abstracts away the individual
//   messages that are sent. Possibly hook up a channel that alerts on
//   agent removal events? Or even simpler, just have a field in the
//   state that is passed from go-mesos-operator include a special
//   slice that lists agents that have been removed in that state change.
//
// Mesos is configured to wait for 10 minutes for an agent to reconnect. Just
// to be safe, we will wait longer than that.
//
// Time is in seconds
//// XXX FOR DEBUG PURPOSES
//const failoverTimeout int64 = 30
const failoverTimeout int64 = 60 * 11

const timerCheckInterval time.Duration = time.Second * 2
const listenerCrashWait time.Duration = time.Second

// ClientSet is a collection of clients that dependencies use to communicate
// with remote services like Consul or Vault.
type ClientSet struct {
	sync.RWMutex

	vault  *vaultClient
	consul *consulClient
	mesos  *mesosClient
}

type mesosClient struct {
	id      int
	snap    MesosPayload
	snapMut sync.RWMutex

	// Map from mesos ID to Unix time
	//
	// This map also depends on snapMut
	taskTimers map[string]int64

	// These maps track the references to frameworks/agents that are
	// carried over as dependencies of tasks. These should never
	// point to "real" values, only "carried" ones.
	//
	// These are closely associated with taskTimers, so they should roughly
	// appear wherever those do
	//
	// These maps also depend on snapMut
	carriedFwRef map[string]int
	carriedAgRef map[string]int

	// This is set when a failover is detected. It is cleared on the
	// next update.
	//
	// Depends on snapMut
	leaderFailover bool
}

type MesosPayload struct {
	Snap mesos.FrameworkSnapshot
	Err  error

	id int
}

func (c *mesosClient) read() MesosPayload {
	// XXX There might be a race condition here, where c.snap can be modified
	//   between the time that the lock is unlocked, and state is returned.
	//   We should try instead to guarantee that snapshots aren't modified
	//   once they're set. As such, there might not be a point to this lock
	//   here at all.
	c.snapMut.RLock()
	sclone, err := mesos.CloneSnapshot(c.snap.Snap)
	state := MesosPayload{
		Snap: sclone,
		Err:  err,
		id:   c.snap.id,
	}
	c.snapMut.RUnlock()
	return state
}

func (c *mesosClient) update(pload MesosPayload) {
	c.snapMut.Lock()
	log.Printf("[DEBUG] (clients) mesos update: running update")
	c.id += 1
	c.snap.id = c.id

	// We check timers before pick out tasks to carry over. Otherwise,
	// expired tasks will get carried over before they can be deleted.
	c.checkTimers()

	var carriedTasks []string

	// Pick out the tasks to carry over
	for tid, oldTask := range c.snap.Snap.Tasks {
		fid := oldTask.GetFrameworkId().GetValue()
		agid := oldTask.GetAgentId().GetValue()
		if _, ok := pload.Snap.Tasks[tid]; ok {
			// The task already exists in the new snapshot
			log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos update task exists in new snapshot: %s", tid))
			c.maybeClearTimer(tid, fid, agid)
			continue
		}
		if _, ok := pload.Snap.Agents[agid]; ok {
			// The task does not exist in the new snapshot yet the
			// old agent has reconnected, so we delete the old task.
			log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos update agent reconnected without task: %s", tid))
			c.maybeClearTimer(tid, fid, agid)
			continue
		}
		carriedTasks = append(carriedTasks, tid)
	}

	// Reset carry maps, so we don't have to keep track of which ones have
	// been replaced by real values.
	c.carriedFwRef = make(map[string]int)
	c.carriedAgRef = make(map[string]int)

	// Update carry maps
	//
	// The reason this is a separate step is because we want to count the
	// number of references to each carried value, which we have to do
	// before modifying the snapshot.
	for _, tid := range carriedTasks {
		oldTask := c.snap.Snap.Tasks[tid]

		// If we carry the task, we also carry over the associated framework
		fid := oldTask.GetFrameworkId().GetValue()
		if _, ok := pload.Snap.Frameworks[fid]; !ok {
			c.fwRefUp(fid)
		}

		// If we carry the task, we also carry over the associated agent
		//
		// It's not possible for the task's agent to already exist, otherwise
		// we wouldn't be carrying over the task in the first place. So
		// we just blindly overwrite it.
		agid := oldTask.GetAgentId().GetValue()
		c.agRefUp(agid)
	}

	// Modify the new snapshot with carried over values
	//
	// The reason this is a separate step is because we don't want to modify
	// the new snapshot while the new snapshot is used to compute which
	// values are to be carried over.
	for _, tid := range carriedTasks {
		log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos update carrying task: %s", tid))
		pload.Snap.Tasks[tid] = c.snap.Snap.Tasks[tid]
	}
	for fid, _ := range c.carriedFwRef {
		log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos update carrying framework %s", fid))
		pload.Snap.Frameworks[fid] = c.snap.Snap.Frameworks[fid]
	}
	for agid, _ := range c.carriedAgRef {
		log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos update carrying agent %s", agid))
		pload.Snap.Agents[agid] = c.snap.Snap.Agents[agid]
	}

	// Replace the old snapshot with new one
	c.snap.Snap.Frameworks = pload.Snap.Frameworks
	c.snap.Snap.Agents = pload.Snap.Agents
	c.snap.Snap.Tasks = pload.Snap.Tasks

	if c.leaderFailover {
		c.leaderFailover = false

		now := time.Now().Unix()
		newTimeout := now + failoverTimeout
		for _, tid := range carriedTasks {
			c.taskTimers[tid] = newTimeout
		}
	}
	c.snapMut.Unlock()
}

func (c *mesosClient) fwRefUp(fid string) {
	// Assumes that it's in a locked context.
	// Depends on snapMut.

	log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos fw ref up: %s", fid))
	refUpHelper(c.carriedFwRef, fid)
}

func (c *mesosClient) agRefUp(agid string) {
	// Assumes that it's in a locked context.
	// Depends on snapMut.

	log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos ag ref up: %s", agid))
	refUpHelper(c.carriedAgRef, agid)
}

func refUpHelper(refMap map[string]int, id string) {
	if count, ok := refMap[id]; !ok {
		log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos ref up: creating %s", id))
		refMap[id] = 1
	} else {
		refMap[id] = count + 1
	}
}

func (c *mesosClient) fwRefDown(fid string) {
	// Assumes that it's in a locked context.
	// Depends on snapMut.

	log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos fw ref down: %s", fid))
	refDownHelper(c.carriedFwRef, fid)
}

func (c *mesosClient) agRefDown(agid string) {
	// Assumes that it's in a locked context.
	// Depends on snapMut.

	log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos ag ref down: %s", agid))
	refDownHelper(c.carriedAgRef, agid)
}

func refDownHelper(refMap map[string]int, id string) {
	// XXX Should write a test that ensures that this never drops below 1

	//// XXX FUNCTION FOR DEBUG ONLY
	//defer func(rmap map[string]int) {
	//	if count, ok := rmap[id]; !ok {
	//		return
	//	} else if count <= 0 {
	//		log.Printf(fmt.Sprintf("[ERROR] (clients) mesos ref TOO LOW: %s %d", id, count))
	//	}
	//}(refMap)

	count := refMap[id]
	if count == 1 {
		log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos ref down: deleting %s", id))
		delete(refMap, id)
		return
	}
	refMap[id] = count - 1
}

// consulClient is a wrapper around a real Consul API client.
type consulClient struct {
	client     *consulapi.Client
	httpClient *http.Client
}

// vaultClient is a wrapper around a real Vault API client.
type vaultClient struct {
	client     *vaultapi.Client
	httpClient *http.Client
}

// CreateConsulClientInput is used as input to the CreateConsulClient function.
type CreateConsulClientInput struct {
	Address      string
	Token        string
	AuthEnabled  bool
	AuthUsername string
	AuthPassword string
	SSLEnabled   bool
	SSLVerify    bool
	SSLCert      string
	SSLKey       string
	SSLCACert    string
	SSLCAPath    string
	ServerName   string
}

// CreateVaultClientInput is used as input to the CreateVaultClient function.
type CreateVaultClientInput struct {
	Address     string
	Token       string
	UnwrapToken bool
	SSLEnabled  bool
	SSLVerify   bool
	SSLCert     string
	SSLKey      string
	SSLCACert   string
	SSLCAPath   string
	ServerName  string
}

// NewClientSet creates a new client set that is ready to accept clients.
func NewClientSet() *ClientSet {
	return &ClientSet{}
}

func (c *ClientSet) CreateMesosClient(mesosInput string) {
	if mesosInput == "" {
		return
	}

	c.mesos = &mesosClient{
		// Initialize this so that the continuous checker will start at a
		// different value.
		id:         1,
		taskTimers: make(map[string]int64),
	}

	handleUpdate := func(ctx context.Context, snapshot mesos.FrameworkSnapshot, err error) {
		// Context is currently unused.

		p := MesosPayload{
			Snap: snapshot,
			Err:  err,
		}
		c.mesos.update(p)
	}

	split := strings.Split(mesosInput, "..")
	addr := split[0]
	var prot mesos.Protocol
	switch split[1] {
	case "https":
		prot = mesos.HTTPS
	case "http":
		fallthrough
	default:
		prot = mesos.HTTP
	}

	go func(mClient *mesosClient) {
		// XXX As future work, instead of assuming every crash here is leader
		//   failure, have a more fine grained error scheme so we can decide
		//   when to crash and when to just carry on.

		// XXX The leader detection here is sketchy, we are just passing in an
		//   address, we should at the very least force a re-resolution of DNS
		//   every time we try to start up the subscribe listener.
		// XXX we should have a "pass in address mode" and "leader detection"
		//   mode. leader detection scheme goes:
		//   - find the leader (zookeeper) and set a watch or something
		//   - connect to the "leader" with subscribe, with with a context so
		//     it can cancel!
		//   - whenever the leader changes, cancel the subscribe, and restart it

		// XXX investigate using a channel instead of the integer for notifications,
		//   a channel will make this stuff easier because the ordering of
		//   events (updates vs timeout mode) is absolute. If we do make this
		//   a channel, must remember to add in some kind of uuid/randomness
		//   so that the consul-template equality check bug thing registers
		//   an update.

		for {
			ctx := context.Background()
			if err := mesos.NewFrameworkListener(ctx, addr, prot, handleUpdate); err != nil {
				msg := fmt.Sprintf("[INFO] (clients) mesos listener crashed, assuming mesos leader failover: %s", err)
				log.Printf(msg)
			}
			mClient.snapMut.Lock()
			mClient.leaderFailover = true
			mClient.snapMut.Unlock()
			time.Sleep(listenerCrashWait)
		}
	}(c.mesos)

	go func(mClient *mesosClient) {
		for {
			mClient.snapMut.Lock()
			mClient.checkTimers()
			mClient.snapMut.Unlock()
			time.Sleep(timerCheckInterval)
		}
	}(c.mesos)
}

// Only clears timer if timer exists
func (c *mesosClient) maybeClearTimer(taskId, frameworkId, agentId string) {
	if _, ok := c.taskTimers[taskId]; ok {
		c.clearTimer(taskId, frameworkId, agentId)
	}
}

func (c *mesosClient) clearTimer(taskId, frameworkId, agentId string) {
	// Assumes that it's in a locked context.
	// Depends on snapMut.

	log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos clearing task timer: %s", taskId))
	c.fwRefDown(frameworkId)
	c.agRefDown(agentId)
	delete(c.taskTimers, taskId)
}

// Scan through the active timers and clear the timer and delete associated
// task/framework/agent if it timed out. Triggers an update if
// something changes.
func (c *mesosClient) checkTimers() {
	// Assumes that it's in a locked context.
	// Depends on snapMut.

	now := time.Now().Unix()
	changed := false
	for tid, endtime := range c.taskTimers {
		if endtime > now {
			continue
		}
		fid := c.snap.Snap.Tasks[tid].GetFrameworkId().GetValue()
		agid := c.snap.Snap.Tasks[tid].GetAgentId().GetValue()
		c.clearTimer(tid, fid, agid)

		log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos timer expired deleting task: %s", tid))
		delete(c.snap.Snap.Tasks, tid)
		if _, ok := c.carriedFwRef[fid]; !ok {
			log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos timer deleting unreferenced framework: %s", fid))
			delete(c.snap.Snap.Frameworks, fid)
		}
		if _, ok := c.carriedAgRef[agid]; !ok {
			log.Printf(fmt.Sprintf("[DEBUG] (clients) mesos timer deleting unreferenced agent: %s", agid))
			delete(c.snap.Snap.Agents, agid)
		}
		changed = true
	}

	if changed {
		log.Printf("[DEBUG] (clients) mesos check timer triggered change")
		// XXX for some reason this didn't trigger a change in the
		//   dependency/mesos or template/funcs.
		//
		//   - Update: This might have been fixed by adding more data to
		//     what is passed to the consul-template functions,
		//     may have some strange equality going on that required passing
		//     in the entire payload (which includes this changing id here)
		//     rather than just the snapshot to register a change.
		//
		//   It gets up to this point:
		//   2017/03/14 18:36:14.819842 [DEBUG] (clients) mesos check timer triggered change
		//   2017/03/14 18:36:15.846146 [DEBUG] (mesos) mesosquery-mesosTaskFrameworkFilter: sent payload
		//   2017/03/14 18:36:15.846185 [DEBUG] (mesos) mesosquery-mesosTaskFrameworkFilter: watch terminated
		//   2017/03/14 18:36:15.846198 [DEBUG] (mesos) mesosquery-mesosTaskFrameworkFilter: reported change
		//   2017/03/14 18:36:15.846257 [DEBUG] (mesos) mesosquery-mesosTaskFrameworkFilter: FETCH 4
		//   2017/03/14 18:36:15.846272 [DEBUG] (mesos) mesosquery-mesosTaskFrameworkFilter: started watch

		c.id += 1
		c.snap.id = c.id
	}
}

// CreateConsulClient creates a new Consul API client from the given input.
func (c *ClientSet) CreateConsulClient(i *CreateConsulClientInput) error {
	consulConfig := consulapi.DefaultConfig()

	if i.Address != "" {
		consulConfig.Address = i.Address
	}

	if i.Token != "" {
		consulConfig.Token = i.Token
	}

	if i.AuthEnabled {
		consulConfig.HttpAuth = &consulapi.HttpBasicAuth{
			Username: i.AuthUsername,
			Password: i.AuthPassword,
		}
	}

	// This transport will attempt to keep connections open to the Consul server.
	transport := cleanhttp.DefaultPooledTransport()

	// Configure SSL
	if i.SSLEnabled {
		consulConfig.Scheme = "https"

		var tlsConfig tls.Config

		// Custom certificate or certificate and key
		if i.SSLCert != "" && i.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(i.SSLCert, i.SSLKey)
			if err != nil {
				return fmt.Errorf("client set: consul: %s", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		} else if i.SSLCert != "" {
			cert, err := tls.LoadX509KeyPair(i.SSLCert, i.SSLCert)
			if err != nil {
				return fmt.Errorf("client set: consul: %s", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Custom CA certificate
		if i.SSLCACert != "" || i.SSLCAPath != "" {
			rootConfig := &rootcerts.Config{
				CAFile: i.SSLCACert,
				CAPath: i.SSLCAPath,
			}
			if err := rootcerts.ConfigureTLS(&tlsConfig, rootConfig); err != nil {
				return fmt.Errorf("client set: consul configuring TLS failed: %s", err)
			}
		}

		// Construct all the certificates now
		tlsConfig.BuildNameToCertificate()

		// SSL verification
		if i.ServerName != "" {
			tlsConfig.ServerName = i.ServerName
			tlsConfig.InsecureSkipVerify = false
		}
		if !i.SSLVerify {
			log.Printf("[WARN] (clients) disabling consul SSL verification")
			tlsConfig.InsecureSkipVerify = true
		}

		// Save the TLS config on our transport
		transport.TLSClientConfig = &tlsConfig
	}

	// Setup the new transport
	consulConfig.HttpClient.Transport = transport

	// Create the API client
	client, err := consulapi.NewClient(consulConfig)
	if err != nil {
		return fmt.Errorf("client set: consul: %s", err)
	}

	// Save the data on ourselves
	c.consul = &consulClient{
		client:     client,
		httpClient: consulConfig.HttpClient,
	}

	return nil
}

func (c *ClientSet) CreateVaultClient(i *CreateVaultClientInput) error {
	vaultConfig := vaultapi.DefaultConfig()

	if i.Address != "" {
		vaultConfig.Address = i.Address
	}

	// This transport will attempt to keep connections open to the Vault server.
	transport := cleanhttp.DefaultPooledTransport()

	// Configure SSL
	if i.SSLEnabled {
		var tlsConfig tls.Config

		// Custom certificate or certificate and key
		if i.SSLCert != "" && i.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(i.SSLCert, i.SSLKey)
			if err != nil {
				return fmt.Errorf("client set: vault: %s", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		} else if i.SSLCert != "" {
			cert, err := tls.LoadX509KeyPair(i.SSLCert, i.SSLCert)
			if err != nil {
				return fmt.Errorf("client set: vault: %s", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Custom CA certificate
		if i.SSLCACert != "" || i.SSLCAPath != "" {
			rootConfig := &rootcerts.Config{
				CAFile: i.SSLCACert,
				CAPath: i.SSLCAPath,
			}
			if err := rootcerts.ConfigureTLS(&tlsConfig, rootConfig); err != nil {
				return fmt.Errorf("client set: vault configuring TLS failed: %s", err)
			}
		}

		// Construct all the certificates now
		tlsConfig.BuildNameToCertificate()

		// SSL verification
		if i.ServerName != "" {
			tlsConfig.ServerName = i.ServerName
			tlsConfig.InsecureSkipVerify = false
		}
		if !i.SSLVerify {
			log.Printf("[WARN] (clients) disabling vault SSL verification")
			tlsConfig.InsecureSkipVerify = true
		}

		// Save the TLS config on our transport
		transport.TLSClientConfig = &tlsConfig
	}

	// Setup the new transport
	vaultConfig.HttpClient.Transport = transport

	// Create the client
	client, err := vaultapi.NewClient(vaultConfig)
	if err != nil {
		return fmt.Errorf("client set: vault: %s", err)
	}

	// Set the token if given
	if i.Token != "" {
		client.SetToken(i.Token)
	}

	// Check if we are unwrapping
	if i.UnwrapToken {
		secret, err := client.Logical().Unwrap(i.Token)
		if err != nil {
			return fmt.Errorf("client set: vault unwrap: %s", err)
		}

		if secret == nil {
			return fmt.Errorf("client set: vault unwrap: no secret")
		}

		if secret.Auth == nil {
			return fmt.Errorf("client set: vault unwrap: no secret auth")
		}

		if secret.Auth.ClientToken == "" {
			return fmt.Errorf("client set: vault unwrap: no token returned")
		}

		client.SetToken(secret.Auth.ClientToken)
	}

	// Save the data on ourselves
	c.vault = &vaultClient{
		client:     client,
		httpClient: vaultConfig.HttpClient,
	}

	return nil
}

// Consul returns the Consul client for this set.
func (c *ClientSet) Consul() *consulapi.Client {
	c.RLock()
	defer c.RUnlock()
	return c.consul.client
}

// Vault returns the Consul client for this set.
func (c *ClientSet) Vault() *vaultapi.Client {
	c.RLock()
	defer c.RUnlock()
	return c.vault.client
}

// Stop closes all idle connections for any attached clients.
func (c *ClientSet) Stop() {
	c.Lock()
	defer c.Unlock()

	if c.consul != nil {
		c.consul.httpClient.Transport.(*http.Transport).CloseIdleConnections()
	}

	if c.vault != nil {
		c.vault.httpClient.Transport.(*http.Transport).CloseIdleConnections()
	}
}

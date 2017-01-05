package dependency

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-cleanhttp"
	rootcerts "github.com/hashicorp/go-rootcerts"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/mesosphere/go-mesos-operator/mesos"
)

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
}

type MesosPayload struct {
	Snap mesos.FrameworkSnapshot
	Err  error

	id int
}

func (c *mesosClient) read() MesosPayload {
	c.snapMut.RLock()
	state := c.snap
	c.snapMut.RUnlock()
	return state
}

func (c *mesosClient) update(p MesosPayload) {
	c.id += 1
	p.id = c.id

	c.snapMut.Lock()
	c.snap = p
	c.snapMut.Unlock()
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
		id: 1,
	}

	handleUpdate := func(snapshot mesos.Snapshot, err error) {
		p := MesosPayload{
			Snap: snapshot.(mesos.FrameworkSnapshot),
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

	go mesos.NewFrameworkListener(addr, prot, handleUpdate)
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

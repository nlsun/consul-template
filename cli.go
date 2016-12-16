package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/hashicorp/consul-template/config"
	"github.com/hashicorp/consul-template/logging"
	"github.com/hashicorp/consul-template/manager"
	"github.com/hashicorp/consul-template/signals"
)

// Exit codes are int values that represent an exit code for a particular error.
// Sub-systems may check this unique error to determine the cause of an error
// without parsing the output or help text.
//
// Errors start at 10
const (
	ExitCodeOK int = 0

	ExitCodeError = 10 + iota
	ExitCodeInterrupt
	ExitCodeParseFlagsError
	ExitCodeRunnerError
	ExitCodeConfigError
)

// CLI is the main entry point.
type CLI struct {
	sync.Mutex

	// outSteam and errStream are the standard out and standard error streams to
	// write messages from the CLI.
	outStream, errStream io.Writer

	// signalCh is the channel where the cli receives signals.
	signalCh chan os.Signal

	// stopCh is an internal channel used to trigger a shutdown of the CLI.
	stopCh  chan struct{}
	stopped bool
}

// NewCLI creates a new CLI object with the given stdout and stderr streams.
func NewCLI(out, err io.Writer) *CLI {
	return &CLI{
		outStream: out,
		errStream: err,
		signalCh:  make(chan os.Signal, 1),
		stopCh:    make(chan struct{}),
	}
}

// Run accepts a slice of arguments and returns an int representing the exit
// status from the command.
func (cli *CLI) Run(args []string) int {
	// Parse the flags
	config, once, dry, version, err := cli.ParseFlags(args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return cli.handleError(err, ExitCodeParseFlagsError)
	}

	// Save original config (defaults + parsed flags) for handling reloads
	baseConfig := config.Copy()

	// Setup the config and logging
	config, err = cli.setup(config)
	if err != nil {
		return cli.handleError(err, ExitCodeConfigError)
	}

	// Print version information for debugging
	log.Printf("[INFO] %s", humanVersion)

	// If the version was requested, return an "error" containing the version
	// information. This might sound weird, but most *nix applications actually
	// print their version on stderr anyway.
	if version {
		log.Printf("[DEBUG] (cli) version flag was given, exiting now")
		fmt.Fprintf(cli.errStream, "%s\n", humanVersion)
		return ExitCodeOK
	}

	// Initial runner
	runner, err := manager.NewRunner(config, dry, once)
	if err != nil {
		return cli.handleError(err, ExitCodeRunnerError)
	}
	go runner.Start()

	// Listen for signals
	signal.Notify(cli.signalCh)

	for {
		select {
		case err := <-runner.ErrCh:
			// Check if the runner's error returned a specific exit status, and return
			// that value. If no value was given, return a generic exit status.
			code := ExitCodeRunnerError
			if typed, ok := err.(manager.ErrExitable); ok {
				code = typed.ExitStatus()
			}
			return cli.handleError(err, code)
		case <-runner.DoneCh:
			return ExitCodeOK
		case s := <-cli.signalCh:
			log.Printf("[DEBUG] (cli) receiving signal %q", s)

			switch s {
			case *config.ReloadSignal:
				fmt.Fprintf(cli.errStream, "Reloading configuration...\n")
				runner.Stop()

				// Load the new configuration from disk
				config, err = cli.setup(baseConfig)
				if err != nil {
					return cli.handleError(err, ExitCodeConfigError)
				}

				runner, err = manager.NewRunner(config, dry, once)
				if err != nil {
					return cli.handleError(err, ExitCodeRunnerError)
				}
				go runner.Start()
			case *config.KillSignal:
				fmt.Fprintf(cli.errStream, "Cleaning up...\n")
				runner.Stop()
				return ExitCodeInterrupt
			case signals.SignalLookup["SIGCHLD"]:
				// The SIGCHLD signal is sent to the parent of a child process when it
				// exits, is interrupted, or resumes after being interrupted. We ignore
				// this signal because the child process is monitored on its own.
				//
				// Also, the reason we do a lookup instead of a direct syscall.SIGCHLD
				// is because that isn't defined on Windows.
			default:
				// Propogate the signal to the child process
				runner.Signal(s)
			}
		case <-cli.stopCh:
			return ExitCodeOK
		}
	}
}

// stop is used internally to shutdown a running CLI
func (cli *CLI) stop() {
	cli.Lock()
	defer cli.Unlock()

	if cli.stopped {
		return
	}

	close(cli.stopCh)
	cli.stopped = true
}

// ParseFlags is a helper function for parsing command line flags using Go's
// Flag library. This is extracted into a helper to keep the main function
// small, but it also makes writing tests for parsing command line arguments
// much easier and cleaner.
func (cli *CLI) ParseFlags(args []string) (*config.Config, bool, bool, bool, error) {
	var dry, once, version bool

	c := config.DefaultConfig()

	// configPaths stores the list of configuration paths on disk
	configPaths := make([]string, 0, 6)

	// Parse the flags and options
	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() { fmt.Fprintf(cli.errStream, usage, Name) }

	flags.Var((funcVar)(func(s string) error {
		a, err := config.ParseAuthConfig(s)
		if err != nil {
			return err
		}
		c.Auth = a
		return nil
	}), "auth", "")

	flags.Var((funcVar)(func(s string) error {
		configPaths = append(configPaths, s)
		return nil
	}), "config", "")

	flags.Var((funcVar)(func(s string) error {
		c.Mesos = config.String(s)
		return nil
	}), "mesos", "")

	flags.Var((funcVar)(func(s string) error {
		c.Consul = config.String(s)
		return nil
	}), "consul", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.Dedup.Enabled = config.Bool(b)
		return nil
	}), "dedup", "")

	flags.BoolVar(&dry, "dry", false, "")

	flags.Var((funcVar)(func(s string) error {
		c.Exec.Enabled = config.Bool(true)
		c.Exec.Command = config.String(s)
		return nil
	}), "exec", "")

	flags.Var((funcVar)(func(s string) error {
		sig, err := signals.Parse(s)
		if err != nil {
			return err
		}
		c.Exec.KillSignal = config.Signal(sig)
		return nil
	}), "exec-kill-signal", "")

	flags.Var((funcDurationVar)(func(d time.Duration) error {
		c.Exec.KillTimeout = config.TimeDuration(d)
		return nil
	}), "exec-kill-timeout", "")

	flags.Var((funcVar)(func(s string) error {
		sig, err := signals.Parse(s)
		if err != nil {
			return err
		}
		c.Exec.ReloadSignal = config.Signal(sig)
		return nil
	}), "exec-reload-signal", "")

	flags.Var((funcDurationVar)(func(d time.Duration) error {
		c.Exec.Splay = config.TimeDuration(d)
		return nil
	}), "exec-splay", "")

	flags.Var((funcVar)(func(s string) error {
		sig, err := signals.Parse(s)
		if err != nil {
			return err
		}
		c.KillSignal = config.Signal(sig)
		return nil
	}), "kill-signal", "")

	flags.Var((funcVar)(func(s string) error {
		c.LogLevel = config.String(s)
		return nil
	}), "log-level", "")

	flags.Var((funcDurationVar)(func(d time.Duration) error {
		c.MaxStale = config.TimeDuration(d)
		return nil
	}), "max-stale", "")

	flags.BoolVar(&once, "once", false, "")
	flags.Var((funcVar)(func(s string) error {
		c.PidFile = config.String(s)
		return nil
	}), "pid-file", "")

	flags.Var((funcVar)(func(s string) error {
		sig, err := signals.Parse(s)
		if err != nil {
			return err
		}
		c.ReloadSignal = config.Signal(sig)
		return nil
	}), "reload-signal", "")

	flags.Var((funcDurationVar)(func(d time.Duration) error {
		c.Retry = config.TimeDuration(d)
		return nil
	}), "retry", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.SSL.Enabled = config.Bool(b)
		return nil
	}), "ssl", "")

	flags.Var((funcVar)(func(s string) error {
		c.SSL.CaCert = config.String(s)
		return nil
	}), "ssl-ca-cert", "")

	flags.Var((funcVar)(func(s string) error {
		c.SSL.CaPath = config.String(s)
		return nil
	}), "ssl-ca-path", "")

	flags.Var((funcVar)(func(s string) error {
		c.SSL.Cert = config.String(s)
		return nil
	}), "ssl-cert", "")

	flags.Var((funcVar)(func(s string) error {
		c.SSL.Key = config.String(s)
		return nil
	}), "ssl-key", "")

	flags.Var((funcVar)(func(s string) error {
		c.SSL.ServerName = config.String(s)
		return nil
	}), "ssl-server-name", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.SSL.Verify = config.Bool(b)
		return nil
	}), "ssl-verify", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.Syslog.Enabled = config.Bool(b)
		return nil
	}), "syslog", "")

	flags.Var((funcVar)(func(s string) error {
		c.Syslog.Facility = config.String(s)
		return nil
	}), "syslog-facility", "")

	flags.Var((funcVar)(func(s string) error {
		t, err := config.ParseTemplateConfig(s)
		if err != nil {
			return err
		}
		*c.Templates = append(*c.Templates, t)
		return nil
	}), "template", "")

	flags.Var((funcVar)(func(s string) error {
		c.Token = config.String(s)
		return nil
	}), "token", "")

	flags.Var((funcVar)(func(s string) error {
		c.Vault.Address = config.String(s)
		return nil
	}), "vault-addr", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.Vault.RenewToken = config.Bool(b)
		return nil
	}), "vault-renew-token", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.Vault.SSL.Enabled = config.Bool(b)
		return nil
	}), "vault-ssl", "")

	flags.Var((funcVar)(func(s string) error {
		c.Vault.SSL.CaCert = config.String(s)
		return nil
	}), "vault-ssl-ca-cert", "")

	flags.Var((funcVar)(func(s string) error {
		c.Vault.SSL.CaPath = config.String(s)
		return nil
	}), "vault-ssl-ca-path", "")

	flags.Var((funcVar)(func(s string) error {
		c.Vault.SSL.Cert = config.String(s)
		return nil
	}), "vault-ssl-cert", "")

	flags.Var((funcVar)(func(s string) error {
		c.Vault.SSL.Key = config.String(s)
		return nil
	}), "vault-ssl-key", "")

	flags.Var((funcVar)(func(s string) error {
		c.Vault.SSL.ServerName = config.String(s)
		return nil
	}), "vault-ssl-server-name", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.Vault.SSL.Verify = config.Bool(b)
		return nil
	}), "vault-ssl-verify", "")

	flags.Var((funcVar)(func(s string) error {
		c.Vault.Token = config.String(s)
		return nil
	}), "vault-token", "")

	flags.Var((funcBoolVar)(func(b bool) error {
		c.Vault.UnwrapToken = config.Bool(b)
		return nil
	}), "vault-unwrap-token", "")

	flags.Var((funcVar)(func(s string) error {
		w, err := config.ParseWaitConfig(s)
		if err != nil {
			return err
		}
		c.Wait = w
		return nil
	}), "wait", "")

	flags.BoolVar(&version, "v", false, "")
	flags.BoolVar(&version, "version", false, "")

	// If there was a parser error, stop
	if err := flags.Parse(args); err != nil {
		return nil, false, false, false, err
	}

	// Error if extra arguments are present
	args = flags.Args()
	if len(args) > 0 {
		return nil, false, false, false, fmt.Errorf("cli: extra args: %q", args)
	}

	// Create the final configuration
	finalC := config.DefaultConfig()

	// Merge all the provided configurations in the order supplied
	for _, path := range configPaths {
		c, err := config.FromPath(path)
		if err != nil {
			return nil, false, false, false, err
		}
		finalC = finalC.Merge(c)
	}

	// Add any CLI configuration options, since that's highest precedence
	finalC = finalC.Merge(c)

	// Finalize the configuration
	finalC.Finalize()

	return finalC, once, dry, version, nil
}

// handleError outputs the given error's Error() to the errStream and returns
// the given exit status.
func (cli *CLI) handleError(err error, status int) int {
	fmt.Fprintf(cli.errStream, "Consul Template returned errors:\n%s\n", err)
	return status
}

func (cli *CLI) setup(conf *config.Config) (*config.Config, error) {
	if err := logging.Setup(&logging.Config{
		Name:           Name,
		Level:          config.StringVal(conf.LogLevel),
		Syslog:         config.BoolVal(conf.Syslog.Enabled),
		SyslogFacility: config.StringVal(conf.Syslog.Facility),
		Writer:         cli.errStream,
	}); err != nil {
		return nil, err
	}

	return conf, nil
}

const usage = `
Usage: %s [options]

  Watches a series of templates on the file system, writing new changes when
  Consul is updated. It runs until an interrupt is received unless the -once
  flag is specified.

Options:

  -auth=<username[:password]>
      Set the basic authentication username (and password)

  -config=<path>
      Sets the path to a configuration file or folder on disk. This can be
      specified multiple times to load multiple files or folders. If multiple
      values are given, they are merged left-to-right, and CLI arguments take
      the top-most precedence.

  -consul=<address>
      Sets the address of the Consul instance

  -mesos=<address..protocol>
      Sets the address and protocol (http, https)

  -dedup
      Enable de-duplication mode - reduces load on Consul when many instances of
      Consul Template are rendering a common template

  -dry
      Print generated templates to stdout instead of rendering

  -exec=<command>
      Enable exec mode to run as a supervisor-like process - the given command
      will receive all signals provided to the parent process and will receive a
      signal when templates change

  -exec-kill-signal=<signal>
      Signal to send when gracefully killing the process

  -exec-kill-timeout=<duration>
      Amount of time to wait before force-killing the child

  -exec-reload-signal=<signal>
      Signal to send when a reload takes place

  -exec-splay=<duration>
      Amount of time to wait before sending signals

  -kill-signal=<signal>
      Signal to listen to gracefully terminate the process

  -log-level=<level>
      Set the logging level - values are "debug", "info", "warn", and "err"

  -max-stale=<duration>
      Set the maximum staleness and allow stale queries to Consul which will
      distribute work among all servers instead of just the leader

  -once
      Do not run the process as a daemon

  -pid-file=<path>
      Path on disk to write the PID of the process

  -reload-signal=<signal>
      Signal to listen to reload configuration

  -retry=<duration>
      The amount of time to wait if Consul returns an error when communicating
      with the API

  -ssl
      Use SSL when connecting to Consul

  -ssl-ca-cert
      Validate server certificate against this CA certificate file list

  -ssl-cert
      SSL client certificate to send to server

  -ssl-key
      SSL/TLS private key for use in client authentication key exchange

  -ssl-verify
      Verify certificates when connecting via SSL

  -syslog
      Send the output to syslog instead of standard error and standard out. The
      syslog facility defaults to LOCAL0 and can be changed using a
      configuration file

  -syslog-facility=<facility>
      Set the facility where syslog should log - if this attribute is supplied,
      the -syslog flag must also be supplied

  -template=<template>
       Adds a new template to watch on disk in the format 'in:out(:command)'

  -token=<token>
      Sets the Consul API token

  -vault-addr=<address>
      Sets the address of the Vault server

  -vault-renew-token
      Periodically renew the provided Vault API token - this defaults to "true"
      and will renew the token at half of the lease duration

  -vault-ssl
      Specifies is communications with Vault should be done via SSL

  -vault-ssl-ca-cert=<string>
      Sets the path to the CA certificate to use for TLS verification

  -vault-ssl-ca-path=<string>
      Sets the path to the CA to use for TLS verification

  -vault-ssl-cert=<string>
      Sets the path to the certificate to use for TLS verification

  -vault-ssl-key=<string>
      Sets the path to the key to use for TLS verification

  -vault-ssl-server-name=<string>
      Sets the name of the server to use when validating TLS.

  -vault-ssl-verify
      Enable SSL verification for communications with Vault.

  -vault-token=<token>
      Sets the Vault API token

  -vault-unwrap-token
      Unwrap the provided Vault API token (see Vault documentation for more
      information on this feature)

  -wait=<duration>
      Sets the 'min(:max)' amount of time to wait before writing a template (and
      triggering a command)

  -v, -version
      Print the version of this daemon
`

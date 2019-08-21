package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

const CREATE_KEYSPACE_TEMPLATE = `CREATE KEYSPACE IF NOT EXISTS yacht
WITH REPLICATION = { 'class': '%s', 'replication_factor' : %d }
AND DURABLE_WRITES=true`

// A pre-installed CQL server to which we connect via a URI
type CQLServerURI struct {
	uri                 string
	replicationFactor   int
	replicationStrategy string
	cluster             *gocql.ClusterConfig
}

func (server *CQLServerURI) ModeName() string {
	return "uri"
}

// Destroy yacht keyspace when done
type CQLServerURI_artefact struct {
	session *gocql.Session
}

func (a *CQLServerURI_artefact) Remove() {
	a.session.Query("DROP KEYSPACE IF EXISTS yacht").Exec()
}

func (server *CQLServerURI) Start(lane *Lane) error {

	if server.replicationFactor == 0 {
		server.replicationFactor = 1
	}
	if server.replicationStrategy == "" {
		server.replicationStrategy = "SimpleStrategy"
	}
	server.cluster = gocql.NewCluster(server.uri)
	server.cluster.Timeout, _ = time.ParseDuration("30s")
	// Create an administrative session to prepare
	// administrative server for testing
	session, err := server.cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	artefact := CQLServerURI_artefact{session: session}
	// Cleanup before running the suit
	artefact.Remove()
	// Create a keyspace for testing
	var create_keyspace = fmt.Sprintf(CREATE_KEYSPACE_TEMPLATE,
		server.replicationStrategy, server.replicationFactor)
	err = session.Query(create_keyspace).Exec()
	if err != nil {
		return merry.Wrap(err)
	}
	server.cluster.Keyspace = "yacht"
	lane.AddSuiteArtefact(&artefact)
	return nil
}

func (server *CQLServerURI) Connect() (Connection, error) {
	session, err := server.cluster.CreateSession()
	if err != nil {
		return nil, merry.Prepend(err, "when connecting to '"+server.uri+"'")
	}
	return &CQLConnection{session: session}, nil
}

// A single Scylla server

type CQLServerConfig struct {
	Dir                       string
	URI                       string
	Seed                      string
	SMP                       int
	ClusterName               string
	SkipWaitForGossipToSettle int
}

var SCYLLA_CONF_TEMPLATE string = `
cluster_name: {{.ClusterName}}
developer_mode: true
data_file_directories:
    - {{.Dir}}/data
commitlog_directory: {{.Dir}}/commitlog
hints_directory: {{.Dir}}/hints
view_hints_directory: {{.Dir}}/view_hints

listen_address: {{.URI}}
rpc_address: {{.URI}}
api_address: {{.URI}}
prometheus_address: {{.URI}}

seed_provider:
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
      parameters:
          - seeds: {{.Seed}}

skip_wait_for_gossip_to_settle: {{.SkipWaitForGossipToSettle}}
ring_delay_ms: 3000
`

type CQLServer struct {
	CQLServerURI
	builddir       string
	cfg            CQLServerConfig
	exe            string
	logFileName    string
	configFileName string
	cmd            *exec.Cmd
	logFile        *os.File
}

func (server *CQLServer) ModeName() string {
	return "single"
}

func (server *CQLServer) Start(lane *Lane) error {

	if err := server.FindScyllaExecutable(); err != nil {
		return err
	}

	if err := server.Install(lane); err != nil {
		return err
	}

	ylog.Printf("Starting server %s...", server.cfg.URI)

	if err := server.DoStart(lane); err != nil {
		return err
	}

	ylog.Printf("Started server %s", server.cfg.URI)

	server.CQLServerURI.uri = server.cfg.URI

	return server.CQLServerURI.Start(lane)
}

func (server *CQLServer) FindScyllaExecutable() error {
	server.exe = path.Join(server.builddir, "scylla")

	if st, err := os.Stat(server.exe); err != nil {
		return err
	} else if st.Mode()|0111 == 0 {
		return merry.Errorf("%s is not executable", server.exe)
	}
	return nil
}

type ReleaseURI_artefact struct {
	uri  string
	lane *Lane
}

func (a *ReleaseURI_artefact) Remove() {
	if a.uri != "" {
		a.lane.ReleaseURI(a.uri)
	}
}

type CQLServer_uninstall_artefact struct {
	server *CQLServer
}

func (a *CQLServer_uninstall_artefact) Remove() {
	os.RemoveAll(a.server.cfg.Dir)
	os.Remove(a.server.logFileName)
}

func (server *CQLServer) Install(lane *Lane) error {

	var err error
	// Scylla assumes all instances of a cluster use the same port,
	// so each instance needs an own IP address. The IP address
	// can be set by the cluster. Otherwise set it here.
	if server.cfg.URI == "" {
		if server.cfg.URI, err = lane.LeaseURI(); err != nil {
			return err
		}
		lane.AddSuiteArtefact(&ReleaseURI_artefact{uri: server.cfg.URI, lane: lane})
	}
	// Set the seed if it has not been pre-set.
	if server.cfg.Seed == "" {
		server.cfg.Seed = server.cfg.URI
	}

	// Instance subdirectory is a directory inside the lane,
	// so that each lane can run a cluster of instances
	// Derive subdirectory name from URI
	server.cfg.Dir = path.Join(lane.Dir(), server.cfg.URI)
	server.cfg.SMP = 1
	// Only reset ClusterName if it was not provided
	if server.cfg.ClusterName == "" {
		server.cfg.ClusterName = uuid.New().String()
	}
	server.logFileName = path.Join(lane.Dir(), server.cfg.URI+".log")
	// SCYLLA_CONF env variable is actually SCYLLA_CONF_DIR environment
	// variable, and the configuration file name is assumed to be scylla.yaml
	server.configFileName = path.Join(server.cfg.Dir, "scylla.yaml")

	lane.AddSuiteArtefact(&CQLServer_uninstall_artefact{server: server})

	if err := os.MkdirAll(server.cfg.Dir, 0750); err != nil {
		return err
	}

	// Redirect command output to a log file, derive log file name
	// from URI
	var logFile *os.File
	if logFile, err = os.OpenFile(server.logFileName,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {

		return err
	}
	// Open another file descriptor to ensure its position is not advanced
	// by writes
	if server.logFile, err = os.Open(server.logFileName); err != nil {
		return err
	}

	// Create a configuration file. Unfortunately, Scylla can't start without
	// one. Since we have to create a configuration file, let's avoid
	// command line options.
	configFile, err := os.OpenFile(server.configFileName,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	statement := template.Must(template.New("SCYLLA_CONF").Parse(SCYLLA_CONF_TEMPLATE))
	statement.Execute(configFile, &server.cfg)
	configFile.Close()

	// Do not confuse Scylla binary if we derived this from the parent process
	os.Unsetenv("SCYLLA_HOME")

	cmd := exec.Command(server.exe, fmt.Sprintf("--smp=%d", server.cfg.SMP))
	cmd.Dir = server.cfg.Dir
	cmd.Env = append(cmd.Env, fmt.Sprintf("SCYLLA_CONF=%s", server.cfg.Dir))
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	server.cmd = cmd

	return nil
}

type CQLServer_stop_artefact struct {
	cmd *exec.Cmd
}

func (a *CQLServer_stop_artefact) Remove() {
	ylog.Printf("Stopping server %d", a.cmd.Process.Pid)
	a.cmd.Process.Kill()
	// 3 seconds is enough for a good database to die gracefully:
	// send SIGKILL if SIGTERM doesn't reach its target
	timer := time.AfterFunc(3*time.Second, func() {
		syscall.Kill(a.cmd.Process.Pid, syscall.SIGKILL)
	})
	timer.Stop()
	a.cmd.Process.Wait()
	ylog.Printf("Stopped server %d", a.cmd.Process.Pid)
}

func FindLogFilePattern(file *os.File, pattern string) bool {
	var patternRE = regexp.MustCompile(pattern)
	var scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		if patternRE.Match(scanner.Bytes()) {
			return true
		}
	}
	return false
}

func (server *CQLServer) DoStart(lane *Lane) error {
	const START_TIMEOUT = 300 * time.Second
	lane.AddExitArtefact(&CQLServer_stop_artefact{cmd: server.cmd})
	if err := server.cmd.Start(); err != nil {
		return err
	}
	start := time.Now()
	for _ = range time.Tick(time.Millisecond * 10) {
		if FindLogFilePattern(server.logFile, "Scylla.*initialization completed") {
			break
		}
		if time.Now().Sub(start) > START_TIMEOUT {
			return merry.Errorf("failed to start server %s on lane %s, check server log at %s",
				server.cfg.URI, lane.id, palette.Path(server.logFileName))
		}
	}
	return nil
}

// CQLCluster testing mode
type CQLCluster struct {
	servers     [3]*CQLServer
	builddir    string
	clusterName string
}

func (cluster *CQLCluster) ModeName() string {
	return "cluster"
}

func (cluster *CQLCluster) Start(lane *Lane) error {

	var seeds = make([]string, len(cluster.servers))
	var err error
	for i, _ := range seeds {
		if seeds[i], err = lane.LeaseURI(); err != nil {
			return err
		}
		lane.AddSuiteArtefact(&ReleaseURI_artefact{uri: seeds[i], lane: lane})
	}
	var seedsStr = strings.Join(seeds, ", ")

	var wg sync.WaitGroup

	wg.Add(len(cluster.servers))

	status := make(chan error, len(cluster.servers))

	startOne := func(server *CQLServer) {
		defer wg.Done()
		if err := server.Start(lane); err != nil {
			status <- err
		}
	}

	cluster.clusterName = uuid.New().String()
	for i, _ := range cluster.servers {
		server := CQLServer{builddir: cluster.builddir}
		// Set a shared cluster name
		server.cfg.ClusterName = cluster.clusterName
		server.cfg.URI = seeds[i]
		server.cfg.Seed = seedsStr
		server.CQLServerURI.replicationFactor = len(cluster.servers)
		// We need gossip for clustered start
		server.cfg.SkipWaitForGossipToSettle = 5

		go startOne(&server)
		cluster.servers[i] = &server
	}
	wg.Wait()
	select {
	case err := <-status:
		return err
	default:
		return nil
	}
}

func (cluster *CQLCluster) Connect() (Connection, error) {
	return cluster.servers[0].Connect()
}

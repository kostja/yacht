package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"syscall"
	"text/template"
	"time"

	"github.com/ansel1/merry"
	"github.com/google/uuid"
)

type CQLServerConfig struct {
	Dir         string
	URI         string
	SMP         int
	ClusterName string
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

seed_provider:
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
      parameters:
          - seeds: {{.URI}}

skip_wait_for_gossip_to_settle: 0
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

	if err := server.DoStart(lane); err != nil {
		return err
	}

	server.CQLServerURI = CQLServerURI{uri: server.cfg.URI}

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

type CQLServer_uninstall_artefact struct {
	server *CQLServer
	lane   *Lane
}

func (a *CQLServer_uninstall_artefact) Remove() {
	a.lane.ReleaseURI(a.server.cfg.URI)
	os.RemoveAll(a.server.cfg.Dir)
	os.Remove(a.server.logFileName)
}

func (server *CQLServer) Install(lane *Lane) error {

	// Scylla assumes all instances of a cluster use the same port,
	// so each instance needs an own IP address.
	server.cfg.URI = lane.LeaseURI()
	// Instance subdirectory is a directory inside the lane,
	// so that each lane can run a cluster of instances
	// Derive subdirectory name from URI
	server.cfg.Dir = path.Join(lane.Dir(), server.cfg.URI)
	server.cfg.SMP = 1
	if server.cfg.ClusterName == "" {
		server.cfg.ClusterName = uuid.New().String()
	}
	server.logFileName = path.Join(lane.Dir(), server.cfg.URI+".log")
	// SCYLLA_CONF env variable is actually SCYLLA_CONF_DIR environment
	// variable, and the configuration file name is assumed to be scylla.yaml
	server.configFileName = path.Join(server.cfg.Dir, "scylla.yaml")

	lane.AddSuiteArtefact(&CQLServer_uninstall_artefact{server: server, lane: lane})

	if err := os.MkdirAll(server.cfg.Dir, 0750); err != nil {
		return err
	}

	// Redirect command output to a log file, derive log file name
	// from URI
	var err error
	server.logFile, err = os.OpenFile(server.logFileName,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
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
	cmd.Stdout = server.logFile
	cmd.Stderr = server.logFile

	server.cmd = cmd

	return nil
}

type CQLServer_stop_artefact struct {
	cmd *exec.Cmd
}

func (a *CQLServer_stop_artefact) Remove() {
	a.cmd.Process.Kill()
	// Send SIGKILL if killing doesn't succeed
	timer := time.AfterFunc(3*time.Second, func() {
		syscall.Kill(a.cmd.Process.Pid, syscall.SIGKILL)
	})
	timer.Stop()
	a.cmd.Process.Wait()
}

func (server *CQLServer) DoStart(lane *Lane) error {
	lane.AddExitArtefact(&CQLServer_stop_artefact{cmd: server.cmd})
	if err := server.cmd.Start(); err != nil {
		return err
	}
	time.Sleep(0 * time.Second)
	return nil
}

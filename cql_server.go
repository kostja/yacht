package main

import (
	"bytes"
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

var OPTIONS = []string{
	"--developer-mode=true",
	"--data-file-directories={{.Dir}}",
	"--commitlog-directory={{.Dir}}",
	"--hints-directory={{.Dir}}",
	"--view-hints-directory={{.Dir}}",
	"--listen-address={{.URI}}",
	"--rpc-address={{.URI}}",
	"--api-address={{.URI}}",
	"--seed-provider-parameters=seeds={{.URI}}",
	"--smp={{.SMP}}",
	"--cluster-name={{.ClusterName}}",
}

type CQLServer struct {
	CQLServerURI
	builddir    string
	cfg         CQLServerConfig
	exe         string
	logfileName string
	cmd         *exec.Cmd
	logfile     *os.File
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
	os.Remove(a.server.logfileName)
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
	server.logfileName = path.Join(lane.Dir(), server.cfg.URI+".log")

	lane.AddSuiteArtefact(&CQLServer_uninstall_artefact{server: server, lane: lane})

	if err := os.MkdirAll(server.cfg.Dir, 0750); err != nil {
		return err
	}

	// Redirect command output to a log file, derive log file name
	// from URI
	var err error
	server.logfile, err = os.OpenFile(server.logfileName,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	options := make([]string, len(OPTIONS))
	for i, option := range OPTIONS {
		var b bytes.Buffer
		statement := template.Must(template.New("OPTIONS").Parse(option))
		statement.Execute(&b, &server.cfg)
		options[i] = b.String()
	}
	// Do not confuse Scylla binary if we derived these from the shell
	os.Unsetenv("SCYLLA_HOME")
	os.Unsetenv("SCYLLA_CONF")

	cmd := exec.Command(server.exe, options...)
	cmd.Dir = server.cfg.Dir
	fmt.Printf("%+v", cmd.Env)
	cmd.Env = append(cmd.Env, fmt.Sprintf("SCYLLA_HOME=%s", server.cfg.Dir))
	cmd.Stdout = server.logfile
	cmd.Stderr = server.logfile

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

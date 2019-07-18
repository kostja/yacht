package main

import "log"
import "os"
import "os/signal"
import "fmt"
import "strings"
import "github.com/spf13/pflag"

//import "bufio"

// Yacht running environment.
// @todo Initialize using command line options, environment variables
// and a configuration file
type Env struct {
	// Continue running tests even if a single test or test case fails
	force bool
	// Run only tests matching the given masks. The masks are
	// separated by space. If multiple
	// masks are provided, every match counts once, so if
	// the same test file matches two masks it is run twice.
	filters []string
	// Where to look for test suites
	srcdir string
	// A temporary directory where to run the tests;
	// Data from previous runs is removed if it remains
	// in the directory
	vardir string
	// Where to look for server binaries
	builddir string
	// Rather than set up and tear down a new server or
	// cluster use the given URI to connect to  an existing
	// server.
	uri string
}

// Parse command line and configuration options and print
// usage if necessary
func (env *Env) Usage() {
	pflag.BoolVar(&env.force, "force", false, `Go on with other tests in case of an
	individual test failure. Default: false`)
	pflag.Parse()
	env.filters = pflag.Args()
}

// An artefact is anything left by a test or suite while
// it runs. A keyspace created against a running server,
// a runnign process, etc.
// Some artefacts must be removed when shutting down,
// others are cleared between tests.
// Artefacts such as server data diretory or log file
// are removed when a test starts, not when it ends, to
// be able to inspect them in case of a crash or test failure.
type Artefact interface {
	Remove()
}

// Test lane is a directory on disk containing
// data of a running server, log files and so on.
type Lane struct {
	// Artefacts which must be removed before a test starts
	pre []Artefact
	// Artefacts which must be removed at harness exit
	post []Artefact
	// Lane data directory
	dir string
}

// Clear the lane beween two test suite invocations
func (lane *Lane) Clear() {
	// Clear the "post" artefacts first, they may depend on "pre"
	// artefacts, e.g. a post artefact is a running server and
	// a pre artefact is its data directory
	for _, artefact := range lane.post {
		artefact.Remove()
	}
	// Clear the artefacts array, the artefacts are now gone
	lane.post = nil

	for _, artefact := range lane.pre {
		artefact.Remove()
	}
	// Clear the artefacts array, the artefacts are now gone
	lane.pre = nil
}

// Remove all artefacts, such as running servers, on an abnormal exit

func (lane *Lane) Abort() {
	for _, artefact := range lane.post {
		artefact.Remove()
	}
	lane.post = nil
}

type TestSuite interface {
	Setup(*Lane)
	Run(bool, *Lane)
}

// The main testing harness state
type Yacht struct {
	env    Env
	lane   Lane
	suites []TestSuite
}

// Check environment, find test suites
func YachtNew(env Env) Yacht {
	yacht := Yacht{}
	yacht.env = env
	return yacht
}

// Remove artefacts of tests which could still be in flight
func (yacht *Yacht) TearDown() {
	yacht.lane.Abort()
}

// Kill running servers on SIGINT but leave the data directory
// intact, available for inspection
func setSignalAction(yacht *Yacht) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			yacht.TearDown()
			log.Fatal("Got signal %v, exiting", sig)
		}
	}()
}

func (yacht *Yacht) PrintGreeting() {
	fmt.Println("Started", strings.Join(os.Args[:], " "))
	fmt.Println(yacht.env.filters)
}

func (yacht *Yacht) PrintSummary() {
}

func (yacht *Yacht) Run() {
	yacht.PrintGreeting()

	for _, suite := range yacht.suites {
		// Clear the lane between test suites
		// Note, it's done before the suite is started,
		// not fater, to preserve important artefacts
		// between runs
		yacht.lane.Clear()
		// @todo: multiple lanes to run tests in parallel
		suite.Setup(&yacht.lane)
		suite.Run(yacht.env.force, &yacht.lane)
		//		if err != nil && env.force == false {
		//			break
		//		}
	}
	yacht.PrintSummary()
}

func main() {

	var env Env

	env.Usage()

	yacht := YachtNew(env)

	defer yacht.TearDown()

	setSignalAction(&yacht)

	yacht.Run()
}

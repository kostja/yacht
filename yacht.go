package main

import "log"
import "os"
import "os/signal"
import "fmt"

import "path"
import "path/filepath"
import "strings"
import "github.com/spf13/pflag"
import "github.com/spf13/viper"

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

func (env *Env) configure() {
	// Look for .yacht.yml or .yacht.json in ~
	//
	viper.SetConfigName(".yacht") // name of config file (without extension)
	viper.AddConfigPath("$HOME/")

	// Helper structures to match the nested json/yaml of the config
	// file. The names have to be uppercased for marshalling
	type Scylla struct {
		Builddir string
		Srcdir   string
	}
	type Configuration struct {
		Scylla Scylla
		Vardir string
	}

	cwd, _ := os.Getwd()

	// Fill with defaults in case the config file is absent or empty
	configuration := Configuration{
		Vardir: cwd,
		Scylla: Scylla{
			Builddir: path.Join(os.Getenv("HOME"), "scylla/build/dev"),
			Srcdir:   path.Join(os.Getenv("HOME"), "scylla/tests"),
		},
	}
	// Check if a config file is present
	if err := viper.ReadInConfig(); err == nil {
		fmt.Printf("Using configuration from %s\n", viper.ConfigFileUsed())
		// Parse the config file
		if err := viper.Unmarshal(&configuration); err != nil {
			log.Fatalf("Parsing configuration failed: %v", err)
		}
	} else if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		// Configuration file not found
	} else {
		// Configuration file is not accessible
		log.Fatalf("Error reading config file %s:\n%v",
			viper.ConfigFileUsed(), err)
	}
	env.builddir, _ = filepath.Abs(configuration.Scylla.Builddir)
	env.srcdir, _ = filepath.Abs(configuration.Scylla.Srcdir)
	env.vardir, _ = filepath.Abs(configuration.Vardir)
}

// Parse command line and configuration options and print
// usage if necessary
func (env *Env) Usage() {
	env.configure()

	pflag.BoolVar(&env.force, "force", false,
		`Go on with other tests in case of an individual test failure.
Default: false`)
	pflag.Usage = func() {
		fmt.Println("yacht - a Yet Another Scylla Harness for Testing")
		fmt.Printf("\nUsage: %v [--force] [pattern [...]]\n", os.Args[0])
		fmt.Println(
			`
Positional arguments:
[pattrn [...]]  List of test name patterns to look for in
                suites. Each name is used as a substring to look for
                in the path to test file, e.g. "desc" will run all
                tests that have "desc" in their name in all suites,
                "lwt/desc" will only enable tests starting with "desc"
                in "lwt" suite. Default: run all tests in all suites.`)
		fmt.Println("\nOptional arguments:")
		pflag.PrintDefaults()
		os.Exit(0)
	}
	pflag.Parse()
	env.filters = pflag.Args()
	log.Println(env)
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
func (yacht *Yacht) TearDown() int {
	yacht.lane.Abort()
	return 0
}

// Kill running servers on SIGINT but leave the data directory
// intact, available for inspection
func setSignalAction(yacht *Yacht) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			yacht.TearDown()
			log.Fatalf("Got signal %v, exiting", sig)
		}
	}()
}

func (yacht *Yacht) PrintGreeting() {
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
	fmt.Println("Started", strings.Join(os.Args[:], " "))

	var env Env
	env.Usage()
	yacht := YachtNew(env)
	setSignalAction(&yacht)
	yacht.Run()

	os.Exit(yacht.TearDown())
}

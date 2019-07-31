package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var ylog *log.Logger

// A directory with tests
type TestSuite interface {
	FindTests(path string, patterns []string) error
	IsEmpty() bool
	AddMode(server Server)
	Servers() []Server
	PrepareLane(*Lane, Server) error
	RunSuite(force bool, lane *Lane, server Server) (int, error)
}

// A single test
type TestFile interface {
	Init()
	RunTest(force bool, c Connection, lane *Lane) (string, error)
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

// A connection is used by a test file to execute queries
type Connection interface {
	Execute(query string) (string, error)
	Close()
}

// A server is a server or instance and something we can connect to
// and run queries
type Server interface {
	Start(lane *Lane) error
	Connect() (Connection, error)
	ModeName() string
}

// Yacht running environment.
type Env struct {
	// Continue running tests even if a single test fails
	force bool
	// Run only tests matching the given patterns. The patterns are
	// separated by space. If multiple
	// patterns are provided, every test name is matched against every
	// pattern, so if the same test file matches two patterns it is run twice.
	patterns []string
	// Where to look for test suites
	srcdir string
	// A temporary directory where to run the tests;
	// Data from previous runs is removed if it remains
	// in the directory
	vardir string
	// Where to look for server binaries
	builddir string
	// --uri option, if provided, or uri: in the configuration file,
	// or "127.0.0.1"
	uri string
}

// Look up a configuration file and load it if found
// Exit on error, such as incorrect configuration syntax.
func (env *Env) configure() {
	// A configuration file can be provided to help yacht find
	// the server binary and test sources.
	// Look for .yacht.yml or .yacht.json in the home directory
	//
	env_cfg := viper.New()
	env_cfg.SetConfigName(".yacht") // name of config file (without extension)
	env_cfg.AddConfigPath("$HOME/")

	// Helper structures to match the nested json/yaml of the configuration
	// file. The names have to be uppercased for Go introspection to work.
	type Scylla struct {
		Builddir string
		Srcdir   string
		Uri      string
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
			Uri:      "127.0.0.1",
		},
	}
	// Check if a config file is present
	if err := env_cfg.ReadInConfig(); err == nil {
		fmt.Printf("Using configuration file %s\n",
			palette.Path(env_cfg.ConfigFileUsed()))
		// Parse the config file
		if err := env_cfg.Unmarshal(&configuration); err != nil {
			fmt.Printf("Parsing configuration failed: %v", err)
			os.Exit(1)
		}
	} else if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		// Configuration file not found
	} else {
		// Configuration file is not accessible
		fmt.Printf("Error reading config file %s:\n%v",
			env_cfg.ConfigFileUsed(), err)
		os.Exit(1)
	}
	env.builddir, _ = filepath.Abs(configuration.Scylla.Builddir)
	env.srcdir, _ = filepath.Abs(configuration.Scylla.Srcdir)
	env.vardir, _ = filepath.Abs(configuration.Vardir)
	env.uri = configuration.Scylla.Uri
	var check_dir = func(name string, value string) {
		var msg string = "Incorrect configuration setting for %s: %v\n"
		st, err := os.Stat(value)
		if err != nil {
			fmt.Printf(msg, name, err)
		} else if st.IsDir() == false {
			fmt.Printf(msg, name, fmt.Sprintf("%s is not a directory", value))
		} else {
			return
		}
		os.Exit(1)
	}
	check_dir("scylla.srcdir", env.srcdir)
	check_dir("scylla.builddir", env.builddir)
	// vardir is ok to not exist
}

// Parse command line and configuration options and print
// usage if necessary. Exit in incorrect options or configuration
// file content.
func (env *Env) Usage() {
	env.configure()

	pflag.BoolVar(&env.force, "force", false,
		`Go on with other tests in case of an individual test failure.
Default: false`)
	pflag.StringVar(&env.uri, "uri", env.uri,
		"Server URI to connect to in URI mode")
	pflag.Usage = func() {
		fmt.Println("yacht - a Yet Another Scylla Harness for Testing")
		fmt.Printf("\nUsage: %v [--force] [pattern [...]]\n", os.Args[0])
		fmt.Println(
			`
Positional arguments:
[pattrn [...]]  List of test name patterns to look for in suites.
                Each name is used as a substring to look for in the
                path to test file, e.g. "desc" will run all tests
                that have "desc" in their name in all suites,
                "lwt/desc" will only enable tests starting with "desc"
                in "lwt" suite. Default: run all tests in all suites.`)
		fmt.Println("\nOptional arguments:")
		pflag.PrintDefaults()
		os.Exit(0)
	}
	pflag.Parse()
	env.patterns = pflag.Args()
	if len(env.patterns) == 0 {
		// Add a wildcard if there are no user defined patterns
		env.patterns = append(env.patterns, "")
	}
}

// Test lane is a directory on disk containing
// data of a running server, log files and so on.
type Lane struct {
	// Artefacts which must be removed before a test starts
	removeBeforeNextSuite []Artefact
	// Artefacts which must be removed at harness exit
	removeBeforeExit []Artefact
	// Lane data directory
	dir string
	// Unique lane id, used as a subdirectory within the directory
	id string
	// The list of failed tests
	failed []string
}

func (lane *Lane) AddExitArtefact(artefact Artefact) {
	lane.removeBeforeExit = append(lane.removeBeforeExit, artefact)
}

func (lane *Lane) AddSuiteArtefact(artefact Artefact) {
	lane.removeBeforeNextSuite = append(lane.removeBeforeNextSuite, artefact)
}

// Used as server working directory
func (lane *Lane) Dir() string {
	return lane.dir
}

// With multiple servers we need to be careful all of them do
// not share the same host/port
func (lane *Lane) LeaseURI() string {
	return "127.0.0.2"
}

func (lane *Lane) ReleaseURI(string) {
}

func (lane *Lane) FailedTests() []string {
	return lane.failed
}

func (lane *Lane) Init(id string, dir string) {
	// @todo add random characters
	lane.id = id
	lane.dir, _ = filepath.Abs(path.Join(dir, id))
	// Create the directory if it doesn't exist or clear
	// it if it does
	if _, err := os.Stat(lane.dir); err != nil && !os.IsNotExist(err) {
		fmt.Printf("Failed to access temporary directory %s", lane.dir)
		os.Exit(1)
	} else if err == nil {
		if err := os.RemoveAll(lane.dir); err != nil {
			fmt.Printf("Failed to remove temporary directory %s", lane.dir)
			os.Exit(1)
		}
	}
	if err := os.MkdirAll(lane.dir, 0750); err != nil {
		fmt.Printf("Failed to create temporary directory %s", lane.dir)
		os.Exit(1)
	}
}

// Clear the lane beween two test suite invocations
func (lane *Lane) CleanupBeforeNextSuite() {
	// Clear the "suite" artefacts first, they may depend on "exit"
	// artefacts, e.g. a running server may depend on the data in
	// the data directory
	for _, artefact := range lane.removeBeforeExit {
		artefact.Remove()
	}
	// Clear the artefacts array, the artefacts are now gone
	lane.removeBeforeExit = nil

	for _, artefact := range lane.removeBeforeNextSuite {
		artefact.Remove()
	}
	// Clear the artefacts array, the artefacts are now gone
	lane.removeBeforeNextSuite = nil
	lane.failed = nil
}

// Remove all artefacts, such as running servers, on an abnormal exit
// Keep the test artefacts for inspection.
func (lane *Lane) CleanupBeforeExit() {
	for _, artefact := range lane.removeBeforeExit {
		artefact.Remove()
	}
	lane.removeBeforeExit = nil
}

// The main testing harness state
type Yacht struct {
	// Options and configuration settings
	env Env
	// Execution environemnt, @todo: have many
	lane Lane
	// List of suites to run, in different configurations
	suites []TestSuite
}

// Kill running servers on SIGINT but leave the data directory
// intact
func setSignalAction(yacht *Yacht) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			yacht.lane.CleanupBeforeExit()
			fmt.Printf("Got signal %v, exiting", sig)
			os.Exit(1)
		}
	}()
}

// Go over files in srcdir and look up suite.yaml/json
// in everything that looks like a dir. If there is a suite
// configuration file and it has suite type that we recocgnize,
// add it as a suite to the list of suite.
// Creating a suite object will put all files in the suite
// directory to the suite inventory
func (yacht *Yacht) findSuites() {

	fmt.Printf("Looking for suites at %s\n", palette.Path(yacht.env.srcdir))
	files, err := filepath.Glob(path.Join(yacht.env.srcdir, "*"))
	if err != nil {
		fmt.Printf("Failed to find suites in %s: %v", yacht.env.srcdir, err)
		os.Exit(1)
	}
	for _, path := range files {
		st, err := os.Stat(path)
		if err != nil {
			fmt.Printf("Skipping broken suite %s: %s",
				palette.Path("%s", path), palette.Warn("%v", err))
			continue
		}
		// Skip non-directories, it's OK to have other files in srcdir
		if st.IsDir() == false {
			continue
		}
		suite_cfg := viper.New()
		suite_cfg.SetConfigName("suite")
		suite_cfg.AddConfigPath(path)
		// Every suite.yaml config must have a suite type and an
		// optional description.
		type BasicSuiteConfiguration struct {
			Type        string
			Description string
			Mode        []map[string]string
		}
		// Skip files which can not be read
		if err := suite_cfg.ReadInConfig(); err == nil {
			var cfg BasicSuiteConfiguration
			if err := suite_cfg.Unmarshal(&cfg); err != nil {
				fmt.Printf("Failed to read suite configuration at %s: %s",
					palette.Path("%s", path), palette.Warn("%v", err))
				continue
			}
			if cfg.Type == "" {
				// There is no configuration file
				continue
			}
			if strings.EqualFold(cfg.Type, "cql") != true {
				fmt.Printf("Skipping unknown suite type '%s' at %s",
					palette.Crit("%s", cfg.Type), palette.Path("%s", path))
				continue
			}
			suite := CQLTestSuite{
				description: cfg.Description,
			}
			if err := suite.FindTests(path, yacht.env.patterns); err != nil {
				fmt.Printf("Failed to initialize a suite at %s: %v",
					palette.Path("%s", path), palette.Crit("%v", err))
				continue
			}
			// Only append the siute if it is not empty
			if suite.IsEmpty() == true {
				continue
			}
			if len(cfg.Mode) == 0 {
				cfg.Mode = append(cfg.Mode, map[string]string{"type": "uri"})
			}
			for _, mode_cfg := range cfg.Mode {
				var server Server
				if strings.EqualFold(mode_cfg["type"], "uri") == true {
					server = &CQLServerURI{uri: yacht.env.uri}
				} else if strings.EqualFold(mode_cfg["type"], "single") == true {
					server = &CQLServer{builddir: yacht.env.builddir}
				} else {
					fmt.Printf("Skipping unknown mode '%s' in suite '%s' at %s\n",
						palette.Crit("%s", mode_cfg["type"]),
						palette.Crit("%s", suite.name),
						palette.Path("%s", suite_cfg.ConfigFileUsed()))
					continue
				}
				suite.AddMode(server)
			}
			if len(suite.Servers()) > 0 {
				yacht.suites = append(yacht.suites, &suite)
			}
		}
	}
	if len(yacht.suites) == 0 {
		fmt.Printf(" ... found no matching suites\n")
	}
}

// Run found suites. Return the list of failed test and result
func (yacht *Yacht) RunSuites() ([]string, int) {

	var rc int = 0
	var failed []string
	for _, suite := range yacht.suites {
		PrintSuiteBeginBlurb()
		for _, server := range suite.Servers() {
			// Clear the lane between test suites
			// Note, it's done before the suite is started,
			// not after, to preserve important artefacts
			// between runs
			yacht.lane.CleanupBeforeNextSuite()
			if err := suite.PrepareLane(&yacht.lane, server); err != nil {
				fmt.Printf("%s%v\n", palette.Crit("lane failure: "), err)
				return failed, 1
			}
			if suite_rc, err := suite.RunSuite(yacht.env.force, &yacht.lane, server); err != nil {
				fmt.Printf("%s%v\n", palette.Crit("yacht failure: "), err)
				return failed, 1
			} else {
				rc |= suite_rc
				failed = append(failed, yacht.lane.FailedTests()...)
				if rc != 0 && yacht.env.force == false {
					break
				}
			}
		}
		PrintSuiteEndBlurb()
	}
	return failed, rc
}

func (yacht *Yacht) Run() int {

	yacht.lane.Init("1", yacht.env.vardir)

	yacht.findSuites()

	failed, rc := yacht.RunSuites()
	if len(failed) != 0 {
		if yacht.env.force == true {
			fmt.Printf("%s %s\n", palette.Warn("Not all tests executed successfully: "),
				palette.Path("%v", failed))
		} else {
			fmt.Printf("%s %s\n", palette.Crit("Test failed: "), palette.Path(failed[0]))
		}
	}
	return rc
}

func OpenLog(dir string) {
	var name = path.Join(dir, "yacht.log")
	logFile, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("%s %s\n", palette.Crit("Failed to open log file"),
			palette.Path(name))
		os.Exit(1)
	}
	ylog = log.New(logFile, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)
}

func main() {
	fmt.Println("Started", strings.Join(os.Args[:], " "))

	var env Env
	env.Usage()

	OpenLog(env.vardir)

	yacht := Yacht{
		env: env,
	}
	setSignalAction(&yacht)
	rc := yacht.Run()
	yacht.lane.CleanupBeforeExit()
	os.Exit(rc)
}

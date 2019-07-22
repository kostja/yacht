package main

import "log"
import "os"
import "os/signal"
import "fmt"
import "bufio"

import "path"
import "path/filepath"
import "strings"
import "github.com/spf13/pflag"
import "github.com/spf13/viper"
import "github.com/gocql/gocql"

// A directory with tests
type TestSuite interface {
	FindTests(path string, patterns []string) error
	IsEmpty() bool
	PrepareLane(*Lane)
	Run(force bool) error
}

// A single test
type TestFile interface {
	Run(force bool, c *Connection) error
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
	if err := env_cfg.ReadInConfig(); err == nil {
		fmt.Printf("Using configuration file %s\n", env_cfg.ConfigFileUsed())
		// Parse the config file
		if err := env_cfg.Unmarshal(&configuration); err != nil {
			log.Fatalf("Parsing configuration failed: %v", err)
		}
	} else if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		// Configuration file not found
	} else {
		// Configuration file is not accessible
		log.Fatalf("Error reading config file %s:\n%v",
			env_cfg.ConfigFileUsed(), err)
	}
	env.builddir, _ = filepath.Abs(configuration.Scylla.Builddir)
	env.srcdir, _ = filepath.Abs(configuration.Scylla.Srcdir)
	env.vardir, _ = filepath.Abs(configuration.Vardir)
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

func (lane *Lane) Init(id string, dir string) {
	// @todo add random characters
	lane.id = id
	lane.dir, _ = filepath.Abs(path.Join(dir, id))
	// Create the directory if it doesn't exist or clear
	// it if it does
	if _, err := os.Stat(lane.dir); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to access temporary directory %s", lane.dir)
	} else if err == nil {
		if err := os.RemoveAll(lane.dir); err != nil {
			log.Fatalf("Failed to remove temporary directory %s", lane.dir)
		}
	}
	if err := os.MkdirAll(lane.dir, 0750); err != nil {
		log.Fatalf("Failed to create temporary directory %s", lane.dir)
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
	// List of suites to run
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
			log.Fatalf("Got signal %v, exiting", sig)
		}
	}()
}

func (yacht *Yacht) PrintSummary() {
}

// Go over files in srcdir and look up suite.yaml/json
// in everything that looks like a dir. If there is a suite
// configuration file and it has suite type that we recocgnize,
// add it as a suite to the list of suite.
// Creating a suite object will put all files in the suite
// directory to the suite inventory
func (yacht *Yacht) findSuites() {

	fmt.Printf("Looking for suites at %s\n", yacht.env.srcdir)
	files, err := filepath.Glob(path.Join(yacht.env.srcdir, "*"))
	if err != nil {
		log.Fatalf("Failed to find suites in %s: %v", yacht.env.srcdir, err)
	}
	for _, path := range files {
		st, err := os.Stat(path)
		if err != nil {
			// @todo: add warning color
			log.Printf("Skipping broken suite %s: %v", path, err)
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
		}
		// Skip files which can not be read
		if err := suite_cfg.ReadInConfig(); err == nil {
			var cfg BasicSuiteConfiguration
			if err := suite_cfg.Unmarshal(&cfg); err != nil {
				// @todo: add warning color
				log.Printf("Failed to read suite configuration at %s: %v", path, err)
				continue
			}
			if cfg.Type == "" {
				// There is no configuration file
				continue
			}
			if cfg.Type != "cql" {
				// @todo: add warning color
				log.Printf("Skipping unknown suite type '%s' at %s",
					cfg.Type, path)
				continue
			}
			suite := cql_test_suite{
				description: cfg.Description,
			}
			if err := suite.FindTests(path, yacht.env.patterns); err != nil {
				// @todo add warning color
				log.Printf("Failed to initialize a suite at %s: %v", path, err)
				continue
			}
			// Only append the siute if it is not empty
			if suite.IsEmpty() == false {
				yacht.suites = append(yacht.suites, &suite)
			}
		}
	}
	if len(yacht.suites) == 0 {
		fmt.Printf(" ... found no matching suites\n")
	}
}

func (yacht *Yacht) PrintSuiteBeginBlurb() {
	fmt.Printf("%s\n", strings.Repeat("=", 80))
	fmt.Printf("LANE ")
	fmt.Printf("%-46s", "TEST")
	fmt.Printf("%-14s", "OPTIONS")
	fmt.Printf("RESULT\n")
	fmt.Printf("%s\n", strings.Repeat("-", 75))
}

func (yacht *Yacht) PrintSuiteEndBlurb() {
	fmt.Printf("%s\n", strings.Repeat("-", 75))
}

func (yacht *Yacht) Run() int {

	yacht.lane.Init("1", yacht.env.vardir)
	var rc int = 0

	yacht.findSuites()

	for _, suite := range yacht.suites {
		// Clear the lane between test suites
		// Note, it's done before the suite is started,
		// not after, to preserve important artefacts
		// between runs
		yacht.lane.CleanupBeforeNextSuite()
		// @todo: multiple lanes to run tests in parallel
		yacht.PrintSuiteBeginBlurb()
		suite.PrepareLane(&yacht.lane)
		if err := suite.Run(yacht.env.force); err != nil {

			rc = 1
		} else {
			yacht.PrintSuiteEndBlurb()
		}

		//		if err != nil && env.force == false {
		//			break
		//		}
	}
	// if all OK clear the lane
	yacht.PrintSummary()
	return rc
}

func main() {
	fmt.Println("Started", strings.Join(os.Args[:], " "))

	var env Env
	env.Usage()
	yacht := Yacht{
		env: env,
	}
	setSignalAction(&yacht)
	defer yacht.lane.CleanupBeforeExit()
	os.Exit(yacht.Run())
}

// Implementation

type cql_connection struct {
	session *gocql.Session
}

func (c *cql_connection) Execute(query string) (string, error) {
	// @todo handle errors and serialize results
	c.session.Query(query).Exec()
	return query, nil
}

func (c *cql_connection) Close() {
	c.session.Close()
}

// A pre-installed CQL server to which we connect via a URI
type cql_server_uri struct {
	uri     string
	cluster *gocql.ClusterConfig
}

// Destroy yacht keyspace when done
type cql_server_uri_artefact struct {
	session *gocql.Session
}

func (a *cql_server_uri_artefact) Remove() {
	a.session.Query("DROP KEYSPACE IF EXISTS yacht").Exec()
}

func (server *cql_server_uri) Start(lane *Lane) error {
	server.cluster = gocql.NewCluster(server.uri)
	// Create an administrative session to prepare
	// administrative server for testing
	session, err := server.cluster.CreateSession()
	if err != nil {
		return err
	}
	artefact := cql_server_uri_artefact{session: session}
	// Cleanup before running the suit
	artefact.Remove()
	// Create a keyspace for testing
	err = session.Query(`CREATE KEYSPACE IF NOT EXISTS yacht WITH REPLICATION =
{ 'class': 'NetworkTopologyStrategy' } AND DURABLE_WRITES=true`).Exec()
	if err != nil {
		return err
	}
	server.cluster.Keyspace = "yacht"
	lane.AddSuiteArtefact(&artefact)
	return nil
}

func (server *cql_server_uri) Connect() (Connection, error) {
	session, err := server.cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cql_connection{session: session}, nil
}

// A suite with CQL tests
type cql_test_suite struct {
	description string
	path        string
	name        string
	tests       []cql_test_file
	server      Server
}

func (suite *cql_test_suite) FindTests(suite_path string, patterns []string) error {
	suite.path = suite_path
	suite.name = path.Base(suite.path)

	files, err := filepath.Glob(path.Join(suite.path, "*.test.cql"))
	if err != nil {
		return err
	}
	// @todo: say nothing if there are no tests
	fmt.Printf("Collecting tests in %-14s (Found %3d tests): %.26s\n",
		fmt.Sprintf("'%.12s'", suite.name), len(files), suite.description)
	for _, file := range files {
		// @todo: filter by pattern here
		suite.tests = append(suite.tests, cql_test_file{path: file, name: path.Base(file)})
	}
	return nil
}

func (suite *cql_test_suite) IsEmpty() bool {
	return len(suite.tests) == 0
}

func (suite *cql_test_suite) PrepareLane(lane *Lane) {
	suite.server = &cql_server_uri{uri: "127.0.0.1"}
	suite.server.Start(lane)
}

func (suite *cql_test_suite) Run(force bool) error {
	c, err := suite.server.Connect()
	if err != nil {
		// 'force' affects .result/reject mismatch,
		// but not harness failures
		return err
	}
	defer c.Close()
	for _, test := range suite.tests {
		err := test.Run(force, c)
		if err != nil {
			// @todo nice progress report
			// 'force' doesn't affect internal errors
			return err
		}
		// @todo nice output, nice progress report
		fmt.Println("OK")
	}
	return nil
}

type cql_test_file struct {
	name string
	path string
}

// Open a file and read it line-by-line, splitting into test cases.
func (test *cql_test_file) Run(force bool, c Connection) error {

	// Open input file
	file, err := os.Open(test.path)
	if err != nil {
		return err
	}

	stream := bufio.NewScanner(file)

	// @todo: fail if found no tests
	for stream.Scan() {
		line := stream.Text()
		if _, err := c.Execute(line); err != nil {
			// @todo: access denied, lost connection
			// should not trigger test failure with 'force'
			return err
		}
		// append output to the output file
	}
	return nil
}

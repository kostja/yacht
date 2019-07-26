package main

import "log"
import "os"
import "os/signal"
import "fmt"
import "bufio"

import "time"

import "reflect"
import "bytes"
import "path"
import "path/filepath"
import "strings"
import "regexp"
import "io/ioutil"
import "github.com/ansel1/merry"
import "github.com/spf13/pflag"
import "github.com/spf13/viper"
import "github.com/gocql/gocql"
import "github.com/udhos/equalfile"
import "github.com/pmezard/go-difflib/difflib"
import "github.com/olekukonko/tablewriter"
import "github.com/fatih/color"

// A directory with tests
type TestSuite interface {
	FindTests(path string, patterns []string) error
	IsEmpty() bool
	PrepareLane(*Lane)
	RunSuite(force bool) (int, error)
	FailedTests() []string
}

// A single test
type TestFile interface {
	PrepareLane(lane *Lane)
	RunTest(force bool, c Connection) (string, error)
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

type ColoredSprintf func(format string, a ...interface{}) string

func CreateColor(attributes ...color.Attribute) ColoredSprintf {
	var c = color.New()
	for _, attribute := range attributes {
		c.Add(attribute)
	}
	return func(format string, a ...interface{}) string {
		return c.Sprintf(format, a...)
	}
}

// A color palette for highlighting harness output
type Palette struct {
	// Passed test
	pass ColoredSprintf
	// Failed test
	fail ColoredSprintf
	// New tes
	new_ ColoredSprintf
	// Skipped or disabled test
	skip ColoredSprintf
	// Path
	path ColoredSprintf
	// A warning or important information
	warn ColoredSprintf
	// Critical error
	crit ColoredSprintf
	// Normal output - this solely for documenting purposes,
	// don't use, use fmt.*print* instead
	info ColoredSprintf
}

var palette = Palette{
	pass: CreateColor(color.FgGreen),
	fail: CreateColor(color.FgRed),
	new_: CreateColor(color.FgBlue),
	skip: CreateColor(color.Faint),
	path: CreateColor(color.Bold),
	warn: CreateColor(color.FgYellow),
	crit: CreateColor(color.FgRed),
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
		fmt.Printf("Using configuration file %s\n",
			palette.path(env_cfg.ConfigFileUsed()))
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

func PrintSuiteBeginBlurb() {
	fmt.Printf("%s\n", strings.Repeat("=", 80))
	fmt.Printf("LANE ")
	fmt.Printf("%-46s", "TEST")
	fmt.Printf("%-14s", "OPTIONS")
	fmt.Printf("RESULT\n")
	fmt.Printf("%s\n", strings.Repeat("-", 75))
}

func PrintSuiteEndBlurb() {
	fmt.Printf("%s\n", strings.Repeat("-", 75))
}

func PrintTestBlurb(lane string, name string, options string, result string) {
	fmt.Printf("%4s %-46s %-14s %-8s\n", lane, name, options, result)
}

func (yacht *Yacht) Run() int {

	yacht.lane.Init("1", yacht.env.vardir)
	var rc int = 0

	yacht.findSuites()

	var failed []string

	for _, suite := range yacht.suites {
		// Clear the lane between test suites
		// Note, it's done before the suite is started,
		// not after, to preserve important artefacts
		// between runs
		yacht.lane.CleanupBeforeNextSuite()
		// @todo: multiple lanes to run tests in parallel
		PrintSuiteBeginBlurb()
		suite.PrepareLane(&yacht.lane)
		if suite_rc, err := suite.RunSuite(yacht.env.force); err != nil {
			log.Printf("yacht failure: %+v", err)
			return 1
		} else {
			rc |= suite_rc
			failed = append(failed, suite.FailedTests()...)
			PrintSuiteEndBlurb()
		}
	}
	if yacht.env.force == true && len(failed) != 0 {
		fmt.Printf("Not all tests executeed successfully: %v\n", failed)
	}
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

var CassandraErrorMap = map[int]string{
	0x0000: "Server error (0x0000)",
	0x000A: "Protocol error (0x000A)",
	0x0100: "Bad credentials (0x0100)",
	0x1000: "Unavailable (0x1000)",
	0x1001: "Overloaded (0x1001)",
	0x1002: "Bootstrapping (0x1002)",
	0x1003: "Truncate error (0x1003)",
	0x1100: "Write timeout (0x1100)",
	0x1200: "Read timeout (0x1200)",
	0x1300: "Read failure (0x1300)",
	0x1400: "Function failure (0x1400)",
	0x1500: "Write failure (0x1500)",
	0x1600: "CDC write failure (0x1600)",
	0x2000: "Syntax error (0x2000)",
	0x2100: "Unauthorized (0x2100)",
	0x2200: "Invalid (0x2200)",
	0x2300: "Config error (0x2300)",
	0x2400: "Already exists (0x2400)",
	0x2500: "Unprepared (0x2500)",
}

// Result of execution of a CQL statement
type CQLResult struct {
	status   string
	code     string
	message  string
	warnings []string
	names    []string
	types    []string
	rows     [][]string
}

func (result *CQLResult) String() string {
	var offset = "  "
	buf := new(bytes.Buffer)
	if result.status != "OK" {
		fmt.Fprintf(buf, "%s%7s: %s\n", offset, "status", result.status)
		fmt.Fprintf(buf, "%s%7s: %s\n", offset, "code", result.code)
		fmt.Fprintf(buf, "%s%7s: %s\n", offset, "message", result.message)
		return string(buf.Bytes())
	}
	if len(result.warnings) != 0 {
		fmt.Fprintf(buf, "%s%8s: %+v\n", offset, "warnings", result.warnings)
	}
	if len(result.rows) != 0 {
		fmt.Fprint(buf, offset)
		table := tablewriter.NewWriter(buf)
		table.SetHeader(result.names)
		// Shift all rows by offset
		table.SetNewLine("\n" + offset)
		for _, v := range result.rows {
			table.Append(v)
		}
		table.Render() // Send output
		// Trim last offset
		buf.Truncate(buf.Len() - len(offset))
	} else {
		fmt.Fprint(buf, "  OK\n")
	}
	return string(buf.Bytes())
}

func (c *cql_connection) Execute(cql string) (string, error) {

	var result CQLResult

	query := c.session.Query(cql)
	err := query.Exec()

	if err == nil {
		// todo: serialize results
		result.status = "OK"

		iter := query.Iter()
		result.warnings = iter.Warnings()
		for _, column := range iter.Columns() {
			result.names = append(result.names, column.Name)
			result.types = append(result.types, column.TypeInfo.Type().String())
		}
		row, _ := iter.RowData()
		for {
			if !iter.Scan(row.Values...) {
				break
			}
			strrow := make([]string, len(row.Values))
			for i, v := range row.Values {
				strrow[i] = fmt.Sprint(reflect.Indirect(reflect.ValueOf(v)))
			}
			result.rows = append(result.rows, strrow)
		}
	} else {
		switch e := err.(type) {
		case gocql.RequestError:
			result.status = "ERROR"
			result.code = CassandraErrorMap[e.Code()]
			result.message = fmt.Sprintf("%.80s", strings.Split(e.Message(), "\n")[0])
		default:
			// Transport error or internal driver error, propagate up
			return "", merry.Wrap(err)
		}
	}
	return result.String(), nil
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
	server.cluster.Timeout, _ = time.ParseDuration("30s")
	// Create an administrative session to prepare
	// administrative server for testing
	session, err := server.cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	artefact := cql_server_uri_artefact{session: session}
	// Cleanup before running the suit
	artefact.Remove()
	// Create a keyspace for testing
	err = session.Query(`CREATE KEYSPACE IF NOT EXISTS yacht WITH REPLICATION =
{ 'class': 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true`).Exec()
	if err != nil {
		return merry.Wrap(err)
	}
	server.cluster.Keyspace = "yacht"
	lane.AddSuiteArtefact(&artefact)
	return nil
}

func (server *cql_server_uri) Connect() (Connection, error) {
	session, err := server.cluster.CreateSession()
	if err != nil {
		return nil, merry.Wrap(err)
	}
	//	session.SetConsistency(gocql.Any)
	return &cql_connection{session: session}, nil
}

// A suite with CQL tests
type cql_test_suite struct {
	description string
	path        string
	name        string
	tests       []cql_test_file
	failed      []string
	lane        *Lane
	server      Server
}

func (suite *cql_test_suite) FailedTests() []string {
	return suite.failed
}

func (suite *cql_test_suite) FindTests(suite_path string, patterns []string) error {
	suite.path = suite_path
	suite.name = path.Base(suite.path)

	files, err := filepath.Glob(path.Join(suite.path, "*.test.cql"))
	if err != nil {
		return merry.Wrap(err)
	}
	// @todo: say nothing if there are no tests
	fmt.Printf("Collecting tests in %-14s ", fmt.Sprintf("'%.12s'", suite.name))
	for _, file := range files {
		for _, pattern := range patterns {
			if strings.Contains(file, pattern) {
				test := cql_test_file{
					path: file,
					name: path.Base(file),
				}
				suite.tests = append(suite.tests, test)
			}
		}
	}
	fmt.Printf("(Found %3d tests): %.26s\n", len(suite.tests), suite.description)
	return nil
}

func (suite *cql_test_suite) IsEmpty() bool {
	return len(suite.tests) == 0
}

func (suite *cql_test_suite) PrepareLane(lane *Lane) {
	suite.lane = lane
	suite.server = &cql_server_uri{uri: "127.0.0.1"}
	suite.server.Start(lane)
	for i, _ := range suite.tests {
		suite.tests[i].PrepareLane(lane)
	}
}

func (suite *cql_test_suite) RunSuite(force bool) (int, error) {
	c, err := suite.server.Connect()
	var suite_rc int = 0
	if err != nil {
		// 'force' affects .result/reject mismatch,
		// but not harness or infrastructure failures
		return 0, merry.Wrap(err)
	}
	defer c.Close()
	for _, test := range suite.tests {
		test_rc, err := test.RunTest(force, c)
		if err != nil {
			return 0, merry.Wrap(err)
		}
		PrintTestBlurb(suite.lane.id, test.name, "", test_rc)
		if test_rc == "FAIL" {
			test.PrintUniDiff()
			suite_rc = 1
			suite.failed = append(suite.failed, path.Join(suite.name, test.name))
			if force == false {
				break
			}
		}
	}
	return suite_rc, nil
}

type cql_test_file struct {
	// Temp name
	name string
	// Path to test case
	path string
	// Where to store temporary state
	vardir string
	// Path to a temporary output file in vardir
	tmp string
	// Path to result file in srcdir
	result string
	// Path to reject file in srcdir
	reject string
	// True if the test output is the same as pre-recorded one
	isEqualResult bool
	// True if the the pre-recorded output did not exist
	isNew bool
}

// matches comments and whitespace
var commentRE = regexp.MustCompile(`^\s*((--|\/\/).*)?$`)
var testCQLRE = regexp.MustCompile(`test\.cql$`)
var resultRE = regexp.MustCompile(`result$`)

func (test *cql_test_file) PrepareLane(lane *Lane) {
	test.vardir = lane.Dir()
	test.tmp = path.Join(test.vardir, testCQLRE.ReplaceAllString(test.name, `result`))
	test.result = testCQLRE.ReplaceAllString(test.path, `result`)
	test.reject = resultRE.ReplaceAllString(test.result, `reject`)
}

// Open a file and read it line-by-line, splitting into test cases.
func (test *cql_test_file) RunTest(force bool, c Connection) (string, error) {

	// Open input file
	test_file, err := os.Open(test.path)
	if err != nil {
		return "", merry.Wrap(err)
	}
	defer test_file.Close()

	// Open a temporary output file
	tmp_file, err := os.OpenFile(test.tmp,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", merry.Prepend(err, test.tmp)
	}
	defer tmp_file.Close()

	input := bufio.NewScanner(test_file)
	output := bufio.NewWriter(tmp_file)

	// @todo: fail if found no test cases in a file
	for input.Scan() {
		line := input.Text()
		fmt.Fprintln(output, line)
		if commentRE.MatchString(line) {
			continue
		}
		if response, err := c.Execute(line); err == nil {
			fmt.Fprint(output, response)
		} else {
			// @todo: access denied, lost connection
			// should not trigger test failure with 'force'
			return "", merry.Wrap(err)
		}
	}
	output.Flush()

	if _, err := os.Stat(test.result); err == nil {
		// Compare output
		test.isEqualResult, _ = equalfile.New(nil, equalfile.Options{}).CompareFile(
			test.tmp, test.result)
	} else if os.IsNotExist(err) {
		test.isNew = true
	} else {
		return "", merry.Wrap(err)
	}

	if test.isEqualResult {
		os.Remove(test.tmp)
		return "OK", nil
	}
	if test.isNew {
		// Create a result file when running for the first time
		if err := os.Rename(test.tmp, test.result); err != nil {
			return "", merry.Wrap(err)
		}
		return "NEW", nil
	}
	if err := os.Rename(test.tmp, test.reject); err != nil {
		return "", merry.Wrap(err)
	}
	// Result content mismatch
	return "FAIL", nil
}

func (test *cql_test_file) PrintUniDiff() {

	var result, reject []byte
	var err error

	if result, err = ioutil.ReadFile(test.result); err != nil {
		return
	}
	if reject, err = ioutil.ReadFile(test.reject); err != nil {
		return
	}
	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(result)),
		B:        difflib.SplitLines(string(reject)),
		FromFile: test.result,
		ToFile:   test.reject,
		Context:  3,
	}
	if text, err := difflib.GetUnifiedDiffString(diff); err == nil {
		fmt.Printf(text)
	}
}

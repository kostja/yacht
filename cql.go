package main

import "os"
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
import "github.com/gocql/gocql"
import "github.com/udhos/equalfile"
import "github.com/pmezard/go-difflib/difflib"
import "github.com/olekukonko/tablewriter"

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

func (server *cql_server_uri) ModeName() string {
	return "uri"
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
		return nil, merry.Prepend(err, "when connecting to '"+server.uri+"'")
	}
	//	session.SetConsistency(gocql.Any)
	return &cql_connection{session: session}, nil
}

// cql_server - a standalone scylla server

type cql_server struct {
	cql_server_uri
}

func (server *cql_server) ModeName() string {
	return "single"
}

func (server *cql_server) Start(lane *Lane) error {

	return server.cql_server_uri.Start(lane)
}

// A suite with CQL tests
type cql_test_suite struct {
	description string
	path        string
	name        string
	tests       []*cql_test_file
	servers     []Server
}

func (suite *cql_test_suite) AddMode(server Server) {
	suite.servers = append(suite.servers, server)
}

func (suite *cql_test_suite) Servers() []Server {
	return suite.servers
}

func (suite *cql_test_suite) FindTests(suite_path string, patterns []string) error {
	suite.path = suite_path
	suite.name = path.Base(suite.path)

	files, err := filepath.Glob(path.Join(suite.path, "*.test.cql"))
	if err != nil {
		return merry.Wrap(err)
	}
	fmt.Printf("Collecting tests in %-14s ", fmt.Sprintf("'%.12s'", suite.name))
	for _, file := range files {
		for _, pattern := range patterns {
			if strings.Contains(file, pattern) {
				test := cql_test_file{
					path: file,
				}
				test.Init()
				suite.tests = append(suite.tests, &test)
			}
		}
	}
	fmt.Printf("(Found %3d tests): %.26s\n", len(suite.tests), suite.description)
	return nil
}

func (suite *cql_test_suite) IsEmpty() bool {
	return len(suite.tests) == 0
}

func (suite *cql_test_suite) PrepareLane(lane *Lane, server Server) {
	server.Start(lane)
}

func (suite *cql_test_suite) RunSuite(force bool, lane *Lane, server Server) (int, error) {
	c, err := server.Connect()
	if err != nil {
		// 'force' affects .result/reject mismatch,
		// but not harness or infrastructure failures
		return 0, merry.Wrap(err)
	}
	defer c.Close()

	var suite_rc int = 0
	for _, test := range suite.tests {
		var full_name = path.Join(suite.name, test.name)
		test_rc, err := test.RunTest(force, c, lane)
		if err != nil {
			return 0, merry.Wrap(err)
		}
		PrintTestBlurb(lane.id, full_name, server.ModeName(), test_rc)
		if test_rc == "fail" {
			test.PrintUniDiff()
			suite_rc = 1
			// Record the failed test name
			lane.failed = append(lane.failed, full_name)
			if force == false {
				return suite_rc, nil
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
	// Path to result file in srcdir
	result string
	// Path to reject file in srcdir
	reject string
}

// matches comments and whitespace
var commentRE = regexp.MustCompile(`^\s*((--|\/\/).*)?$`)
var testCQLRE = regexp.MustCompile(`test\.cql$`)
var resultRE = regexp.MustCompile(`result$`)

func (test *cql_test_file) Init() {
	test.name = path.Base(test.path)
	test.result = testCQLRE.ReplaceAllString(test.path, `result`)
	test.reject = resultRE.ReplaceAllString(test.result, `reject`)
}

// Open a file and read it line-by-line, splitting into test cases.
func (test *cql_test_file) RunTest(force bool, c Connection, lane *Lane) (string, error) {

	tmpfile_name := path.Join(lane.Dir(), testCQLRE.ReplaceAllString(test.name, `result`))
	var isEqualResult bool
	var isNew bool
	// Open input file
	test_file, err := os.Open(test.path)
	if err != nil {
		return "", merry.Wrap(err)
	}
	defer test_file.Close()

	// Open a temporary output file
	tmp_file, err := os.OpenFile(tmpfile_name,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", merry.Prepend(err, tmpfile_name)
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
		isEqualResult, _ = equalfile.New(nil, equalfile.Options{}).CompareFile(
			tmpfile_name, test.result)
	} else if os.IsNotExist(err) {
		isNew = true
	} else {
		return "", merry.Wrap(err)
	}

	if isEqualResult {
		os.Remove(tmpfile_name)
		return "pass", nil
	}
	if isNew {
		// Create a result file when running for the first time
		if err := os.Rename(tmpfile_name, test.result); err != nil {
			return "", merry.Wrap(err)
		}
		return "new", nil
	}
	if err := os.Rename(tmpfile_name, test.reject); err != nil {
		return "", merry.Wrap(err)
	}
	// Result content mismatch
	return "fail", nil
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
		FromFile: palette.Path(test.result),
		ToFile:   palette.Path(test.reject),
		Context:  3,
	}
	if text, err := difflib.GetUnifiedDiffString(diff); err == nil {
		fmt.Printf(TrimAndColorizeDiff(text))
	}
}

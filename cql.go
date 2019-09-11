package main

import (
	"bufio"
	"fmt"
	"os"

	"io/ioutil"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ansel1/merry"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/udhos/equalfile"
)

// A suite with CQL tests
type CQLTestSuite struct {
	description string
	path        string
	name        string
	tests       []*CQLTestFile
	servers     []Server
}

func (suite *CQLTestSuite) AddMode(server Server) {
	suite.servers = append(suite.servers, server)
}

func (suite *CQLTestSuite) Servers() []Server {
	return suite.servers
}

func (suite *CQLTestSuite) FindTests(suite_path string, patterns []string) error {
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
				test := CQLTestFile{
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

func (suite *CQLTestSuite) IsEmpty() bool {
	return len(suite.tests) == 0
}

func (suite *CQLTestSuite) PrepareLane(lane *Lane, server Server) error {
	return server.Start(lane)
}

func (suite *CQLTestSuite) RunSuite(force bool, lane *Lane, server Server) (int, error) {
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

type CQLTestFile struct {
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
var delimiterRE = regexp.MustCompile(`;[[:space:]]*$`)
var testCQLRE = regexp.MustCompile(`test\.cql$`)
var resultRE = regexp.MustCompile(`result$`)

func (test *CQLTestFile) Init() {
	test.name = path.Base(test.path)
	test.result = testCQLRE.ReplaceAllString(test.path, `result`)
	test.reject = resultRE.ReplaceAllString(test.result, `reject`)
}

// Open a file and read it line-by-line, splitting into test cases.
func (test *CQLTestFile) RunTest(force bool, c Connection, lane *Lane) (string, error) {

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
		// Complete multiline statements, skipping comments
		if delimiterRE.MatchString(line) == false {
			multiline_statement := []string{line}
			for input.Scan() {
				line := input.Text()
				fmt.Fprintln(output, line)
				if commentRE.MatchString(line) {
					continue
				}
				multiline_statement = append(multiline_statement, line)
				if delimiterRE.MatchString(line) {
					break
				}
			}
			line = strings.Join(multiline_statement, "\n")
		}
		if response, err := c.Execute(line); err == nil {
			fmt.Fprint(output, response)
		} else {
			// @todo: access denied, lost connection
			// should not trigger test failure with 'force'
			output.Flush()
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

func (test *CQLTestFile) PrintUniDiff() {

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

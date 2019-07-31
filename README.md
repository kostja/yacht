Usage
-----

You need go 1.12 or later installed.

To build:

    $ make

To use:

Create a directory with a CQL file. The file must end with .test.cql. 
This directory is a test suite. Add a suite.yaml configuration file to the
directory, using [this example](https://github.com/kostja/yacht/blob/master/example.suite.yaml).

Add a configuration file `~/.yacht.yaml`, following [this example](https://github.com/kostja/yacht/blob/master/example.yacht.yaml), 
and point it at your Scylla binary, the directory with the test suite and a
directory for temporary test artefacts.

Run the suite:

    $ ./yacht

This will create a temporary directory, initialize a Scylla server instance
in it and run tests found in the source directory against this server. It
will print output as the testing progresses.

What this program does
----------------------

On start, it looks for test suites and test files, locates a Scylla binary,
installs it in the test directory, and runs tests against it.
The CQL queries are executed against 'yacht' keyspace, which is created
and destroyed automatically.
It can also be used to connect to an existing Scylla instance and run tests
against it, set suite type to 'uri' for that and provide 'uri' option
on the command line or in the config file.

## Definitions

### Test suite

A collection of tests in a single directory. The directory must provide a
suite configuration file (suite.json or suite.yaml). A suite file
contains suite description, type and running modes.
The only supported type is "cql", which means that each .test.cql file in
the suite is read linewise and sent to a Scylla server. Supported
running modes are 'single', i.e. run against a single server instance
which is installed automatically, and 'uri', which uses an existing
instance.

### Test

For CQL test suite, a single test file must have .test.cql extension.
The harness creates an accompanying file with .result extension on the first
test run. The .result file contains server output as produced by the tested
Scylla server. If there
is a result file, the harness will create a temporary file, store the server
output in it, and compare it with the pre-recorded .result file when the
test completes. The test is considered failed if executing any CQL statement
produced an error (e.g.  because the server crashed during execution) or server
output does not match one recorded in .result file. In the event of output
mismatch a file testname.reject is created, and first lines of the diff
between the two files are output. To update .result file with the new
output, simply overwrite it with the reject file:
    mv suitename/testname.re*

A single CQL test file is a collection of test cases. Each test starts with
-- yacht: test "test-case-name" line. The test case ends when the next test
case is found or end-of-file marker is read. Using test cases within a large
file allows to quickly navigate to a failed test, enable or disable
individual test cases.

Each test consists of files `*.test.cql`, `*.result`.
On first run (without `.result`) `.result` is generated from server output.
After `.test.cql` is executed and `.reject` file is created, `.reject` is
compared with `.result`. If the two files differ, 30 lines of the diff
are printed and .reject is left in the suite directory. Otherwise,
`.reject` file is deleted.

### Lane

Lane is a concept used to represent the runtime environment of a test. It
includes a temporary directory with temporary artefacts, created during a
test, such as server data and log file. The harness uses a short temporary
name to identify a lane. When the harness is srarted, a new directory with
lane name is created and a new server is initialized in this directory.
When the testing ends successfully, the lane is cleaned up, and the
directory is removed. Upon failure the lane directory is left intact. The
lane is not only about a directory, but is a container of all external
artefacts, such as used ports, running processes and so on. In future the
harness will support multiple lanes, for parallel testing.

### Server

A server is the testing subject. It can be a standalone server, created
outside the harness (`--uri` option) or a temporary instance created by the
harness automatically.

Patterns
--------

When yacht starts, it begins by looking for tests in all suites it can find.
If you want to run a specific test, write the test name as a command line
argument, for example:

    $ ./yacht lwt # runs only tests which have "lwt" in their name

The pattern matching simply looks for the specified substring in the test
name or suite name, so if you want to run only a test in a specific suite,
write:

    $ ./yacht cql/lwt # runs cql/lwt.test.cql  only

Multiple patterns can be provided, and each match counts independently.
This is useful to run multiple suites, multiple specific tests, or
a single test multiple times:

    $ ./yacht lwt lwt lwt lwt lwt # runs cql/lwt.test.cql 5 times

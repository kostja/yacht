Usage
-----

You need go 1.12 or later installed.

To build:

    $ make

To use:

Add a configuration file `~/.yacht.yaml`, an example configuration file
is provided in the project directory.

    $ ./yacht

will create a temporary directory, initialize a Scylla server instance in it
and run tests found in the source directory against this server. It will
print output as the testing progress.


What this program does
----------------------

On start, it connects to an existing Cassandra instance,
creates 'yacht' keyspace, and on shutdown deletes the keyspace.


## Definitions

### Test suite

A collection of tests in a single directory. The directory must provide a
suite configuration file (suite.json or suite.yaml). A suite file
contains suite description and type. The only supported type is "cql", which
means that each .test.cql file in the suite is read linewise and sent to a
Scylla server.

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

Each test consists of files `*.test(.lua|.sql|.py)?`, `*.result`, and may have
skip condition file `*.skipcond`.  On first run (without `.result`) `.result`
is generated from output.  Each run, in the beggining, `.skipcond` file is
executed. In the local env there's object `self`, that's `Test` object. If test
must be skipped - you must put `self.skip = 1` in this file. Next,
`.test(.lua|.py)?` is executed and file `.reject` is created, then `.reject` is
compared with `.result`. If something differs, then 15 last string of this diff
file are printed and `.reject` file is saving in the folder, where `.result`
file is. If not, then `.reject` is deleted.

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

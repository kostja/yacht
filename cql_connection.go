package main

import (
	"fmt"

	"bytes"
	"reflect"
	"strings"

	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	"github.com/olekukonko/tablewriter"
)

type CQLConnection struct {
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

func (c *CQLConnection) Execute(cql string) (string, error) {

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

func (c *CQLConnection) Close() {
	c.session.Close()
}
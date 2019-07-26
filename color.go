package main

import "strings"
import "regexp"
import "fmt"
import "github.com/fatih/color"

type ColoredSprintf func(format string, a ...interface{}) string

func CreateColor(attributes ...color.Attribute) ColoredSprintf {
	var c = color.New()
	for _, attribute := range attributes {
		c.Add(attribute)
	}
	return c.SprintfFunc()
}

// A color palette for highlighting harness output
type Palette struct {
	// Passed test
	Pass ColoredSprintf
	// Failed test
	Fail ColoredSprintf
	// New tes
	New ColoredSprintf
	// Skipped or disabled test
	Skip ColoredSprintf
	// Path
	Path ColoredSprintf
	// diff +
	DiffIn ColoredSprintf
	// diff -
	DiffOut ColoredSprintf
	// A warning or important information
	Warn ColoredSprintf
	// Critical error
	Crit ColoredSprintf
	// Normal output - this solely for documenting purposes,
	// don't use, use fmt.*print* instead
	Info ColoredSprintf
}

var palette = Palette{
	Pass:    CreateColor(color.FgGreen),
	Fail:    CreateColor(color.FgRed),
	New:     CreateColor(color.FgBlue),
	Skip:    CreateColor(color.Faint),
	Path:    CreateColor(color.Bold),
	DiffIn:  CreateColor(color.FgGreen),
	DiffOut: CreateColor(color.FgRed),
	Warn:    CreateColor(color.FgYellow),
	Crit:    CreateColor(color.FgRed),
}

func PrintSuiteBeginBlurb() {
	fmt.Printf("%s\n", strings.Repeat("=", 80))
	fmt.Printf("LANE ")
	fmt.Printf("%-52s", "TEST")
	fmt.Printf(palette.Warn("%-11s", "MODE"))
	fmt.Printf(palette.Pass("RESULT"))
	fmt.Printf("\n")
	fmt.Printf("%s\n", strings.Repeat("-", 75))
}

func PrintSuiteEndBlurb() {
	fmt.Printf("%s\n", strings.Repeat("-", 75))
}

func PrintTestBlurb(lane string, name string, mode string, result string) {
	switch result {
	case "pass":
		result = palette.Pass("[ %s ]", result)
	case "fail":
		result = palette.Fail("[ %s ]", result)
	case "new":
		result = palette.New("[ %s  ]", result)
	default:
		result = palette.Skip(result)
	}
	mode = palette.Warn("%.12s", mode)
	fmt.Printf("[%3s] %-50s %-18s %-8s\n", lane, name, mode, result)
}

var inRE = regexp.MustCompile(`^\+.*$`)
var outRE = regexp.MustCompile(`^\-.*$`)

func TrimAndColorizeDiff(diff string) string {
	var lines = strings.Split(diff, "\n")
	if len(lines) > 60 {
		lines = lines[:60]
	}
	// Skip the first two lines of the diff
	for i := 2; i < len(lines); i++ {
		if inRE.MatchString(lines[i]) {
			lines[i] = palette.DiffIn("%s", lines[i])
		} else if outRE.MatchString(lines[i]) {
			lines[i] = palette.DiffOut("%s", lines[i])
		}
	}
	return strings.Join(lines, "\n")
}

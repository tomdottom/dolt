// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"regexp"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

const (
	numLinesParam =  "number"
	logOneLineFlag = "oneline"
	logDiffSummary = "diff"
)

var logDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show commit logs`,
	LongDesc: `Shows the commit logs

The command takes options to control what is shown and how.`,
	Synopsis: []string{
		`[-n {{.LessThan}}num_commits{{.GreaterThan}}] [{{.LessThan}}commit{{.GreaterThan}}]`,
	},
}

type commitLoggerFunc func(*doltdb.DoltDB, *doltdb.CommitMeta, []hash.Hash, hash.Hash) error

func logToStdOutFunc(_ *doltdb.DoltDB, cm *doltdb.CommitMeta, parentHashes []hash.Hash, ch hash.Hash) error {
	cli.Println(color.YellowString("commit %s", ch.String()))

	if len(parentHashes) > 1 {
		printMerge(parentHashes)
	}

	printAuthor(cm)
	printDate(cm)
	printDesc(cm)
	return nil
}

func logOnelineToStdOutFunc(_ *doltdb.DoltDB, cm *doltdb.CommitMeta, _ []hash.Hash, ch hash.Hash) error {
	ych := color.YellowString(ch.String())
	formattedDesc := regexp.MustCompile(`\r?\n`).ReplaceAllString(cm.Description, " ")
	cli.Println(fmt.Sprintf("%s %s", ych, formattedDesc))
	return nil
}

func logSchemaDiffToStdOutFunc(ddb *doltdb.DoltDB, cm *doltdb.CommitMeta, parentHashes []hash.Hash, ch hash.Hash) error {
	switch len(parentHashes) {
	case 0:
		_ = logOnelineToStdOutFunc(ddb, cm, parentHashes, ch)
	case 1:
		twoDot := color.YellowString(fmt.Sprintf("%s..%s", ch.String(), parentHashes[0].String()))
		formattedDesc := regexp.MustCompile(`\r?\n`).ReplaceAllString(cm.Description, " ")
		cli.Println(fmt.Sprintf("%s %s", twoDot, formattedDesc))
		err := logSchemaDiff(ddb, ch, parentHashes[0])
		if err != nil {
			return err
		}
	case 2:
		twoDot := color.HiYellowString(fmt.Sprintf("%s..%s", ch.String(), parentHashes[0].String()))
		formattedDesc := regexp.MustCompile(`\r?\n`).ReplaceAllString(cm.Description, " ")
		cli.Println(fmt.Sprintf("%s %s", twoDot, formattedDesc))
		err := logSchemaDiff(ddb, ch, parentHashes[0])
		if err != nil {
			return err
		}

		twoDot = color.HiYellowString(fmt.Sprintf("%s..%s", ch.String(), parentHashes[1].String()))
		formattedDesc = regexp.MustCompile(`\r?\n`).ReplaceAllString(cm.Description, " ")
		cli.Println(fmt.Sprintf("%s %s", twoDot, formattedDesc))
		err = logSchemaDiff(ddb, ch, parentHashes[1])
		if err != nil {
			return err
		}
	default:
		cli.PrintErrln("Logging octopus merges is not yet supported")
	}
	return nil
}

func printMerge(hashes []hash.Hash) {
	cli.Print("Merge:")
	for _, h := range hashes {
		cli.Print(" " + h.String())
	}
	cli.Println()
}

func printAuthor(cm *doltdb.CommitMeta) {
	cli.Printf("Author: %s <%s>\n", cm.Name, cm.Email)
}

func printDate(cm *doltdb.CommitMeta) {
	timeStr := cm.FormatTS()
	cli.Println("Date:  ", timeStr)
}

func printDesc(cm *doltdb.CommitMeta) {
	formattedDesc := "\n\t" + strings.Replace(cm.Description, "\n", "\n\t", -1) + "\n"
	cli.Println(formattedDesc)
}

type LogCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LogCmd) Name() string {
	return "log"
}

// Description returns a description of the command
func (cmd LogCmd) Description() string {
	return "Show commit logs."
}

// EventType returns the type of the event to log
func (cmd LogCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LOG
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd LogCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := createLogArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, logDocs, ap))
}

func createLogArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsInt(numLinesParam, "n", "num_commits", "Limit the number of commits to output")
	ap.SupportsFlag(logOneLineFlag, "", "Summarize commit log to one line per commit")
	ap.SupportsFlag(logDiffSummary, "", "Print schema diffs at each commit")
	return ap
}

// Exec executes the command
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	return logWithLoggerFunc(ctx, commandStr, args, dEnv)
}

func logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := createLogArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, logDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() > 1 {
		usage()
		return 1
	}

	cs, err := parseCommitSpec(dEnv, apr)
	if err != nil {
		cli.PrintErr(err)
		return 1
	}

	var loggerFunc commitLoggerFunc
	switch {
	case apr.Contains(logOneLineFlag):
		loggerFunc = logOnelineToStdOutFunc
	case apr.Contains(logDiffSummary):
		loggerFunc = logSchemaDiffToStdOutFunc
	default:
		loggerFunc = logToStdOutFunc
	}

	numLines := apr.GetIntOrDefault(numLinesParam, -1)
	return logCommits(ctx, dEnv, cs, loggerFunc, numLines)
}

func parseCommitSpec(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*doltdb.CommitSpec, error) {
	if apr.NArg() == 0 || apr.Arg(0) == "--" {
		return dEnv.RepoState.CWBHeadSpec(), nil
	}

	comSpecStr := apr.Arg(0)
	cs, err := doltdb.NewCommitSpec(comSpecStr, dEnv.RepoState.CWBHeadRef().String())

	if err != nil {
		return nil, fmt.Errorf("invalid commit %s\n", comSpecStr)
	}

	return cs, nil
}

func logCommits(ctx context.Context, dEnv *env.DoltEnv, cs *doltdb.CommitSpec, loggerFunc commitLoggerFunc, numLines int) int {
	commit, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
		return 1
	}

	h, err := commit.HashOf()

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit hash"))
		return 1
	}

	commits, err := commitwalk.GetTopNTopoOrderedCommits(ctx, dEnv.DoltDB, h, numLines)

	if err != nil {
		cli.PrintErrln("Error retrieving commit.")
		return 1
	}

	for _, comm := range commits {
		meta, err := comm.GetCommitMeta()

		if err != nil {
			cli.PrintErrln("error: failed to get commit metadata")
			return 1
		}

		pHashes, err := comm.ParentHashes(ctx)

		if err != nil {
			cli.PrintErrln("error: failed to get parent hashes")
			return 1
		}

		cmHash, err := comm.HashOf()

		if err != nil {
			cli.PrintErrln("error: failed to get commit hash")
			return 1
		}

		err = loggerFunc(dEnv.DoltDB, meta, pHashes, cmHash)

		if err != nil {
			cli.PrintErrln(err.Error())
			return 1
		}
	}

	return 0
}

func logSchemaDiff(ddb *doltdb.DoltDB, commitHash, parentHash hash.Hash) error {
	ctx := context.Background()

	cs, err := doltdb.NewCommitSpec(commitHash.String(), "")

	if err != nil {
		return err
	}

	cm, err := ddb.Resolve(ctx, cs)

	if err != nil {
		return err
	}

	root, err := cm.GetRootValue()

	if err != nil {
		return err
	}

	pcs, err := doltdb.NewCommitSpec(parentHash.String(), "")

	if err != nil {
		return err
	}

	pcm, err := ddb.Resolve(ctx, pcs)

	if err != nil {
		return err
	}

	parentRoot, err := pcm.GetRootValue()

	if err != nil {
		return err
	}

	allTableNames, err := doltdb.UnionTableNames(ctx, root, parentRoot)

	if err != nil {
		return err
	}

	var sch schema.Schema
	var parentSch schema.Schema
	for _, tn := range allTableNames {
		sch = schema.EmptySchema
		parentSch = schema.EmptySchema

		t, found, err := root.GetTable(ctx, tn)

		if err != nil {
			return err
		}

		if found {
			sch, err = t.GetSchema(ctx)

			if err != nil {
				return err
			}
		}

		pt, found, err := parentRoot.GetTable(ctx, tn)

		if err != nil {
			return err
		}

		if found {
			parentSch, err = pt.GetSchema(ctx)

			if err != nil {
				return err
			}
		}

		verr := diffSchemas(tn, parentSch, sch, &diffArgs{diffOutput: TabularDiffOutput, diffParts: SchemaOnlyDiff})

		if verr != nil {
			return verr
		}
	}
	return nil
}

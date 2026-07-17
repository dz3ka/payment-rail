// Command conduitctl is the operator CLI: submit test payments, replay
// webhooks, trigger reconciliation, and inspect the ledger.
//
// M0: prints version and usage only. Real subcommands are added alongside the
// services they drive (M1+).
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dz3ka/payment-rail/internal/version"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = usage
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		return
	}

	// M0: no subcommands wired yet.
	usage()
	os.Exit(2)
}

func usage() {
	fmt.Fprintf(os.Stderr, `conduitctl — Conduit operator CLI (%s)

Usage:
  conduitctl [flags] <command>

Flags:
  --version    print version and exit

Commands:
  (none yet — subcommands land with milestones M1+)
`, version.String())
}

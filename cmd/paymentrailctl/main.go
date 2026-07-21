// Command paymentrailctl is the operator CLI: submit test payments, replay
// webhooks, trigger reconciliation, and inspect the ledger.
//
// Dispatch is deliberately a single switch on the first argument — no command
// registry — so the entrypoint stays a thin router and each subcommand owns its
// own flag parsing (see submit.go). --version keeps its M0 behavior.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dz3ka/payment-rail/internal/version"
)

func main() {
	// Subcommands come before flag parsing: "submit" owns its own FlagSet, so the
	// top-level flag package must not try to interpret its flags.
	if len(os.Args) > 1 && os.Args[1] == "submit" {
		if err := runSubmit(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "approve" {
		if err := runApprove(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "replay-webhook" {
		if err := runReplayWebhook(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = usage
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		return
	}

	usage()
	os.Exit(2)
}

func usage() {
	fmt.Fprintf(os.Stderr, `paymentrailctl — Payment Rail operator CLI (%s)

Usage:
  paymentrailctl [flags] <command>

Flags:
  --version    print version and exit

Commands:
  submit          sign and broadcast one payment (--to, --amount, [--asset], [--key-id], [--proposer])
  approve         approve and broadcast a parked four-eyes payment (approve <approval-id> --approver=<id>)
  replay-webhook  re-drive dead-lettered webhook deliveries for a subscription (--subscription-id <uuid>)
`, version.String())
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"opensearch-balanser/internal/api"
	"opensearch-balanser/internal/app"
	"opensearch-balanser/internal/collector"
	"opensearch-balanser/internal/config"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd := os.Args[1]
	switch cmd {
	case "audit":
		runAudit(os.Args[2:])
	case "plan":
		runPlan(os.Args[2:])
	case "dry-run":
		runDryRun(os.Args[2:])
	case "apply":
		runApply(os.Args[2:])
	case "force":
		runForce(os.Args[2:])
	case "emergency-drain":
		runEmergencyDrain(os.Args[2:])
	case "serve-api":
		runServeAPI(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func runAudit(args []string) {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "Path to YAML config")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	svc := mustService(*cfgPath)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bundle, err := svc.Audit(ctx)
	if err != nil {
		fatal(err)
	}
	printJSON(bundle)
}

func runPlan(args []string) {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "Path to YAML config")
	out := fs.String("out", "plan-bundle.json", "Output plan bundle JSON")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	svc := mustService(*cfgPath)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	bundle, err := svc.Plan(ctx)
	if err != nil {
		fatal(err)
	}
	if err := app.SaveBundle(*out, bundle); err != nil {
		fatal(err)
	}
	fmt.Printf("Plan bundle written: %s\n", *out)
}

func runDryRun(args []string) {
	fs := flag.NewFlagSet("dry-run", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "Path to YAML config")
	planFile := fs.String("plan", "plan-bundle.json", "Plan bundle JSON")
	out := fs.String("out", "plan-bundle.json", "Output updated plan bundle JSON")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	svc := mustService(*cfgPath)
	bundle, err := app.LoadBundle(*planFile)
	if err != nil {
		fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	updated, err := svc.DryRun(ctx, bundle)
	if err != nil {
		fatal(err)
	}
	if err := app.SaveBundle(*out, updated); err != nil {
		fatal(err)
	}
	fmt.Printf("Dry-run completed. Updated bundle written: %s\n", *out)
	if updated.Simulation != nil {
		fmt.Printf("Dry-run status: %s\n", updated.Simulation.Summary)
		if len(updated.Simulation.ConflictSummary) > 0 {
			fmt.Println("Conflict summary:")
			keys := make([]string, 0, len(updated.Simulation.ConflictSummary))
			for k := range updated.Simulation.ConflictSummary {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("  - %s: %d\n", k, updated.Simulation.ConflictSummary[k])
			}
		}
	}
}

func runApply(args []string) {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "Path to YAML config")
	planFile := fs.String("plan", "plan-bundle.json", "Plan bundle JSON with successful dry-run")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	svc := mustService(*cfgPath)
	bundle, err := app.LoadBundle(*planFile)
	if err != nil {
		fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	result, err := svc.Apply(ctx, bundle)
	if err != nil {
		fatal(err)
	}
	printJSON(result)
}

func runForce(args []string) {
	fs := flag.NewFlagSet("force", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "Path to YAML config")
	out := fs.String("out", "plan-bundle.json", "Output plan bundle JSON (plan+dry-run)")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	svc := mustService(*cfgPath)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	bundle, err := svc.Plan(ctx)
	if err != nil {
		fatal(err)
	}
	bundle, err = svc.DryRun(ctx, bundle)
	if err != nil {
		fatal(err)
	}
	if err := app.SaveBundle(*out, bundle); err != nil {
		fatal(err)
	}
	result, err := svc.ApplyForce(ctx, bundle)
	if err != nil {
		fatal(err)
	}
	printJSON(map[string]any{
		"mode":       "force",
		"bundle_out": *out,
		"result":     result,
	})
}

func runEmergencyDrain(args []string) {
	fs := flag.NewFlagSet("emergency-drain", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "Path to YAML config")
	nodeID := fs.String("node", "", "Node ID to drain")
	out := fs.String("out", "emergency-plan-bundle.json", "Output plan bundle JSON")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	if *nodeID == "" {
		fatal(fmt.Errorf("--node is required"))
	}
	svc := mustService(*cfgPath)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	bundle, err := svc.EmergencyPlan(ctx, *nodeID)
	if err != nil {
		fatal(err)
	}
	if err := app.SaveBundle(*out, bundle); err != nil {
		fatal(err)
	}
	fmt.Printf("Emergency drain plan written: %s\n", *out)
}

func runServeAPI(args []string) {
	fs := flag.NewFlagSet("serve-api", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "Path to YAML config")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	cfg, svc, rt := mustBootstrap(*cfgPath)
	if !cfg.API.Enabled {
		fatal(fmt.Errorf("api.enabled is false; set api.enabled=true in config to start HTTP API"))
	}
	server := api.NewServer(cfg.API.Listen, cfg.API.ReadTimeout, cfg.API.WriteTimeout, svc, rt)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	fmt.Printf("API server listening on %s\n", cfg.API.Listen)
	if err := server.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
		fatal(err)
	}
}

func mustService(cfgPath string) app.Service {
	_, svc, _ := mustBootstrap(cfgPath)
	return svc
}

func mustBootstrap(cfgPath string) (config.Config, app.Service, app.Runtime) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		fatal(err)
	}
	adapter, err := collector.NewOpenSearchAdapter(cfg.Cluster)
	if err != nil {
		fatal(err)
	}
	store, err := app.NewFileExecutionStore(cfg.Runtime.DataDir)
	if err != nil {
		fatal(err)
	}
	rt, err := app.NewRuntime(cfg)
	if err != nil {
		fatal(err)
	}
	svc := app.NewService(cfg, adapter, store, rt)
	return cfg, svc, rt
}

func usage() {
	fmt.Println("Usage: balancer <command> [flags]")
	fmt.Println("Commands: audit, plan, dry-run, apply, force, emergency-drain, serve-api")
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

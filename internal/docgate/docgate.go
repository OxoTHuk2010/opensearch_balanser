package docgate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Requirement struct {
	Path     string
	Headings []string
}

func DefaultRequirements() []Requirement {
	return []Requirement{
		{Path: "README.md", Headings: []string{"## Quick Start (CLI)", "## HTTP API", "## Safety Model"}},
		{Path: "API.md", Headings: []string{"## Endpoints", "## Request/Response Examples", "## Error Model"}},
		{Path: "OPERATIONS.md", Headings: []string{"## Runbook", "## Incident Response", "## Rollback and Stop"}},
		{Path: "SECURITY.md", Headings: []string{"## Access Model", "## Secrets", "## TLS and Transport Security"}},
		{Path: "CHANGELOG.md", Headings: []string{"## Unreleased"}},
		{Path: filepath.Join("docs", "adr", "0001-safety-first-gates.md"), Headings: []string{"# ADR 0001", "## Context", "## Decision", "## Consequences"}},
	}
}

func CheckRepo(root string) error {
	reqs := DefaultRequirements()
	var problems []string
	for _, req := range reqs {
		p := filepath.Join(root, req.Path)
		b, err := os.ReadFile(p)
		if err != nil {
			problems = append(problems, fmt.Sprintf("missing file: %s", req.Path))
			continue
		}
		content := string(b)
		for _, h := range req.Headings {
			if !strings.Contains(content, h) {
				problems = append(problems, fmt.Sprintf("%s missing heading: %s", req.Path, h))
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("documentation gate failed:\n- %s", strings.Join(problems, "\n- "))
	}
	return nil
}

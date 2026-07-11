<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=docs/ai-maintenance/entry.md,package.json -->
# Task Workflow

Start each task with the goal, non-goals, evidence, affected boundaries, risks, validation commands, and documentation updates.

1. Route from the entry and verify symbols with `rg`.
2. Reproduce bugs first; define input, output, errors, and compatibility for new features.
3. Implement the smallest complete change and update both sides of native contracts.
4. Add tests for requested behavior and error paths.
5. Run risk-appropriate checks, inspect the diff, and check resource cleanup.
6. Update living docs and user docs when behavior is user-visible.

Done means the requirement has executable evidence, checks are recorded, and a new AI can locate the change within two links from the entry.

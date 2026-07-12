# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Layout

**Single-context** — one domain: academic paper PDF pipeline.

## Before exploring, read these

- **`SKILL.md`** at the repo root — this is the canonical domain doc for this skill repo.
- **`README.md`** — overview, workflows, and usage.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in.

If these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill creates them lazily when terms or decisions actually get resolved.

## File structure

```
/
├── SKILL.md             ← entry point for Hermes agents, domain glossary
├── README.md            ← usage overview for humans
├── pyproject.toml       ← package config
├── scripts/             ← Python module files
└── docs/adr/            ← architectural decision records (lazy)
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `SKILL.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_

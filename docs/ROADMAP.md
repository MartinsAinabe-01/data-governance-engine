# 🗺 COURSE ROADMAP – EXECUTION PLAN

This roadmap translates the architecture vision into execution phases.

It is not aspirational.
It defines completion checkpoints.

---

# 📍 Current Status

Phase: 1  
Day: ~16  
Kernel: Functional  
GitHub CI/CD: Operational  
Contract Engine: Implemented  

Goal: Freeze Governance Core Before Cloud Expansion.

---

# 🔹 PHASE 1 – GOVERNANCE CORE (Local Python)

## Objective
Stabilize portable enforcement kernel.

## Must Complete

- [ ] Add metadata block parsing (non-enforced)
- [ ] Standardize compatibility report schema
- [ ] Freeze compatibility report JSON format
- [ ] Document enforcement lifecycle (done)
- [ ] Clean repository structure
- [ ] Remove experimental drift logic
- [ ] Tag v1.0.0 of kernel

## Completion Definition

Kernel:
- Validates version
- Detects field drift
- Classifies impact
- Blocks CI/CD correctly
- Produces stable JSON artifact

Once stable → Phase 1 complete.

No TTL.
No SLA.
No ODCS.
No cloud yet.

---

# 🔹 PHASE 2 – AWS INJECTION

## Objective
Prove kernel portability.

## Build

- S3 input
- Lambda or Glue ETL
- Inject governance check
- Store compatibility report in S3

## Learn

- IAM implications
- Cloud logging differences
- Event-driven enforcement
- Cost implications

Deliverable:
Cloud-based enforcement demo.

---

# 🔹 PHASE 3 – AZURE + METADATA

## Objective
Abstract toward enterprise integration.

## Compare

- Purview metadata model
- DataHub-like abstractions
- Lineage concepts
- Catalog mapping

Deliverable:
Metadata abstraction interface draft.

No direct production integration yet.

---

# 🔹 PHASE 4 – POWERBI GOVERNANCE DASHBOARD

## Objective
Turn enforcement artifacts into executive visibility.

## Build Dashboard Showing:

- Compatibility pass/fail rates
- Drift categories
- Impact tier trends
- Breaking change frequency
- Deployment blocks

Deliverable:
Executive-grade governance dashboard.

---

# 🔹 PHASE 5 – POST-COURSE EXPANSION

Only after Phase 1–4 complete.

Possible expansions:

- TTL enforcement logic
- SLA monitoring
- ODCS YAML compatibility
- Contract registry abstraction
- Cloud-native metadata publishing
- Multi-environment enforcement hooks

These are enhancements.
Not course requirements.

---

# 🚨 Scope Guardrail

Before building anything new:

Ask:

Does this move us closer to:

Portable enforcement kernel  
Injected into execution environments  
Publishing structured governance metadata  

If no → park it.

---

# 🎯 Final Course Completion Criteria

The course is complete when:

- Kernel runs locally
- Kernel runs in AWS
- Kernel publishes structured governance artifacts
- PowerBI dashboard consumes artifacts
- Architecture is documented and teachable

Everything else is expansion.
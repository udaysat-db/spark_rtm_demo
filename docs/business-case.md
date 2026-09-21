# Business Case: Real-Time Cold-Chain Alerting with Spark Real-Time Mode

This repository demonstrates sub-second operational alerting on streaming IoT
telemetry using **Apache Spark Real-Time Mode (RTM)**, applied to a grocery
cold-chain (refrigeration) failure-detection use case. The goal is to show that
low-latency stream processing — historically the domain of a separate streaming
engine — is now achievable with the standard Spark API and portable across
open-source Spark, cloud runtimes, and Databricks.

## Background

Grocery retailers operate large fleets of refrigerated and frozen units — cases,
reach-ins, and walk-ins — across thousands of stores. A national chain can run on
the order of tens of thousands of instrumented units. These assets already emit
telemetry: temperature, door state, compressor health, and power state. Frozen
goods must stay at or below 0°F and refrigerated goods at or below 40°F; sustained
excursions turn into discarded inventory and food-safety exceptions.

## The problem

**In operational alerting, an alert's value decays with time.** Detecting a
refrigeration anomaly is only useful if the alert arrives while intervention is
still possible. Three factors make latency a business problem rather than an
engineering nicety:

1. **Latency degrades under load, exactly when it matters most.** During a
   correlated event — a regional heat wave stressing hundreds of units at once —
   micro-batch pipelines accumulate backlog. Alerts bunch at trigger boundaries and
   arrive tens of seconds late and out of priority order, precisely when the
   operations team most needs timely, ranked signal.

2. **Automated remediation requires a closed loop.** Actions such as restarting a
   compressor, cutting over to backup cooling, or auto-dispatching a technician
   depend on sub-second, per-record alerting. At multi-second latency a human is
   forced back into the loop and the reaction window shrinks.

3. **Prioritization depends on per-record processing.** Sub-second, per-event
   handling lets teams rank and dispatch to the highest-value, most-critical units
   first. Batched delivery arrives as an undifferentiated set, degrading triage.

The net business impact is not a single melted freezer — spoilage plays out over
minutes to hours. It is that during the correlated event that matters most, a slow
pipeline falls behind and the organization loses the ability to intervene and
prioritize across the fleet.

## The status quo and its cost

Teams that already run Spark for batch and near-real-time ETL have typically
reached for a **second, separate streaming engine** when they needed sub-second
latency. That introduces a parallel technology stack: additional skills to hire and
maintain, separate operational tooling, and a forked codebase where the same
business rules must be implemented and kept in sync in two places.

## The solution: Spark Real-Time Mode

**Apache Spark 4.1 introduces Real-Time Mode**, a continuous execution mode for
Structured Streaming that processes records as they arrive rather than at
micro-batch boundaries, achieving millisecond-scale end-to-end latency for the
supported workload shape. The alerting logic in this repository — stateless rules
plus a broadcast enrichment join — sits squarely within RTM's supported operations.

Because Real-Time Mode is part of **open-source Apache Spark**, the same code runs
on OSS Spark, cloud-managed Spark runtimes, and Databricks. The business rules are a
single, testable transform reused unchanged across every runtime and across both
micro-batch and real-time execution.

## Business value

- **Meet operational SLAs** for alerting (sub-second) that were previously out of
  reach for a Spark-based pipeline.
- **Retire the second engine.** One engine, one API, one codebase covering both
  batch and sub-second streaming reduces cost, operational risk, and skills sprawl.
- **Portability.** No vendor lock-in on the core capability: the same pipeline runs
  wherever Spark runs.
- **Consistency.** Identical business rules across execution modes and runtimes mean
  results are directly comparable and behavior does not drift between environments.

## How latency maps to business outcomes

| End-to-end latency | Operational meaning |
|---|---|
| ≤ 250 ms | Automated intervention window (dispatch, cut-over, reroute) |
| ≤ 1 s | Good human-operator experience |
| ≤ 5 s | Visible, but may already be too late for high-value assets |
| > 5 s | Stale — the intervention window has passed |

## What this repository demonstrates

- A synthetic IoT producer publishing freezer telemetry to Kafka.
- The same alerting logic executed as **micro-batch** and as **Real-Time Mode**, so
  latency and throughput can be compared side by side under normal and burst load.
- The same code running across multiple engines — open-source Spark, a cloud
  runtime, and Databricks — to show portability of the capability.
- A live analytics view that surfaces latency distributions and alert volume in
  real time, mapped to the business buckets above.

## Scope and assumptions

- Kafka and the compute/workspace are **bring-your-own**; this repository contains
  no infrastructure-provisioning code. Connection details are supplied via
  configuration and secrets are provided out of band.
- Figures used for illustration (fleet size, per-unit value) are representative and
  intended to be mapped to a reader's own environment, not drawn from a specific
  customer.
- Real-Time Mode requires continuously running compute; this repository demonstrates
  the capability and its portability, not a cost comparison.

# Home Dashboard — Complete User Manual

## Overview

The Home Dashboard is the central hub of the Code Analyzer platform. It provides a consolidated view of all analysis tools, quick submission capabilities, real-time queue monitoring, progress tracking, and AI session oversight. All interaction begins here.

The application exposes four web UIs accessible from the dashboard:

| URL | Tool |
|-----|------|
| `/` | Home Dashboard (this page) |
| `/jar/` | JAR / WAR Analyzer |
| `/parser/` | PL/SQL Parse Analyzer |
| `/plsql/` | Legacy PL/SQL Analyzer |

The layout is organized top to bottom:

1. Two tool cards
2. Quick Submit panel
3. Analysis Queue bar
4. Progress Report panels (side by side)
5. Claude Sessions table

---

## Tool Cards

Two clickable cards appear at the top of the dashboard, one for each analyzer.

### JAR / WAR Analyzer

- **Navigation:** Click to open `/jar/`.
- **Live statistics displayed on the card:**
  - Total JARs / WARs analyzed
  - Entry points discovered (REST, AMQP, Kafka, WebSocket, Scheduled, Event-driven)
  - Classes cataloged
  - AI Enriched count (number of entry points enriched by Claude)

### PL/SQL Parse Analyzer

- **Navigation:** Click to open `/parser/`.
- **Live statistics displayed on the card:**
  - Total analyses completed
  - Procedures parsed
  - Tables referenced
  - Lines of Code processed

Statistics update each time the dashboard loads or is refreshed.

---

## Quick Submit Panel

The Quick Submit panel provides two tabs for submitting work directly from the dashboard without navigating to an individual tool screen.

### PL/SQL Parser Tab

Use this tab to queue PL/SQL procedure parsing jobs.

- **Input:** A multi-line textarea. Enter one procedure per line.
- **Supported name formats:**
  - `PKG.PROC` — package-qualified procedure
  - `SCHEMA.PKG.PROC` — fully qualified with schema
  - `PROCEDURE_NAME` — standalone procedure name
- **Submit to Queue button:** Each line in the textarea becomes a separate queue job.
- **Duplicate handling:** If a procedure name already exists in the system, the existing record is automatically archived with a timestamp before the new job is queued.
- **Result notification:** A banner appears after submission.
  - Green on success, showing how many jobs were queued.
  - Red on error, showing the failure reason.
  - The notification auto-hides after 6 seconds.

### JAR Analyzer Tab

Use this tab to queue JAR file analysis jobs.

- **Input:** A text field for the local JAR file path.
  - Example: `C:\path\to\app-0.0.1-SNAPSHOT.jar`
  - The server reads the file directly from the local filesystem at the specified path.
- **Analysis mode:** Radio button selection.
  - **Static** — Structural analysis only (endpoints, classes, methods).
  - **Claude** — AI-powered enrichment analysis.
- **Submit to Queue button:** Submits the JAR for processing.
- **Result notification:** Shows the queued JAR name and the assigned job ID.

---

## Analysis Queue

A summary bar appears below the Quick Submit panel. It displays a one-line status such as:

> 1 running, 2 queued, 5 done

If a job is currently running, the bar also shows the job name and its progress percentage.

### Expanding the Queue Modal

Click the Analysis Queue bar to open a full modal with three sections.

#### Running Job Section

- **Blue pulse indicator** — Animated dot signaling active processing.
- **Type badge** — Color-coded label identifying the job type (JAR, PL/SQL, Parser, Claude, etc.).
- **Job name** — The target being analyzed.
- **Elapsed time** — How long the job has been running.
- **Progress bar** — Visual bar with percentage label.
- **Current step text** — Description of the operation in progress (e.g., "Parsing package body...").
- **Cancel button** — Immediately cancels the running job.

#### Queued Section

Lists all pending jobs in execution order.

- Each entry shows a sequential position number (#1, #2, ...), a type badge, and the job name.
- **Move Up / Move Down buttons** — Reorder a job's position in the queue.
- **Cancel (x) button** — Remove a job from the queue before it starts.
- When the order has been changed, two action buttons appear:
  - **Apply Order** — Commits the new queue ordering to the server.
  - **Discard** — Reverts to the original order.

#### History Section

Displays the last 20 completed, failed, or cancelled jobs.

Each entry shows:

| Element | Description |
|---------|-------------|
| Status icon | Checkmark for completed, X for failed, circle-slash for cancelled |
| Type badge | Color-coded job type label |
| Job name | The target that was analyzed |
| Result name | The resulting analysis name, if applicable |
| Error text | Failure reason, shown only for failed jobs |
| Elapsed time | Total duration of the job |

### Auto-Refresh

The queue polls the server at `/api/queue` every 3 seconds to keep all sections current. No manual refresh is needed.

### Job Type Badges

The following job types may appear throughout the queue:

| Badge | Meaning |
|-------|---------|
| JAR | JAR file structural analysis |
| PL/SQL | Standard PL/SQL analysis |
| PL/SQL Fast | Lightweight PL/SQL analysis |
| Parser | PL/SQL grammar parsing |
| Claude Enrich | AI enrichment of a batch |
| Claude Enrich (1) | AI enrichment of a single item |
| Claude Rescan | AI re-analysis of previously enriched items |
| Claude Full Scan | Complete AI analysis across all items |
| Claude Correct | AI correction of a batch |
| Claude Correct (1) | AI correction of a single item |
| PL/SQL Verify | AI verification of parsed PL/SQL results |

---

## Progress Report

Two side-by-side panels show aggregated statistics and AI enrichment progress for each analyzer.

### JAR Analyzer (Left Panel)

| Metric | Description |
|--------|-------------|
| Total JARs | Number of JAR files analyzed |
| Endpoints | Total REST/web endpoints discovered |
| Classes | Total Java classes cataloged |
| AI Enriched | Endpoints enriched by Claude (green) |
| AI Running | Enrichment jobs currently in progress (blue, shown only when nonzero) |
| AI Pending | Enrichment jobs waiting to start (orange, shown only when nonzero) |
| Not Enriched | Endpoints without AI enrichment |
| Progress bar | Visual bar showing percentage of endpoints AI-enriched |

### PL/SQL Parse Analyzer (Right Panel)

| Metric | Description |
|--------|-------------|
| Analyses | Total parsing analyses completed |
| Total Procs | Number of procedures parsed |
| Tables | Number of database tables referenced |
| Lines of Code | Total LOC processed across all analyses |
| AI Verified | Procedures verified by Claude (green) |
| AI Sessions Running | Active verification sessions (blue, shown only when nonzero) |
| AI Sessions Failed | Failed verification sessions (red, shown only when nonzero) |
| Progress bar | Visual bar showing percentage of procedures AI-verified |

A **Refresh** button at the top of the Progress Report section reloads both panels simultaneously.

---

## Claude Sessions Panel

A table listing all AI analysis sessions across both analyzers.

| Column | Description |
|--------|-------------|
| Analyzer | Which tool owns the session. Shown as a colored badge: **JAR** (indigo) or **Parser** (green). |
| ID | Unique session identifier, displayed in monospace at a smaller font size. |
| Status | Current session state: **RUNNING** (blue), **COMPLETE** (green), **FAILED** (red), or **KILLED** (orange). |
| Type | The kind of AI analysis being performed (e.g., Enrich, Verify, Correct). |
| Detail | The target endpoint or analysis name. Long values are truncated; hover to see the full text in a tooltip. |
| Duration | Elapsed wall-clock time for the session (e.g., "2m 35s"). |

### Refresh Behavior

- Auto-refreshes every 30 seconds.
- A manual **Refresh** button is available for immediate reload.

---

## Footer

The page footer displays:

> Unified Analyzer v1.0 — Port 8083

---

## Global Features (Present on All Screens)

The following elements are available on every screen in the application, not just the Home Dashboard.

### Log FAB (Bottom-Left)

A floating action button anchored to the bottom-left corner of the viewport. Click to open a panel displaying live application logs (served from `GET /api/jar/logs` or `GET /api/plsql/logs`).

### Chat FAB (Bottom-Right)

A floating action button anchored to the bottom-right corner of the viewport. Click to open the AI chatbox for conversational interaction with the analyzer. The chatbox uses the current tool's analysis context.

### Chat Toggle (Top Bar)

When available, a toggle in the top navigation bar switches between two chat interface modes:

- **New** — Opens the chatbox overlay (floating panel). Uses the JAR-specific chatbox API (`/api/jars/{id}/chat`).
- **Classic** — Opens the chat in a session-based side panel with full conversation history. Uses the shared sessions API (`/api/chat/sessions`).

Chat mode preference persists across page reloads.

### Help Button (?)

A floating button positioned to the left of the Chat FAB. Click to open the **Help & Documentation** modal, which offers two modes:

- **Business Guide** — Non-technical overview of features, workflows, and use cases.
- **Technical Guide** — Developer-oriented documentation covering APIs, architecture, and configuration.

### Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Escape | Closes any open modal (queue, help, logs, chat) |

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

#### Resource Monitor Improvements (2025-12-23)
- **Bug #1**: Zero-value schedule validation - Added explicit validation to prevent zero-second intervals with clear error messages
- **Bug #3**: Elapsed time parsing crash - Added format validation to prevent crashes on malformed time strings with warning logs
- **Bug #5**: Node state matching - Implemented consistent substring matching for node states (down/drain/maint/reserved) instead of exact matching
- **Bug #6**: Resource availability initialization - Fixed state initialization to prevent missed notifications by using lazy initialization pattern
- **Bug #8**: Report failure messaging - Added accurate success/failure messages for initial report generation

### Improved

#### Code Quality (2025-12-23)
- Added warning logs for invalid elapsed time formats to aid debugging
- Enhanced test coverage with 2 new edge case tests:
  - Unparseable GPU count handling in SLURM output
  - Nonexistent partition graceful error handling
- Improved keyboard interrupt test assertions for better signal handling verification
- All 19 resource monitor tests passing
- Pre-commit hooks (ruff, mypy) passing

#### Documentation (2025-12-23)
- Created comprehensive monitoring guide (`docs/monitoring.rst`) covering:
  - Job monitoring with state transitions
  - Resource monitoring with GPU availability tracking
  - Scheduled reporting with Slack integration
  - Programmatic usage examples
  - Best practices and troubleshooting
- Updated README.md with new `srunx monitor` subcommand structure
- Updated API reference documentation

## [0.8.0] - 2025-12-13

### Added

#### Scheduled Reporting System
- **Report Content**:
  - Job queue statistics (pending, running, completed, failed, cancelled)
  - GPU resource utilization (total, in-use, available with percentage)
  - Node statistics (total, idle, down nodes)
  - User-specific job statistics (optional)

- **Configuration Options**:
  - `--include`: Customize report sections (jobs, resources, user)
  - `--partition`: Target specific SLURM partition
  - `--user`: Filter for specific user's jobs
  - `--timeframe`: Aggregation window for historical job data (default: 24h)
  - `--daemon`: Run as background daemon (default: true)

- **ScheduledReporter Class**: Core scheduling engine using APScheduler
  - Interval and cron trigger support
  - Graceful shutdown on SIGINT/SIGTERM
  - Error recovery and retry logic
  - Minimum interval enforcement (60 seconds) to prevent SLURM overload

- **Enhanced SlackCallback**:
  - `on_scheduled_report()`: Format and send rich Slack messages
  - Structured report sections with emojis for readability
  - GPU utilization percentage calculation
  - Partition-specific resource display

### Dependencies

- **apscheduler>=3.10.0**: Scheduling framework for periodic task execution
- **tzlocal>=5.3.1**: Timezone handling for cron triggers (dependency of APScheduler)

### Documentation

- Updated README with scheduled reporting examples and usage patterns
- Added comprehensive docstrings for all new classes and methods
- Created `SPEC_SCHEDULED_REPORTING.md` with detailed technical specification

## [0.7.0] - 2025-12-13

### Added

#### Job and Resource Monitoring System
- **JobMonitor**: Monitor SLURM job state transitions (PENDING → RUNNING → COMPLETED/FAILED/CANCELLED)
  - `watch_until()`: Block until jobs reach target states or timeout
  - `watch_continuous()`: Continuous monitoring with callback notifications
  - Support for multiple job monitoring with dependency tracking
  - Configurable poll intervals and timeout settings
  - Graceful error recovery for SLURM command failures

- **ResourceMonitor**: Monitor GPU resource availability on partitions
  - `watch_until()`: Block until minimum GPU threshold is met
  - `watch_continuous()`: Continuous resource monitoring with state change notifications
  - Real-time GPU availability tracking via `sinfo` and `squeue`
  - Node statistics (total, idle, down nodes)
  - Configurable GPU thresholds and partition filtering

- **Callback System**: Extensible notification framework
  - `Callback`: Base class for job and resource state notifications
  - `SlackCallback`: Send notifications to Slack via webhooks
    - Job state notifications (submitted, running, completed, failed, cancelled)
    - Workflow completion notifications
    - Resource availability/exhaustion alerts
    - Security hardening with webhook URL validation and text sanitization

- **CLI Commands**
  - `srunx monitor <job_id>`: Monitor job until completion
  - `srunx monitor --continuous <job_id>`: Continuous job monitoring
  - `srunx list --watch`: Live job queue updates

### Security

- **Slack Webhook Validation**: Strict URL pattern validation for Slack webhook URLs
  - Requires exactly 3 path segments in webhook URL format
  - Rejects non-HTTPS URLs

- **Text Sanitization**: Comprehensive input sanitization for Slack messages
  - HTML entity escaping to prevent script injection
  - Markdown character escaping to prevent formatting abuse
  - Control character removal for message integrity
  - Length limits to prevent message overflow
  - Correct escaping order to prevent double-escaping vulnerabilities

### Testing

- **Comprehensive Test Coverage**
  - 37 callback tests including 25 security-focused tests
  - 14 error recovery tests for SLURM command failures
  - 10 timeout validation tests for monitoring edge cases
  - 64 CLI command tests
  - Full coverage for monitoring functionality

### Documentation

- Updated README with monitoring system usage examples
- Added monitoring section to CLAUDE.md with architecture details
- Comprehensive docstrings for all monitoring classes and methods

## [0.6.3] - 2024-XX-XX

### Changed
- Default log level adjustments
- SSH bug fixes
- Documentation updates

## [0.6.0] - 2024-XX-XX

### Added
- SSH integration for remote SLURM clusters
- Slack notification support
- GitHub Pages documentation

[0.7.0]: https://github.com/ksterx/srunx/compare/v0.6.3...v0.7.0
[0.6.3]: https://github.com/ksterx/srunx/compare/v0.6.0...v0.6.3
[0.6.0]: https://github.com/ksterx/srunx/releases/tag/v0.6.0

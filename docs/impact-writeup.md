# AMP Analyzer — Employee-Led Innovation with Measurable Outcome

As part of a large-scale legacy modernization program, our team was analyzing and converting hundreds of Oracle PL/SQL procedures covering more than 2 lakh lines of code, along with Java JAR/WAR applications. The existing approach required engineers to manually trace procedure call chains, search for table operations, identify joins, views, triggers, dynamic SQL, and build dependency maps across multiple files. This made analysis slow, error-prone for long call chains, and highly dependent on individual engineers' knowledge of the application and database schema.

I led the design and implementation of AMP Analyzer, an internal static-analysis platform that automated this complete analysis workflow. The platform supports Oracle, SQL Server, MySQL, Db2, and PostgreSQL, along with Java JAR/WAR analysis. It automatically builds complete procedure call chains, identifies callers and callees, captures table operations with line numbers, joins with exact conditions, views, triggers, dynamic SQL, dependencies, and complexity metrics. The solution uses precomputed analysis views, sidecar files, byte-offset indexes, and a layout-manifest storage design to make the analysis results available almost instantly through 400+ REST APIs.

The innovation delivered a ~35,000× reduction in analysis-view read latency, from 71.5 seconds to approximately 2 milliseconds, and a ~2,000–4,000× improvement in search performance, from 35–41 seconds to 7–19 milliseconds. More importantly, batch validation of 2,00,000+ lines of code was reduced from 3–5 days of manual effort to 2–3 hours, while initial static analysis of the complete codebase was reduced to approximately 10–15 minutes. Call-chain analysis that previously required hours of manual tracing can now be retrieved in approximately 4.6 milliseconds.

## Oracle Analysis – Before vs After AMP Analyzer

| Activity | Before AMP Analyzer | After AMP Analyzer |
|---|---|---|
| Oracle Code Analysis | Days | 10–15 minutes |
| Validation of 2+ lakh lines | 3–5 days | 2–3 hours |
| Call Chain Analysis | Hours | ~2 milliseconds |
| Dependency Analysis | Hours | Few milliseconds |
| Table Usage Analysis | ~½ day | <20 milliseconds |
| Debugging / Impact Analysis | Hours | ~4.6 milliseconds |
| Conversion Planning | Days | Minutes |

## Simple Summary

| Area | Before | After |
|---|---|---|
| Oracle Analysis | Days | 10–15 minutes |
| Validation | 3–5 days | 2–3 hours |
| Debugging | Hours | ~4.6 ms |
| Call Chain | Hours | ~2 ms |
| Table/Dependency Search | Hours | <20 ms |
| Conversion Planning | Days | Minutes |

The platform was built using Java 17, Spring Boot 3.2, custom parsers for five SQL dialects and Java bytecode, JDBC, AWS S3, AWS SSO/STS, Kubernetes/Kanopy, Docker, Drone CI/CD, Helm, and GHCR. It was validated through a 3,452-test automated quality gate and a 769-combination API verification sweep with zero server errors.

The solution was employee-led from identifying the daily analysis bottleneck through architecture, implementation, deployment, quality validation, and team adoption. It transformed a highly manual and expertise-dependent analysis process into a fast, repeatable, and evidence-based workflow, enabling analysts to plan conversions, understand dependencies, debug call chains, and validate changes significantly faster while returning several days of engineering effort on every large validation batch.

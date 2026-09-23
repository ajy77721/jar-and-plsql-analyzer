# PL/SQL Analyzer - Config

Shared configuration module that loads and manages Oracle database connection
settings, project/environment hierarchies, and schema mappings from YAML files.
Consumed as a dependency by `plsql-parser`, `plsql-analyzer-core`, and `unified-web`.

## Technology Stack

| Concern | Library | Version |
|---------|---------|---------|
| YAML parsing | Jackson Dataformat YAML | (managed by Spring Boot 3.2.5) |
| Serialization | Jackson Databind | (managed by Spring Boot 3.2.5) |
| Java | JDK | 17 |

## Package Structure

```
com.plsqlanalyzer.config
  PlsqlConfig           Root config: source paths, schema mappings, DB credentials,
                        project list; supports both legacy single-env and multi-env modes
  ConfigLoader          Loads/saves PlsqlConfig from YAML files or classpath resources
  ProjectConfig         Named project containing a list of environments
  EnvironmentConfig     Named environment (e.g., PROD, UAT) with zone and connections
  ConnectionConfig      Single Oracle connection: hostname, port, service name/SID/TNS,
                        connection type (SERVICE_NAME, SID, TNS, LDAP, CUSTOM), role
  DbUserConfig          Username/password/description tuple for Oracle authentication
  SchemaMapping         Maps an alias to an Oracle schema name with optional DB link
```

## YAML Configuration Format

The module reads `plsql-config.yaml` with the following structure:

```yaml
projects:
  - name: "MY_PROJECT"
    description: "Project description"
    environments:
      - name: "PROD"
        zone: "EU-CENTRAL"
        connections:
          - name: "CORE_PROD"
            username: "CORE_USER"
            password: "..."
            hostname: "db-host.example.com"
            port: 1521
            service_name: "ORCL"
            connection_type: "SERVICE_NAME"   # SERVICE_NAME | SID | TNS | LDAP | CUSTOM
            role: "DEFAULT"                   # DEFAULT | SYSDBA | SYSOPER

schema_mappings:
  core:
    schema: "CORE_SCHEMA"
    description: "Core schema"
    db_link: null
```

Legacy single-environment mode (flat `db_hostname`, `db_port`, `db_service_name`,
`db_users` fields) is also supported. `PlsqlConfig.isLegacyMode()` returns `true`
when no projects are defined.

## Key Methods

- **ConfigLoader.loadFromFile(Path)** -- Reads YAML from a file path.
- **ConfigLoader.loadDefault()** -- Loads `plsql-config.yaml` from the classpath.
- **ConfigLoader.saveToFile(PlsqlConfig, Path)** -- Writes config back to YAML.
- **PlsqlConfig.resolveEnvironment(project, env)** -- Looks up a specific environment.
- **PlsqlConfig.resolveSchema(alias)** -- Maps an alias to its Oracle schema name.
- **ConnectionConfig.getJdbcUrl()** -- Builds a JDBC URL from the connection fields.

## Build

```bash
mvn clean install -pl plsql-config
```

This module has no standalone entry point. It is consumed as a dependency by
`plsql-parser`, `plsql-analyzer-core`, and `unified-web`.

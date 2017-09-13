# Retz Digdag Plugin [![CircleCI](https://circleci.com/gh/retz/retz-digdag-plugin.svg?style=svg)](https://circleci.com/gh/retz/retz-digdag-plugin)

This plugin helps you to managing [Retz](https://github.com/retz/retz) ( a job queuing and execution service for [Apache Mesos](http://mesos.apache.org) ) jobs on
[Digdag](https://github.com/treasure-data/digdag) workflow.

```yaml
timezone: Asia/Tokyo

_export:
  plugin:
    repositories:
      - http://retz-maven.s3.amazonaws.com/releases
    dependencies:
      - io.github.retz:retz-digdag-plugin:0.1.0
  retz:
    appname: default
    cpu: 1
    mem: 512m

+simple_app:
  retz_run>: /opt/app/simple-app.sh

+heavy_batch:
  retz_run>: /opt/app/heavy-batch.sh ${session_date}
  cpu: 8
  mem: 16g
  priority: 1
  timeout: 30

+learn:
  retz_run>: /opt/app/learn.py
  appname: learn-ubuntu
  gpu: 2
  mem: 4g
  env: [FOO=123, BAR=456]
```

## Requirement
### Digdag
- [Digdag](https://github.com/treasure-data/digdag) Version: 0.9.13+

### Retz

This Plugin Version | [Retz Server](https://github.com/retz/retz/blob/master/doc/getting-started-server.rst) Version
--- | ---
0.0.x | 0.2.x (0.2.9+)
0.1.x | 0.3.x
0.2.x | 0.4.x

## Usage

### Digdag configuration property

To use the plugin, define the following Retz client settings to digdag config file (e.g. `digdag.properties`):

- `retz.server.uri`
  - Defines Retz server location to send all requests.
- `retz.authentication`
  - Set `true` to enable authentication.
- `retz.access.key`
  - Defines user identity to send requests to servers with.
- `retz.access.secret`
  - Defines access secret to identify and authenticate a user.

```properties
retz.server.uri = http://10.0.0.1:9090
retz.authentication = true
retz.access.key = deadbeef
retz.access.secret = cafebabe
```

Please refer to [Retz Client documents](https://github.com/retz/retz/blob/master/doc/api.rst#client-configuration-file) for details.

### Workflow settings

Include the following plugin settings in your digadg workflow file (`*.dig`):

```yaml
_export:
  plugin:
    repositories:
      - http://retz-maven.s3.amazonaws.com/releases
    dependencies:
      - io.github.retz:retz-digdag-plugin:0.1.0
```

## Provided operator plugins

### retz_run>: Submitting a Retz job

Submit a job by using Retz Client WebAPI. This operator schedules a job to Retz server and waits for it finish either successfully or not. Please refer to [Retz Client documents](https://github.com/retz/retz/blob/master/doc/api.rst#client-cli-and-api) for details.

```yaml
+run:
  retz_run>: echo "hello, Retz!"
  appname: your-test-app
```

### Options

- `retz_run>:` COMMAND [ARGS...]
    - **(required)** Remote command to run
- `appname`: STRING
    - **(required)** Application name you loaded by `retz-client load-app`
- `cpu`: NUMBER
    - Number of CPU cores assigned to the job
    - default: `1`
- `mem`: STRING
    - Number of size of RAM(MB) assigned to the job
    - default: `32MB`
    - This option can specify a size unit (e.g. `64m`, `128GB`)
        - MiB: `m`|`mb`, GiB:`g`|`gb`, TiB:`t`|`tb` (case insensitive)
- `disk`: STRING
    - Amount of temporary disk space in MB which the job is going to use
    - default: `32MB`
    - This option can specify a size unit (e.g. `64m`, `128GB`)
        - MiB: `m`|`mb`, GiB:`g`|`gb`, TiB:`t`|`tb` (case insensitive)
- `ports`: NUMBER
    - Number of ports (up to 1000) required to the job
    - default: `0`
- `gpu`: NUMBER
    - Number of GPU cards assigned to the job
    - default: `0`
- `priority`: NUMBER
    - Job priority. Priority handling depends on server planner setting
    - default: `0`.
- `name`: STRING
    - Human readable job name
    - default: `session_id`#`attempt_id`..`task_name`
- `tags`: [ARRAY OF NAMES]
    - Mark the job with tags
- `env`:  [ARRAY OF "KEY=VALUE"]
    - List of environment values. `$HOME` and `$MESOS_*` are overwritten by Mesos executor
- `timeout`: NUMBER
    - Timeout in minutes. After timeout, the client tries to kill the job
    - `-1` or `0` for no timeout
    - default: `1440` (24 hours)
- `verbose`: BOOLEAN
    - Set `true` to display detailed processing information
    - default: `false`
- `client_config`: STRING **(deprecated)**
    - Configuration file path for Retz Client.
    - This option is deperecated and will be removed in a future version.

### Output parameters

- `retz.last_job_id`
    - The job id this task executed.

### Examples

```yaml
_export:
  retz:
    verbose: true
    tags: ["${session_local_time}","${task_name}"]

+run_asakusa_m3bp:
  retz_run>: /opt/asakusa/yaess/bin/yaess-batch.sh m3bp.app -A date=${session_date}
  appname: m3bp-app
  cpu: 32
  mem: 8192
  priority: 2
  env: [
    "ASAKUSA_HOME=/opt/asakusa",
    "ASAKUSA_M3BP_OPTS=-Xmx${Math.round(mem * 0.3)}m",
  ]
  timeout: 120
```

## License
* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)


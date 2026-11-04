# sync-tables documentation

sync-tables copies PostgreSQL tables between environments, one way only, along a small
set of edges compiled into the tool. It is meant for two jobs:

- giving a developer a local copy of a shared development warehouse, without a tunnel and
  without development database credentials on the laptop;
- mirroring a selected set of warehouse tables from development into production, without
  production credentials ever leaving production.

| Document | What it covers |
|---|---|
| [architecture.md](architecture.md) | Roles, edges and transports, the wire protocol, how a table is copied |
| [security.md](security.md) | Threat model, every guard and what it stops, the limits that remain |
| [operations.md](operations.md) | Deploying the listeners, certificates and tokens, rotation, the failed-attempt limit, checks to run |

The command-line usage and the configuration format are in the [top-level README](../README.md).

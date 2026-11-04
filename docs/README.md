# sync-tables documentation

sync-tables copies PostgreSQL tables between environments, one way only, along edges
both sides declare, under one compiled rule: only `dev` writes to `prod`. It is meant for
three jobs:

- giving a developer a local copy of a shared development warehouse, without a tunnel and
  without development database credentials on the laptop;
- mirroring a selected set of warehouse tables from development into production (and the
  environments that mirror it), without production credentials ever leaving production;
- carrying the pipelines' run events with that mirror, so each environment hears of a run
  only after its data has arrived (the relay).

| Document | What it covers |
|---|---|
| [architecture.md](architecture.md) | Roles, edges and transports, the wire protocol, how a table is copied, run events and the relay |
| [security.md](security.md) | Threat model, every guard and what it stops, the limits that remain |
| [operations.md](operations.md) | Deploying the listeners and the relays, certificates and tokens, rotation, the failed-attempt limit, upgrading to 0.8.0, checks to run |

The command-line usage and the configuration format are in the [top-level README](../README.md).

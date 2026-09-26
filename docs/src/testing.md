# Testing and contributing

## Running the tests

```sh
julia --project -e 'using Pkg; Pkg.test()'
```

The suite uses an in-process mock AMQP broker (Service Bus semantics, TLS,
WebSockets), local HTTP stand-ins for Entra ID, IMDS and the REST APIs, and
generates throw-away certificates at run time. It also runs
[Aqua.jl](https://github.com/JuliaTesting/Aqua.jl) quality checks.

Integration tests against local emulators run when enabled:

```sh
# Azurite (Blob Storage; MIT licensed, no EULA)
docker compose -f test/emulator/azurite-compose.yml up -d
AZURESDK_AZURITE=1 julia --project -e 'using Pkg; Pkg.test()'

# Event Hubs emulator + Azurite
ACCEPT_EULA=Y docker compose -f test/emulator/docker-compose.yml up -d
# Service Bus emulator + SQL Server (~2 GB RAM)
export MSSQL_SA_PASSWORD='<any strong password>'
ACCEPT_EULA=Y docker compose -f test/emulator/servicebus-compose.yml up -d

AZURESDK_EVENTHUBS_EMULATOR=1 AZURESDK_SERVICEBUS_EMULATOR=1 julia --project -e 'using Pkg; Pkg.test()'
```

Starting the Event Hubs and Service Bus emulators means accepting Microsoft's
emulator (and SQL Server) license terms. Those emulators do not validate SAS
keys and do not support TLS or WebSockets, so those paths are covered by the
mock broker. Azurite does validate Shared Key and SAS signatures.

In CI, the Azurite tests run on every push. The Event Hubs and Service Bus
emulator jobs run only when the repository variable `ACCEPT_EMULATOR_EULA` is
set to `Y`.

## Building the documentation

```sh
julia --project=docs -e 'using Pkg; Pkg.develop(path="."); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

## Contributing

After cloning, run `sh scripts/configure-private-git.sh` to use a neutral
commit identity, get a `git ci` alias that records UTC timestamps, and install
a pre-commit hook that refuses changes containing your home directory path,
e-mail address or host name.

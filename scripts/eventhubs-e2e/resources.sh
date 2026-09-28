#!/usr/bin/env bash
# Create or delete the Azure resources for the Event Hubs end-to-end scripts
# send.jl and listen.jl in this directory: a resource group, an Event Hubs
# namespace, one event hub and a Send/Listen shared access policy.
#
# Usage:
#   scripts/eventhubs-e2e/resources.sh create      # writes scripts/eventhubs-e2e/.env
#   scripts/eventhubs-e2e/resources.sh show        # prints the saved settings (without keys)
#   scripts/eventhubs-e2e/resources.sh grant       # (re)assigns the data role for --auth entra
#   scripts/eventhubs-e2e/resources.sh delete [-y] # deletes everything create made
#
# Needs the Azure CLI, logged in (`az login`) to the subscription to use.
# Optional settings for `create` (environment variables):
#   LOCATION        Azure region                         (default: westeurope)
#   RESOURCE_GROUP  resource group; created unless it exists (default: azuresdk-jl-eh-e2e)
#   NAMESPACE       globally unique namespace name       (default: azsdkjl-e2e-<random>)
#   EVENTHUB        event hub name                       (default: e2e-hub)
#   SKU             Basic | Standard | Premium           (default: Basic, the cheapest)
#   PARTITIONS      number of partitions                 (default: 2)
#   ASSIGN_ROLE     1 = give the signed-in user the "Azure Event Hubs Data Owner"
#                   role on the namespace, for `--auth entra` (default: 1)
#
# The namespace is billed per hour while it exists: run `delete` when you are done.
set -euo pipefail

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
env_file="$dir/.env"
tag="purpose=azuresdk-jl-eventhubs-e2e"
policy="e2e-send-listen"

die() { echo "error: $*" >&2; exit 1; }
log() { echo "==> $*" >&2; }

command -v az >/dev/null || die "the Azure CLI (az) is not installed"

load_env() {
    [[ -f "$env_file" ]] || die "$env_file not found; run '$0 create' first"
    # values are written single-quoted by `create`
    # shellcheck disable=SC1090
    source "$env_file"
}

create() {
    az account show >/dev/null 2>&1 || die "not logged in; run 'az login'"

    local location=${LOCATION:-westeurope}
    local rg=${RESOURCE_GROUP:-azuresdk-jl-eh-e2e}
    local ns=${NAMESPACE:-azsdkjl-e2e-$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom | head -c 8)}
    local hub=${EVENTHUB:-e2e-hub}
    local sku=${SKU:-Basic}
    local partitions=${PARTITIONS:-2}
    local created_rg=0

    if [[ -f "$env_file" ]]; then
        load_env
        [[ -z $EVENTHUB_CONNECTION_STRING ]] || die "$env_file already exists; run '$0 delete' first (or remove the file)"
        # an earlier `create` stopped half-way: finish it with the same names (the az create calls are idempotent)
        rg=$E2E_RESOURCE_GROUP ns=${EVENTHUB_NAMESPACE%%.*} hub=$EVENTHUB_NAME created_rg=$E2E_CREATED_RESOURCE_GROUP
        location=$(az group show -n "$rg" --query location -o tsv)
        log "resuming an unfinished create for namespace $ns"
    fi

    log "subscription: $(az account show --query name -o tsv)"
    if [[ $created_rg == 1 ]]; then
        log "using resource group $rg"
    elif [[ $(az group exists -n "$rg") == "true" ]]; then
        log "using existing resource group $rg (it will not be deleted by '$0 delete')"
    else
        log "creating resource group $rg in $location"
        az group create -n "$rg" -l "$location" --tags "$tag" -o none
        created_rg=1
    fi

    # Save what exists so far, so `delete` can clean up even if a later step fails.
    write_env "$rg" "$created_rg" "$ns" "$hub" ""

    log "creating Event Hubs namespace $ns ($sku)"
    az eventhubs namespace create -g "$rg" -n "$ns" -l "$location" --sku "$sku" --tags "$tag" -o none

    log "creating event hub $hub ($partitions partitions, 1 h retention)"
    az eventhubs eventhub create -g "$rg" --namespace-name "$ns" -n "$hub" \
        --partition-count "$partitions" --cleanup-policy Delete --retention-time-in-hours 1 -o none

    log "creating shared access policy $policy (Send, Listen) on the event hub"
    az eventhubs eventhub authorization-rule create -g "$rg" --namespace-name "$ns" --eventhub-name "$hub" \
        --name "$policy" --rights Send Listen -o none
    local conn
    conn=$(az eventhubs eventhub authorization-rule keys list -g "$rg" --namespace-name "$ns" \
        --eventhub-name "$hub" --name "$policy" --query primaryConnectionString -o tsv)

    write_env "$rg" "$created_rg" "$ns" "$hub" "$conn"

    if [[ ${ASSIGN_ROLE:-1} == 1 ]]; then
        grant_role "$rg" "$ns" || echo "warning: --auth entra will not work until '$0 grant' succeeds; --auth key works" >&2
    fi

    log "done; settings saved to $env_file"
    cat >&2 <<EOF

Try it (in two terminals):
  julia --project scripts/eventhubs-e2e/listen.jl
  julia --project scripts/eventhubs-e2e/send.jl "hello" "world"

Delete the resources afterwards:
  scripts/eventhubs-e2e/resources.sh delete
EOF
}

# Object id of the signed-in user. Falls back to the token's `oid` claim when
# Microsoft Graph is unavailable (as happens for guest / personal accounts).
signed_in_object_id() {
    local id payload
    if id=$(az ad signed-in-user show --query id -o tsv 2>/dev/null) && [[ -n $id ]]; then
        echo "$id"; return
    fi
    payload=$(az account get-access-token --query accessToken -o tsv | cut -d. -f2 | tr '_-' '/+')
    while (( ${#payload} % 4 )); do payload+="="; done
    id=$(printf '%s' "$payload" | base64 -d 2>/dev/null | grep -o '"oid":"[^"]*"' | cut -d'"' -f4)
    [[ -n $id ]] || return 1
    echo "$id"
}

# Give the signed-in user "Azure Event Hubs Data Owner" on the namespace (for --auth entra).
grant_role() {
    local rg=$1 ns=$2 user_id ns_id
    [[ $(az account show --query user.type -o tsv) == "user" ]] \
        || { echo "warning: not signed in as a user; skipped the role assignment" >&2; return 1; }
    user_id=$(signed_in_object_id) || { echo "error: could not determine the signed-in user's object id" >&2; return 1; }
    ns_id=$(az eventhubs namespace show -g "$rg" -n "$ns" --query id -o tsv)
    log "granting 'Azure Event Hubs Data Owner' on $ns to $user_id"
    az role assignment create --assignee-object-id "$user_id" --assignee-principal-type User \
        --role "Azure Event Hubs Data Owner" --scope "$ns_id" -o none || return 1
    log "role assigned; it can take a few minutes until Event Hubs accepts it"
}

write_env() {
    local rg=$1 created_rg=$2 ns=$3 hub=$4 conn=$5
    (
        umask 077
        cat >"$env_file" <<EOF
# Written by scripts/eventhubs-e2e/resources.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ). Contains a secret key.
E2E_RESOURCE_GROUP='$rg'
E2E_CREATED_RESOURCE_GROUP='$created_rg'
EVENTHUB_NAMESPACE='$ns.servicebus.windows.net'
EVENTHUB_NAME='$hub'
EVENTHUB_CONNECTION_STRING='$conn'
EOF
    )
}

show() {
    load_env
    echo "resource group:  $E2E_RESOURCE_GROUP (created by this script: $E2E_CREATED_RESOURCE_GROUP)"
    echo "namespace:       $EVENTHUB_NAMESPACE"
    echo "event hub:       $EVENTHUB_NAME"
    echo "connection str.: $([[ -n $EVENTHUB_CONNECTION_STRING ]] && echo "saved in $env_file" || echo missing)"
}

delete() {
    local yes=${1:-}
    load_env
    local ns=${EVENTHUB_NAMESPACE%%.*}
    if [[ $E2E_CREATED_RESOURCE_GROUP == 1 ]]; then
        if [[ $(az group exists -n "$E2E_RESOURCE_GROUP") == "false" ]]; then
            log "resource group $E2E_RESOURCE_GROUP no longer exists"
            rm -f "$env_file"
            return
        fi
        [[ $(az group show -n "$E2E_RESOURCE_GROUP" --query tags.purpose -o tsv) == "${tag#purpose=}" ]] \
            || die "resource group $E2E_RESOURCE_GROUP lacks the tag $tag; not deleting it"
        confirm "$yes" "Delete resource group $E2E_RESOURCE_GROUP and everything in it?"
        log "deleting resource group $E2E_RESOURCE_GROUP (takes a few minutes)"
        az group delete -n "$E2E_RESOURCE_GROUP" --yes
    else
        confirm "$yes" "Delete Event Hubs namespace $ns in resource group $E2E_RESOURCE_GROUP?"
        log "deleting namespace $ns"
        az eventhubs namespace delete -g "$E2E_RESOURCE_GROUP" -n "$ns"
    fi
    rm -f "$env_file"
    log "done"
}

confirm() {
    [[ $1 == "-y" || $1 == "--yes" ]] && return
    local answer
    read -r -p "$2 [y/N] " answer
    [[ $answer == [yY]* ]] || die "aborted"
}

case ${1:-} in
    create) create ;;
    delete) delete "${2:-}" ;;
    grant) load_env; grant_role "$E2E_RESOURCE_GROUP" "${EVENTHUB_NAMESPACE%%.*}" ;;
    show) show ;;
    *) sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac

#!/usr/bin/env bash
# Managed identity / workload identity end-to-end tests in Azure.
#
# Creates an Event Hubs namespace, a Service Bus namespace and a storage account
# (all with key/SAS access disabled, so only Entra ID works), a user-assigned
# managed identity, a container registry, and the compute targets that run
# identity_test.jl in a container built from this working tree:
#
#   aci  Azure Container Instances  managed identity over IMDS: user-assigned (by client, object and
#                                   resource id) and, in a second container group, system-assigned
#   aca  Azure Container Apps job   managed identity over IDENTITY_ENDPOINT (App Service style)
#   aks  Azure Kubernetes Service   workload identity (federated token file), the managed identity
#                                   token exchange, and the node's (kubelet) identity over IMDS
#
# Usage:
#   scripts/identity-e2e/resources.sh create [aci] [aca] [aks]  # default: aci aca (aks takes ~10 min)
#   scripts/identity-e2e/resources.sh run [target ...] [--no-build]
#                         # builds the image from the working tree, runs the tests, prints the results
#   scripts/identity-e2e/resources.sh show
#   scripts/identity-e2e/resources.sh delete [-y]
#
# Settings for `create` (environment variables): LOCATION (default westeurope),
# RESOURCE_GROUP (default azuresdk-jl-id-e2e; must not exist yet), AKS_VM_SIZE
# (default Standard_B2s), BUILD (acr = build in Azure Container Registry (default),
# local = docker build and push). Settings are saved to scripts/identity-e2e/.env.
#
# Cost: Container Instances and Container Apps only bill while a test runs; the
# registry (Basic), the namespaces (Basic) and the AKS node bill while they
# exist. Run `delete` when you are done.
set -euo pipefail

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$dir/../.." && pwd)
env_file="$dir/.env"
tag="purpose=azuresdk-jl-identity-e2e"
image_repo="azuresdk-e2e"
k8s_ns="azuresdk-e2e"

tmp_dirs=()
trap 'rm -rf "${tmp_dirs[@]}"' EXIT
tempdir() { local d; d=$(mktemp -d); tmp_dirs+=("$d"); echo "$d"; }

die() { echo "error: $*" >&2; exit 1; }
log() { echo "==> $*" >&2; }

command -v az >/dev/null || die "the Azure CLI (az) is not installed"

# --- settings -----------------------------------------------------------------

load_env() {
    [[ -f "$env_file" ]] || die "$env_file not found; run '$0 create' first"
    # shellcheck disable=SC1090
    source "$env_file"
    names
}

save_env() {
    (
        umask 077
        cat >"$env_file" <<EOF
# Written by scripts/identity-e2e/resources.sh
E2E_RESOURCE_GROUP='$E2E_RESOURCE_GROUP'
E2E_LOCATION='$E2E_LOCATION'
E2E_SUFFIX='$E2E_SUFFIX'
E2E_TARGETS='$E2E_TARGETS'
E2E_BUILD='$E2E_BUILD'
E2E_IMAGE_TAG='${E2E_IMAGE_TAG:-}'
EOF
    )
}

# Resource names, derived from the random suffix.
names() {
    rg=$E2E_RESOURCE_GROUP
    loc=$E2E_LOCATION
    uami=e2e-identity
    acr=azsdkjle2e$E2E_SUFFIX
    eh_ns=azsdkjl-id-eh-$E2E_SUFFIX
    hub=e2e-hub
    sb_ns=azsdkjl-id-sb-$E2E_SUFFIX
    queue=e2e-queue
    storage=azsdkjle2e$E2E_SUFFIX
    results=e2e-results
    aci=e2e-aci
    aci_sys=e2e-aci-sys
    aca_env=e2e-aca-env
    aca_job=e2e-aca-job
    aks=e2e-aks
}

has_target() { [[ " $E2E_TARGETS " == *" $1 "* ]]; }

# Environment variables for the test container (without E2E_CREDENTIALS).
test_env() {
    local uami_json
    uami_json=$(az identity show -g "$rg" -n "$uami" --query "[clientId,principalId,id]" -o tsv | tr '\t\n' '  ')
    read -r uami_client uami_principal uami_id <<<"$uami_json"
    echo "E2E_EVENTHUBS_NAMESPACE=$eh_ns.servicebus.windows.net E2E_EVENTHUB=$hub" \
        "E2E_SERVICEBUS_NAMESPACE=$sb_ns.servicebus.windows.net E2E_QUEUE=$queue" \
        "E2E_STORAGE_URL=https://$storage.blob.core.windows.net E2E_RESULTS_CONTAINER=$results" \
        "E2E_UAMI_CLIENT_ID=$uami_client E2E_UAMI_PRINCIPAL_ID=$uami_principal E2E_UAMI_RESOURCE_ID=$uami_id"
}

# Object id of the signed-in user; falls back to the token's `oid` claim when
# Microsoft Graph is unavailable (guest / personal accounts).
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

# --- roles --------------------------------------------------------------------

role() {  # principal-id principal-type role scope
    az role assignment create --assignee-object-id "$1" --assignee-principal-type "$2" \
        --role "$3" --scope "$4" -o none
}

# Data-plane access to all three services for one principal.
grant_data_roles() {  # principal-id principal-type label
    log "granting data roles to $3"
    role "$1" "$2" "Azure Event Hubs Data Owner" "$(az eventhubs namespace show -g "$rg" -n "$eh_ns" --query id -o tsv)"
    role "$1" "$2" "Azure Service Bus Data Owner" "$(az servicebus namespace show -g "$rg" -n "$sb_ns" --query id -o tsv)"
    role "$1" "$2" "Storage Blob Data Contributor" "$(az storage account show -g "$rg" -n "$storage" --query id -o tsv)"
}

# --- image --------------------------------------------------------------------

build_image() {
    local stage tag_ts login
    tag_ts=$(date -u +%Y%m%d-%H%M%S)
    login=$(az acr show -n "$acr" --query loginServer -o tsv)
    stage=$(tempdir)
    # Only the tracked and non-ignored files the Dockerfile needs are sent (never .env).
    (cd "$repo" && git ls-files -z --cached --others --exclude-standard -- Project.toml src scripts/identity-e2e \
        | tar --null --ignore-failed-read -T - -cf - 2>/dev/null) | tar -xf - -C "$stage"
    cp "$dir/Dockerfile" "$stage/Dockerfile.e2e"
    if [[ $E2E_BUILD == local ]]; then
        log "building $image_repo:$tag_ts locally and pushing it to $login"
        docker build -t "$login/$image_repo:$tag_ts" -t "$login/$image_repo:latest" -f "$stage/Dockerfile.e2e" "$stage"
        az acr login -n "$acr"
        docker push "$login/$image_repo:$tag_ts"
        docker push "$login/$image_repo:latest"
    else
        log "building $image_repo:$tag_ts in Azure Container Registry (the first build takes several minutes)"
        # az acr build resolves --file against the current directory, not the build context
        (cd "$stage" && az acr build -r "$acr" -t "$image_repo:$tag_ts" -t "$image_repo:latest" -f Dockerfile.e2e .) \
            || die "ACR build failed (ACR Tasks are unavailable on some subscriptions); retry with E2E_BUILD='local' in $env_file"
    fi
    E2E_IMAGE_TAG=$tag_ts
    save_env
}

image() { echo "$(az acr show -n "$acr" --query loginServer -o tsv)/$image_repo:${1:-$E2E_IMAGE_TAG}"; }

# --- create -------------------------------------------------------------------

create() {
    az account show >/dev/null 2>&1 || die "not logged in; run 'az login'"
    local targets=${*:-aci aca}
    for t in $targets; do [[ $t == aci || $t == aca || $t == aks ]] || die "unknown target $t"; done

    if [[ -f "$env_file" ]]; then
        load_env
        log "adding targets to the existing setup in $rg"
        for t in $targets; do has_target "$t" || E2E_TARGETS="$E2E_TARGETS $t"; done
    else
        E2E_RESOURCE_GROUP=${RESOURCE_GROUP:-azuresdk-jl-id-e2e}
        E2E_LOCATION=${LOCATION:-westeurope}
        E2E_SUFFIX=$(od -An -N4 -tx4 /dev/urandom | tr -d ' \n' | cut -c1-6)
        E2E_TARGETS=$targets
        E2E_BUILD=${BUILD:-acr}
        names
        [[ $(az group exists -n "$rg") == "false" ]] || die "resource group $rg exists already; set RESOURCE_GROUP"
        log "subscription: $(az account show --query name -o tsv)"
        log "creating resource group $rg in $loc"
        az group create -n "$rg" -l "$loc" --tags "$tag" -o none
    fi
    E2E_TARGETS=$(echo $E2E_TARGETS)   # normalise spaces
    save_env

    # Subscriptions that never used these services need their resource providers registered (free).
    local providers="Microsoft.ManagedIdentity Microsoft.EventHub Microsoft.ServiceBus Microsoft.Storage Microsoft.ContainerRegistry"
    has_target aci && providers+=" Microsoft.ContainerInstance"
    has_target aca && providers+=" Microsoft.App Microsoft.OperationalInsights"
    has_target aks && providers+=" Microsoft.ContainerService"
    for p in $providers; do
        if [[ $(az provider show -n "$p" --query registrationState -o tsv) != Registered ]]; then
            log "registering resource provider $p in the subscription"
            az provider register -n "$p" --wait -o none
        fi
    done

    log "creating the user-assigned identity $uami"
    az identity create -g "$rg" -n "$uami" -l "$loc" -o none
    log "creating Event Hubs namespace $eh_ns with event hub $hub (Entra ID only)"
    az eventhubs namespace create -g "$rg" -n "$eh_ns" -l "$loc" --sku Basic --disable-local-auth true -o none
    az eventhubs eventhub create -g "$rg" --namespace-name "$eh_ns" --name "$hub" --partition-count 1 \
        --cleanup-policy Delete --retention-time-in-hours 1 -o none
    log "creating Service Bus namespace $sb_ns with queue $queue (Entra ID only)"
    az servicebus namespace create -g "$rg" -n "$sb_ns" -l "$loc" --sku Basic --disable-local-auth true -o none
    az servicebus queue create -g "$rg" --namespace-name "$sb_ns" --name "$queue" -o none
    log "creating storage account $storage with container $results (Entra ID only)"
    az storage account create -g "$rg" -n "$storage" -l "$loc" --sku Standard_LRS --kind StorageV2 \
        --allow-blob-public-access false --allow-shared-key-access false --min-tls-version TLS1_2 -o none
    az storage container-rm create -g "$rg" --storage-account "$storage" --name "$results" -o none
    if ! az acr show -g "$rg" -n "$acr" -o none 2>/dev/null; then
        log "creating container registry $acr"
        az acr create -g "$rg" -n "$acr" -l "$loc" --sku Basic -o none
    fi

    local uami_principal uami_id me
    uami_principal=$(az identity show -g "$rg" -n "$uami" --query principalId -o tsv)
    uami_id=$(az identity show -g "$rg" -n "$uami" --query id -o tsv)
    grant_data_roles "$uami_principal" ServicePrincipal "the user-assigned identity"
    role "$uami_principal" ServicePrincipal AcrPull "$(az acr show -n "$acr" --query id -o tsv)"
    if me=$(signed_in_object_id); then
        log "granting you 'Storage Blob Data Reader' to read the test results"
        role "$me" User "Storage Blob Data Reader" "$(az storage account show -g "$rg" -n "$storage" --query id -o tsv)"
    fi

    [[ -n ${E2E_IMAGE_TAG:-} ]] || build_image
    local env_vars
    env_vars=$(test_env)

    # Container Instances' IMDS refuses tokens for the system-assigned identity when a group also has a
    # user-assigned one (HTTP 500 "Received invalid token", also with plain curl), so each kind gets its own group.
    if has_target aci && ! az container show -g "$rg" -n "$aci" -o none 2>/dev/null; then
        log "creating container instance $aci (user-assigned identity)"
        # The image pull fails until the new AcrPull role has propagated, hence the retries.
        local i
        for i in 1 2 3 4 5 6; do
            # shellcheck disable=SC2086
            az container create -g "$rg" -n "$aci" -l "$loc" --image "$(image latest)" --os-type Linux \
                --cpu 2 --memory 4 --restart-policy Never --assign-identity "$uami_id" --acr-identity "$uami_id" \
                -e $env_vars E2E_TARGET=aci AZURE_CLIENT_ID="$(az identity show -g "$rg" -n "$uami" --query clientId -o tsv)" \
                E2E_CREDENTIALS=mi-client-id,mi-object-id,mi-resource-id,default -o none && break
            (( i < 6 )) || die "could not create the container instance"
            log "retrying in 30 s (the registry permission may not have propagated yet)"
            sleep 30
        done
    fi
    if has_target aci && ! az container show -g "$rg" -n "$aci_sys" -o none 2>/dev/null; then
        log "creating container instance $aci_sys (system-assigned identity)"
        # A system identity only exists once its group does, so it cannot pull the image: use a pull-only registry token.
        az acr token show -r "$acr" -n aci-pull -o none 2>/dev/null \
            || az acr token create -r "$acr" -n aci-pull --scope-map _repositories_pull --no-passwords -o none
        local pull_pw
        pull_pw=$(az acr token credential generate -r "$acr" -n aci-pull --password1 --expiration-in-days 30 \
            --query "passwords[0].value" -o tsv)
        # It runs once right away, before its identity has any roles; `run` starts it again.
        # shellcheck disable=SC2086
        az container create -g "$rg" -n "$aci_sys" -l "$loc" --image "$(image latest)" --os-type Linux \
            --cpu 2 --memory 4 --restart-policy Never --assign-identity "[system]" \
            --registry-login-server "$(az acr show -n "$acr" --query loginServer -o tsv)" \
            --registry-username aci-pull --registry-password "$pull_pw" \
            -e $env_vars E2E_TARGET=aci-sys E2E_CREDENTIALS=mi-system,default -o none
        grant_data_roles "$(az container show -g "$rg" -n "$aci_sys" --query identity.principalId -o tsv)" \
            ServicePrincipal "the container instance's system identity"
    fi

    if has_target aca && ! az containerapp job show -g "$rg" -n "$aca_job" -o none 2>/dev/null; then
        log "creating Container Apps environment $aca_env (takes a few minutes)"
        az containerapp env create -g "$rg" -n "$aca_env" -l "$loc" --logs-destination none -o none
        log "creating Container Apps job $aca_job (system- and user-assigned identity)"
        # shellcheck disable=SC2086
        az containerapp job create -g "$rg" -n "$aca_job" --environment "$aca_env" \
            --trigger-type Manual --replica-timeout 1800 --replica-retry-limit 0 \
            --parallelism 1 --replica-completion-count 1 \
            --image "$(image)" --cpu 2 --memory 4Gi \
            --registry-server "$(az acr show -n "$acr" --query loginServer -o tsv)" --registry-identity "$uami_id" \
            --mi-system-assigned --mi-user-assigned "$uami_id" \
            --env-vars $env_vars E2E_TARGET=aca \
            AZURE_CLIENT_ID="$(az identity show -g "$rg" -n "$uami" --query clientId -o tsv)" \
            E2E_CREDENTIALS=mi-system,mi-client-id,mi-object-id,mi-resource-id,default -o none
        grant_data_roles "$(az containerapp job show -g "$rg" -n "$aca_job" --query identity.principalId -o tsv)" \
            ServicePrincipal "the Container Apps job's system identity"
    fi

    if has_target aks && ! az aks show -g "$rg" -n "$aks" -o none 2>/dev/null; then
        log "creating AKS cluster $aks with one ${AKS_VM_SIZE:-Standard_B2s} node (takes ~10 minutes)"
        az aks create -g "$rg" -n "$aks" -l "$loc" --tier free --node-count 1 \
            --node-vm-size "${AKS_VM_SIZE:-Standard_B2s}" --enable-oidc-issuer --enable-workload-identity \
            --attach-acr "$acr" --no-ssh-key -o none
        log "trusting the cluster's service account $k8s_ns/$k8s_ns in the user-assigned identity"
        az identity federated-credential create -g "$rg" --identity-name "$uami" --name aks-e2e \
            --issuer "$(az aks show -g "$rg" -n "$aks" --query oidcIssuerProfile.issuerUrl -o tsv)" \
            --subject "system:serviceaccount:$k8s_ns:$k8s_ns" --audiences api://AzureADTokenExchange -o none
        grant_data_roles "$(az aks show -g "$rg" -n "$aks" --query identityProfile.kubeletidentity.objectId -o tsv)" \
            ServicePrincipal "the cluster's kubelet identity"
    fi

    log "done. New role assignments can take ~5 minutes to apply; then run:"
    echo "  $0 run" >&2
}

# --- run ----------------------------------------------------------------------

# Print <target>/latest.txt once it is newer than $1 (epoch seconds); retries while roles propagate.
fetch_results() {  # since target
    local modified i
    for i in $(seq 1 12); do
        modified=$(az storage blob show --auth-mode login --account-name "$storage" -c "$results" \
            -n "$2/latest.txt" --query properties.lastModified -o tsv 2>/dev/null || true)
        if [[ -n $modified ]] && (( $(date -d "$modified" +%s) >= $1 )); then
            local f
            f=$(tempdir)/latest.txt
            if az storage blob download --auth-mode login --account-name "$storage" -c "$results" \
                -n "$2/latest.txt" --file "$f" --no-progress -o none 2>/dev/null; then
                cat "$f"
                return 0
            fi
        fi
        sleep 10
    done
    echo "(no results in $storage/$results/$2/latest.txt; no credential could write there, or you lack read access yet)"
    return 1
}

run_container() {  # group label
    log "$2: starting container instance $1"
    az container restart -g "$rg" -n "$1" -o none 2>/dev/null || az container start -g "$rg" -n "$1" -o none
    local state code
    while state=$(az container show -g "$rg" -n "$1" --query "containers[0].instanceView.currentState.state" -o tsv) \
        && [[ $state != Terminated ]]; do
        sleep 10
    done
    code=$(az container show -g "$rg" -n "$1" --query "containers[0].instanceView.currentState.exitCode" -o tsv)
    log "$2: finished with exit code $code"
    [[ $code == 0 ]]
}
run_aci() { run_container "$aci" aci; }
run_aci_sys() { run_container "$aci_sys" aci-sys; }

run_aca() {
    log "aca: updating job $aca_job to image $E2E_IMAGE_TAG and starting it"
    az containerapp job update -g "$rg" -n "$aca_job" --image "$(image)" -o none
    local exec status
    exec=$(az containerapp job start -g "$rg" -n "$aca_job" --query name -o tsv)
    while status=$(az containerapp job execution show -g "$rg" -n "$aca_job" --job-execution-name "$exec" \
        --query properties.status -o tsv) && [[ $status == Running || $status == Processing || -z $status ]]; do
        sleep 10
    done
    log "aca: execution $exec finished: $status"
    [[ $status == Succeeded ]]
}

run_aks() {
    local job="e2e-${E2E_IMAGE_TAG}" manifest env_yaml kubelet_client
    kubelet_client=$(az aks show -g "$rg" -n "$aks" --query identityProfile.kubeletidentity.clientId -o tsv)
    env_yaml=$(for kv in $(test_env) E2E_TARGET=aks E2E_KUBELET_CLIENT_ID="$kubelet_client" \
        E2E_CREDENTIALS=workload,default,mi-client-id,mi-imds-kubelet; do
        printf '            - name: %s\n              value: "%s"\n' "${kv%%=*}" "${kv#*=}"
    done)
    manifest=$(tempdir)
    cat >"$manifest/e2e.yaml" <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $k8s_ns
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $k8s_ns
  namespace: $k8s_ns
  annotations:
    azure.workload.identity/client-id: "$(az identity show -g "$rg" -n "$uami" --query clientId -o tsv)"
---
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $k8s_ns
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        azure.workload.identity/use: "true"
    spec:
      serviceAccountName: $k8s_ns
      restartPolicy: Never
      containers:
        - name: test
          image: $(image)
          resources:
            requests: {cpu: 500m, memory: 1Gi}
          env:
$env_yaml
EOF
    log "aks: running job $job (through 'az aks command invoke'; no local kubectl needed)"
    local out
    out=$(az aks command invoke -g "$rg" -n "$aks" --file "$manifest/e2e.yaml" --command "
        kubectl apply -f e2e.yaml >/dev/null
        for i in \$(seq 1 180); do
            s=\$(kubectl get job $job -n $k8s_ns -o jsonpath='{.status.succeeded}{.status.failed}')
            [ -n \"\$s\" ] && break; sleep 5
        done
        kubectl logs job/$job -n $k8s_ns | tail -n 60
        [ \"\$(kubectl get job $job -n $k8s_ns -o jsonpath='{.status.succeeded}')\" = 1 ]")
    echo "$out"
    grep -q "exitcode=0" <<<"$out"   # az aks command invoke itself exits 0 either way
}

run() {
    load_env
    local build=1 targets=()
    for a in "$@"; do
        case $a in
            --no-build) build=0 ;;
            aci | aca | aks) has_target "$a" || die "target $a was not created; run '$0 create $a'"; targets+=("$a") ;;
            *) die "unknown argument $a" ;;
        esac
    done
    (( ${#targets[@]} )) || read -r -a targets <<<"$E2E_TARGETS"
    (( build )) && build_image
    local failed=() since labels=()
    for t in "${targets[@]}"; do
        [[ $t == aci ]] && labels+=(aci aci-sys) || labels+=("$t")
    done
    for t in "${labels[@]}"; do
        since=$(date +%s)
        "run_${t//-/_}" || failed+=("$t")
        echo; log "$t: results"
        fetch_results "$since" "$t" || true
        echo
    done
    if (( ${#failed[@]} )); then
        log "FAILED: ${failed[*]}"
        return 1
    fi
    log "all targets passed: ${labels[*]}"
}

# --- show / delete ------------------------------------------------------------

show() {
    load_env
    echo "resource group:  $rg ($loc)"
    echo "targets:         $E2E_TARGETS"
    echo "image:           ${E2E_IMAGE_TAG:-not built yet} (build: $E2E_BUILD)"
    echo "event hubs:      $eh_ns.servicebus.windows.net/$hub"
    echo "service bus:     $sb_ns.servicebus.windows.net/$queue"
    echo "storage:         https://$storage.blob.core.windows.net/$results"
}

delete() {
    load_env
    if [[ $(az group exists -n "$rg") == "false" ]]; then
        log "resource group $rg no longer exists"
    else
        [[ $(az group show -n "$rg" --query tags.purpose -o tsv) == "${tag#purpose=}" ]] \
            || die "resource group $rg lacks the tag $tag; not deleting it"
        if [[ ${1:-} != -y && ${1:-} != --yes ]]; then
            local answer
            read -r -p "Delete resource group $rg and everything in it? [y/N] " answer
            [[ $answer == [yY]* ]] || die "aborted"
        fi
        log "deleting resource group $rg (takes several minutes)"
        az group delete -n "$rg" --yes
    fi
    rm -f "$env_file"
    log "done"
}

cmd=${1:-}
shift || true
case $cmd in
    create) create "$@" ;;
    run) run "$@" ;;
    show) show ;;
    delete) delete "${1:-}" ;;
    *) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac

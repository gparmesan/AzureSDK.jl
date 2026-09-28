# Image for the managed identity end-to-end tests (see resources.sh). The build
# context is a copy of the repository's tracked and non-ignored files.
FROM julia:1.12

WORKDIR /app
# Compile package images for several CPU generations: Azure hands out varying hardware.
ENV JULIA_DEPOT_PATH=/opt/julia-depot JULIA_PKG_PRECOMPILE_AUTO=0 \
    JULIA_CPU_TARGET="generic;sandybridge,-xsaveopt,clone_all;haswell,-rdrnd,base(1)"
COPY Project.toml ./
COPY src ./src
RUN julia --project=/app -e 'using Pkg; Pkg.instantiate(); Pkg.precompile()'
COPY scripts/identity-e2e ./scripts/identity-e2e

CMD ["julia", "--project=/app", "--threads=auto", "scripts/identity-e2e/identity_test.jl"]

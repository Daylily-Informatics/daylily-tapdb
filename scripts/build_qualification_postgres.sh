#!/usr/bin/env bash
# Build the exact release-gate server, independently of a package manager's
# moving PostgreSQL 16 package. Caller supplies new absolute artifact paths.
set -euo pipefail

if [[ $# != 3 || "$2" != /* || "$3" != /* ]]; then
    echo "Usage: $0 VERSION ABSOLUTE_NEW_PREFIX ABSOLUTE_EMPTY_BUILD_DIRECTORY" >&2
    exit 2
fi
qualification_version="$1"
qualification_prefix="$2"
qualification_build="$3"
case "$qualification_version" in
    16.13) qualification_expected_hash=dc2ddbbd245c0265a689408e3d2f2f3f9ba2da96bd19318214b313cdd9797287 ;;
    17.11) qualification_expected_hash=dd27f2b3c59e73ed14aa3324901242bf69a032a6347805f274e6260322d42979 ;;
    *) echo "Unsupported qualification PostgreSQL version: $qualification_version" >&2; exit 2 ;;
esac
if [[ -e "$qualification_prefix" || -e "$qualification_build" ]]; then
    echo "Qualification prefix and build directory must not already exist" >&2
    exit 2
fi
mkdir -p "$qualification_build"
cd "$qualification_build"
curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
    -o "postgresql-$qualification_version.tar.bz2" \
    "https://ftp.postgresql.org/pub/source/v$qualification_version/postgresql-$qualification_version.tar.bz2"
qualification_hash="$(shasum -a 256 "postgresql-$qualification_version.tar.bz2")"
if [[ "${qualification_hash%% *}" != "$qualification_expected_hash" ]]; then
    echo "PostgreSQL $qualification_version source checksum mismatch" >&2
    exit 1
fi
tar -xjf "postgresql-$qualification_version.tar.bz2"
cd "postgresql-$qualification_version"
./configure --prefix="$qualification_prefix" --without-readline --without-zlib --without-icu
make -s -j 4
make -s install
"$qualification_prefix/bin/postgres" --version

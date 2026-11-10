#!/bin/sh
set -eu
output="${1:?license output directory required}"
mkdir -p "$output"
go list -m -f '{{.Path}}|{{.Dir}}' all | while IFS='|' read -r module directory; do
    [ -n "$directory" ] || continue
    for notice in "$directory"/LICENSE* "$directory"/COPYING* "$directory"/NOTICE*; do
        [ -f "$notice" ] || continue
        mkdir -p "$output/$module"
        cp "$notice" "$output/$module/$(basename "$notice")"
    done
done
cp dependencies/sipgo.json "$output/sipgo-source.json"

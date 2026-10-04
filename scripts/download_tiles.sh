#!/bin/bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Humble-Helper-96
#
# Part of the terrain_builder pipeline — see HOW_TO_USE.md for full documentation.

# =============================================================================
# download_tiles.sh
# Compact USGS DEM tile downloader — sourced by the build scripts
#
# PURPOSE:
#   Replaces `wget -c -i LIST -P DIR`, whose per-tile progress bars flood the
#   build logs, with one summary line per tile:
#
#     [  3/12] OK    USGS_13_n46w119_20240416.tif   412.3 MB   38s
#     [  4/12] SKIP  USGS_13_n46w120_20240416.tif   398.1 MB   already downloaded
#     [  5/12] FAIL  USGS_13_n47w117_20240401.tif   ERROR 404: Not Found.
#
# USAGE:
#   source "${SCRIPT_DIR}/download_tiles.sh"
#   download_tile_list USGS_DL_Lists/WA_data.txt raw_dem/
#
# BEHAVIOUR:
#   - Each URL is fetched with `wget -c` (resumes partial files; a file that
#     is already complete is reported as SKIP)
#   - Like `wget -i`, a failed tile does not stop the remaining downloads;
#     the function prints a summary and returns non-zero if any tile failed
#   - Blank lines, '#' comments, duplicate URLs and Windows line endings in
#     LIST are ignored
# =============================================================================

# Format a byte count as MB or GB
_dl_fmt_size() {
    awk -v b="$1" 'BEGIN {
        if (b >= 1073741824) printf "%.1f GB", b / 1073741824
        else                 printf "%.1f MB", b / 1048576
    }'
}

# Format a duration in seconds as "Xh Ym Zs", omitting leading zero units
_dl_fmt_time() {
    local s=$1
    if (( s >= 3600 )); then
        printf '%dh %dm %ds' $(( s / 3600 )) $(( (s % 3600) / 60 )) $(( s % 60 ))
    elif (( s >= 60 )); then
        printf '%dm %ds' $(( s / 60 )) $(( s % 60 ))
    else
        printf '%ds' "$s"
    fi
}

# download_tile_list LIST DEST_DIR
download_tile_list() {
    local list="$1" dest="$2"
    local urls=() line url name file tmp_err
    local i total width rc before after elapsed start tile_start reason
    local ok=0 skipped=0 bytes=0 dupes=0 failed=()
    local -A seen=()

    mkdir -p "$dest"

    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -z "$line" || "$line" == \#* ]] && continue
        if [[ -n "${seen[$line]+x}" ]]; then
            dupes=$(( dupes + 1 ))
            continue
        fi
        seen[$line]=1
        urls+=("$line")
    done < "$list"

    total=${#urls[@]}
    if (( total == 0 )); then
        echo "[WARN]  No URLs found in ${list}"
        return 1
    fi

    (( dupes > 0 )) && echo "[WARN]  Ignoring ${dupes} duplicate URL(s) in ${list}"

    width=${#total}
    start=${SECONDS}
    tmp_err="$(mktemp)"

    for (( i = 0; i < total; i++ )); do
        url="${urls[$i]}"
        name="${url##*/}"
        name="${name%%\?*}"
        file="${dest%/}/${name}"

        before=-1
        [[ -f "$file" ]] && before=$(stat -c %s "$file" 2>/dev/null || echo -1)

        tile_start=${SECONDS}
        rc=0
        wget -c -nv -P "$dest" "$url" 2>"$tmp_err" || rc=$?
        elapsed=$(( SECONDS - tile_start ))

        after=0
        [[ -f "$file" ]] && after=$(stat -c %s "$file" 2>/dev/null || echo 0)

        if (( rc == 0 )); then
            bytes=$(( bytes + after ))
            if (( before == after )); then
                skipped=$(( skipped + 1 ))
                printf '[%*d/%d] SKIP  %-34s %10s   already downloaded\n' \
                    "$width" $(( i + 1 )) "$total" "$name" "$(_dl_fmt_size "$after")"
            else
                ok=$(( ok + 1 ))
                printf '[%*d/%d] OK    %-34s %10s   %s\n' \
                    "$width" $(( i + 1 )) "$total" "$name" "$(_dl_fmt_size "$after")" \
                    "$(_dl_fmt_time "$elapsed")"
            fi
        else
            failed+=("$url")
            # Last meaningful wget message, without its timestamp prefix
            reason="$(grep -v '^[[:space:]]*$' "$tmp_err" | tail -n 1 \
                | sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8} //')"
            [[ -z "$reason" ]] && reason="wget exit code ${rc}"
            printf '[%*d/%d] FAIL  %-34s %s\n' \
                "$width" $(( i + 1 )) "$total" "$name" "$reason"
        fi
    done

    rm -f "$tmp_err"

    echo "        ${ok} downloaded, ${skipped} already present, ${#failed[@]} failed" \
         "— $(_dl_fmt_size "$bytes") in $(_dl_fmt_time $(( SECONDS - start )))"

    if (( ${#failed[@]} > 0 )); then
        echo "        Failed URLs:"
        printf '          %s\n' "${failed[@]}"
        return 1
    fi
    return 0
}

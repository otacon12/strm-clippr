#!/bin/bash
# extract_audio.sh — produce the audio upload artifact from a recording (Mac side).
#
# Audio-only transport ruling (2026-08-06, operator "GO"): audio ships to the
# server, NEVER video. Three OOMs proved a 2.6 GB video cannot pass through
# n8n's heap under the container's 4 GB cap. Video stays archival on the Mac.
#
# Multi-stream aware (2026-08-06): OBS recordings can carry MULTIPLE audio
# tracks (e.g. an unused track of digital silence next to the real mix). A
# blind default-selection extract could ship the silent track — the exact
# vod-6 failure this script exists to prevent. So: measure EVERY audio stream,
# select the loudest, extract only that one, and prove the output matches it.
#
# Usage: bash app/scripts/extract_audio.sh <video-file>
# Output: <same-dir>/<basename>.m4a, then upload it to Drive folder to_clip.
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }

# --- 1. args ---------------------------------------------------------------
[ $# -eq 1 ] || die "usage: bash app/scripts/extract_audio.sh <video-file>"
IN="$1"
[ -f "$IN" ] || die "input file not found: $IN"

# --- 2. enumerate audio streams; input must HAVE audio ---------------------
# A whole session once recorded silent (clpr D-028, vod 6). Refuse loudly
# rather than produce an empty artifact.
AUD_LIST=$(ffprobe -v error -select_streams a -show_entries stream=index,codec_name \
  -of csv=p=0 "$IN" || true)
if [ -z "$AUD_LIST" ]; then
  echo "REFUSED: input has NO audio stream: $IN" >&2
  echo "A silent recording produced an empty artifact once before (clpr D-028, vod 6)." >&2
  echo "Nothing was extracted. Check the recorder's audio source and re-record." >&2
  exit 1
fi

N_STREAMS=0
while IFS=, read -r gidx codec; do
  S_GIDX[$N_STREAMS]="$gidx"
  S_CODEC[$N_STREAMS]="$codec"
  N_STREAMS=$((N_STREAMS+1))
done <<< "$AUD_LIST"

IN_DUR=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$IN")
[ -n "$IN_DUR" ] || die "could not read input duration via ffprobe: $IN"
IN_MIN=$(awk -v d="$IN_DUR" 'BEGIN{printf "%.1f", d/60}')

echo "INPUT   $IN"
echo "  duration      : ${IN_MIN} min (${IN_DUR}s)"
echo "  audio streams : $N_STREAMS"

# --- 2b. per-stream volumedetect; select the loudest -----------------------
# One volumedetect pass PER stream (-map 0:a:<i>), so each parse sees exactly
# one mean_volume line. A combined pass emits one line per stream and the awk
# comparison dies on the embedded newline (live failure, mainoooooooooo.mp4).
SEL=-1
SEL_VOL=""
i=0
while [ "$i" -lt "$N_STREAMS" ]; do
  VD_LOG=$(mktemp)
  ffmpeg -hide_banner -nostats -nostdin -i "$IN" -map "0:a:$i" -af volumedetect -f null - 2>"$VD_LOG" \
    || { cat "$VD_LOG" >&2; rm -f "$VD_LOG"; die "volumedetect pass failed on stream a:$i of: $IN"; }
  MV=$(awk '/mean_volume:/ {print $(NF-1)}' "$VD_LOG")
  rm -f "$VD_LOG"
  [ -n "$MV" ] || die "could not parse mean_volume for stream a:$i (a check that cannot fail loudly is not a check)"
  [ "$(printf '%s' "$MV" | wc -l | tr -d ' ')" = "0" ] \
    || die "multiple mean_volume lines for single mapped stream a:$i — parse ambiguity, refusing to guess"

  # -inf / non-numeric mean_volume is treated as silence (refuse, don't coerce).
  SILENT=$(awk -v mv="$MV" 'BEGIN{ if (mv+0 != mv || mv <= -80) print 1; else print 0 }')
  [ "$MV" = "-inf" ] && SILENT=1
  if [ "$SILENT" = "1" ]; then
    VERDICT="silent"
  else
    VERDICT="LIVE"
    if [ "$SEL" -lt 0 ] || awk -v a="$MV" -v b="$SEL_VOL" 'BEGIN{exit !(a+0 > b+0)}'; then
      SEL=$i
      SEL_VOL="$MV"
    fi
  fi
  S_VOL[$i]="$MV"
  S_VERDICT[$i]="$VERDICT"
  echo "  a:$i  index=${S_GIDX[$i]}  codec=${S_CODEC[$i]}  mean_volume=${MV} dB  ${VERDICT}"
  i=$((i+1))
done

if [ "$SEL" -lt 0 ]; then
  echo "REFUSED: all $N_STREAMS audio streams are digitally silent (mean_volume below -80 dB)." >&2
  echo "This is the vod 6 signature (clpr D-028): a whole session recorded silent." >&2
  echo "Nothing was extracted. Check the recorder's audio source and re-record." >&2
  exit 1
fi

ACODEC="${S_CODEC[$SEL]}"
OTHERS=""
i=0
while [ "$i" -lt "$N_STREAMS" ]; do
  if [ "$i" -ne "$SEL" ]; then
    OTHERS="${OTHERS:+$OTHERS; }a:$i was ${S_VOL[$i]} dB ${S_VERDICT[$i]}"
  fi
  i=$((i+1))
done
if [ -n "$OTHERS" ]; then
  echo "SELECTED audio stream a:$SEL (mean ${SEL_VOL} dB; ${OTHERS})"
else
  echo "SELECTED audio stream a:$SEL (mean ${SEL_VOL} dB; only audio stream)"
fi

# --- 3. extract ONLY the selected stream: stream copy, aac fallback --------
OUT_DIR=$(dirname "$IN")
BASE=$(basename "$IN")
OUT="$OUT_DIR/${BASE%.*}.m4a"

if ffmpeg -v error -nostdin -y -i "$IN" -map "0:a:$SEL" -c:a copy "$OUT" 2>/dev/null; then
  echo "EXTRACT lossless stream copy (-map 0:a:$SEL -c:a copy) -> $OUT"
else
  echo "NOTE: stream copy to .m4a failed for codec '$ACODEC' (container/codec combo cannot remux)."
  echo "NOTE: falling back to re-encode: -c:a aac -b:a 128k"
  rm -f "$OUT"
  ffmpeg -v error -nostdin -y -i "$IN" -map "0:a:$SEL" -c:a aac -b:a 128k "$OUT" \
    || { rm -f "$OUT"; die "aac fallback encode failed on: $IN"; }
  echo "EXTRACT aac 128k re-encode (-map 0:a:$SEL) -> $OUT"
fi

# --- 4. verify the OUTPUT --------------------------------------------------
[ -f "$OUT" ] || die "output missing after extraction: $OUT"
OUT_BYTES=$(wc -c < "$OUT" | tr -d ' ')
[ "$OUT_BYTES" -gt 10240 ] || { rm -f "$OUT"; die "output suspiciously small (${OUT_BYTES} bytes): $OUT"; }

OUT_ACODEC=$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name \
  -of default=nw=1:nk=1 "$OUT" || true)
[ -n "$OUT_ACODEC" ] || { rm -f "$OUT"; die "output has no audio stream: $OUT"; }

OUT_DUR=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$OUT")
[ -n "$OUT_DUR" ] || { rm -f "$OUT"; die "could not read output duration via ffprobe: $OUT"; }
echo "VERIFY  input duration ${IN_DUR}s vs output duration ${OUT_DUR}s"
DUR_OK=$(awk -v a="$IN_DUR" -v b="$OUT_DUR" 'BEGIN{d=a-b; if (d<0) d=-d; print (d<=2) ? 1 : 0}')
[ "$DUR_OK" = "1" ] || { rm -f "$OUT"; die "duration mismatch > 2s (input ${IN_DUR}s, output ${OUT_DUR}s)"; }

# Prove the RIGHT track got copied, not asserted: the output's own measured
# mean_volume must sit within 3 dB of the selected input stream's.
VD_LOG=$(mktemp)
ffmpeg -hide_banner -nostats -nostdin -i "$OUT" -af volumedetect -f null - 2>"$VD_LOG" \
  || { cat "$VD_LOG" >&2; rm -f "$VD_LOG"; rm -f "$OUT"; die "volumedetect pass failed on output: $OUT"; }
OUT_MV=$(awk '/mean_volume:/ {print $(NF-1)}' "$VD_LOG")
rm -f "$VD_LOG"
[ -n "$OUT_MV" ] || { rm -f "$OUT"; die "could not parse mean_volume from output volumedetect (a check that cannot fail loudly is not a check)"; }
echo "VERIFY  output mean_volume ${OUT_MV} dB vs selected input stream a:$SEL ${SEL_VOL} dB"
MV_OK=$(awk -v a="$OUT_MV" -v b="$SEL_VOL" 'BEGIN{ if (a+0 != a) print 0; else {d=a-b; if (d<0) d=-d; print (d<=3) ? 1 : 0} }')
[ "$MV_OK" = "1" ] || { rm -f "$OUT"; die "output mean_volume ${OUT_MV} dB is not within 3 dB of selected stream a:$SEL (${SEL_VOL} dB) — wrong track extracted?"; }

DUR_MIN=$(awk -v d="$OUT_DUR" 'BEGIN{printf "%.1f", d/60}')
SIZE_MB=$(awk -v b="$OUT_BYTES" 'BEGIN{printf "%.1f", b/1048576}')

echo "RESULT extract_audio ok=1 out=\"$OUT\" duration_min=$DUR_MIN size_mb=$SIZE_MB stream=a:$SEL streams_total=$N_STREAMS"
echo "Next step: Upload this .m4a to Drive folder to_clip"

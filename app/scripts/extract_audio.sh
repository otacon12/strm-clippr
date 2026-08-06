#!/bin/bash
# extract_audio.sh — produce the audio upload artifact from a recording (Mac side).
#
# Audio-only transport ruling (2026-08-06, operator "GO"): audio ships to the
# server, NEVER video. Three OOMs proved a 2.6 GB video cannot pass through
# n8n's heap under the container's 4 GB cap. Video stays archival on the Mac.
#
# Usage: bash app/scripts/extract_audio.sh <video-file>
# Output: <same-dir>/<basename>.m4a, then upload it to Drive folder to_clip.
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }

# --- 1. args ---------------------------------------------------------------
[ $# -eq 1 ] || die "usage: bash app/scripts/extract_audio.sh <video-file>"
IN="$1"
[ -f "$IN" ] || die "input file not found: $IN"

# --- 2. input must HAVE audio, and it must not be silence ------------------
# A whole session once recorded silent (clpr D-028, vod 6). Refuse loudly
# rather than produce an empty artifact.
ACODEC=$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name \
  -of default=nw=1:nk=1 "$IN" || true)
if [ -z "$ACODEC" ]; then
  echo "REFUSED: input has NO audio stream: $IN" >&2
  echo "A silent recording produced an empty artifact once before (clpr D-028, vod 6)." >&2
  echo "Nothing was extracted. Check the recorder's audio source and re-record." >&2
  exit 1
fi

IN_DUR=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$IN")
[ -n "$IN_DUR" ] || die "could not read input duration via ffprobe: $IN"
IN_MIN=$(awk -v d="$IN_DUR" 'BEGIN{printf "%.1f", d/60}')

# Full volumedetect pass (audio only, decoded once, no output file).
VD_LOG=$(mktemp)
ffmpeg -hide_banner -nostats -i "$IN" -vn -af volumedetect -f null - 2>"$VD_LOG" \
  || { cat "$VD_LOG" >&2; rm -f "$VD_LOG"; die "volumedetect pass failed on: $IN"; }
MEAN_VOL=$(awk '/mean_volume:/ {print $(NF-1)}' "$VD_LOG")
rm -f "$VD_LOG"
[ -n "$MEAN_VOL" ] || die "could not parse mean_volume from volumedetect output (a check that cannot fail loudly is not a check)"

echo "INPUT   $IN"
echo "  audio codec : $ACODEC"
echo "  duration    : ${IN_MIN} min (${IN_DUR}s)"
echo "  mean_volume : ${MEAN_VOL} dB"

# -inf / non-numeric mean_volume is treated as silence (refuse, don't coerce).
SILENT=$(awk -v mv="$MEAN_VOL" 'BEGIN{ if (mv+0 != mv || mv <= -80) print 1; else print 0 }')
if [ "$MEAN_VOL" = "-inf" ] || [ "$SILENT" = "1" ]; then
  echo "REFUSED: mean_volume ${MEAN_VOL} dB is below -80 dB — digital silence." >&2
  echo "This is the vod 6 signature (clpr D-028): a whole session recorded silent." >&2
  echo "Nothing was extracted. Check the recorder's audio source and re-record." >&2
  exit 1
fi

# --- 3. extract: lossless stream copy, aac fallback ------------------------
OUT_DIR=$(dirname "$IN")
BASE=$(basename "$IN")
OUT="$OUT_DIR/${BASE%.*}.m4a"

if ffmpeg -v error -y -i "$IN" -vn -c:a copy "$OUT" 2>/dev/null; then
  echo "EXTRACT lossless stream copy (-c:a copy) -> $OUT"
else
  echo "NOTE: stream copy to .m4a failed for codec '$ACODEC' (container/codec combo cannot remux)."
  echo "NOTE: falling back to re-encode: -c:a aac -b:a 128k"
  rm -f "$OUT"
  ffmpeg -v error -y -i "$IN" -vn -c:a aac -b:a 128k "$OUT" \
    || { rm -f "$OUT"; die "aac fallback encode failed on: $IN"; }
  echo "EXTRACT aac 128k re-encode -> $OUT"
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

DUR_MIN=$(awk -v d="$OUT_DUR" 'BEGIN{printf "%.1f", d/60}')
SIZE_MB=$(awk -v b="$OUT_BYTES" 'BEGIN{printf "%.1f", b/1048576}')

echo "RESULT extract_audio ok=1 out=\"$OUT\" duration_min=$DUR_MIN size_mb=$SIZE_MB"
echo "Next step: Upload this .m4a to Drive folder to_clip"

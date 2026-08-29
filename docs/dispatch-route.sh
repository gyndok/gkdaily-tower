# GK Daily routes for ~/clawd/dispatch.sh
#
# dispatch.sh is a local, mixed-purpose router that is NOT tracked in any git
# repo, so this file is the reconstructible copy of the GK Daily portion.
# Paste the block below into dispatch.sh immediately BEFORE the line:
#
#     # ─── Medical Practice ───────────────────────────────────────────────
#
# Position matters: it must come before the generic routes, or an episode
# topic gets swallowed by "^news" or the "not working" bug-triage route.
#
# Known trap, fixed 2026-08-28: dispatch.sh did not parse at all. Line 438
# read `"${QUERY:-show today's events}"` — an apostrophe inside a ${VAR:-default}
# opens a quote bash never closes, so every route in the file was dead.
# Verify any edit with `bash -n dispatch.sh` before trusting it.

# ─── GK Daily Special Editions ─────────────────────────────────────────────
# gkdaily-special.py runs the whole pipeline (research -> script -> TTS ->
# Spotify -> feed verification) and reports each stage to Telegram itself, so
# these return immediately and the phone tells the story.

if echo "$MESSAGE" | grep -qiE "^gk ?daily (status|check)|^podcast status"; then
    echo ">> GK Daily status"
    "$CLAWD_DIR/gkdaily-special.py" --status
    exit 0
fi

# "make the next gk daily special" -> top uncovered topic from the topics doc
if echo "$MESSAGE" | grep -qiE "^(make|produce|run|do) (the )?next gk ?daily( special)?|^next gk ?daily|^gk ?daily next"; then
    echo ">> GK Daily special — next topic from the queue"
    "$CLAWD_DIR/gkdaily-special.py" --detach --next \
        >> "$HOME/Library/Logs/gkdaily-special.log" 2>&1
    echo "   Started. Telegram will report each stage (~12-15 min to live)."
    exit 0
fi

# "gk daily special on <topic>" / "special episode about <topic>"
if echo "$MESSAGE" | grep -qiE "gk ?daily special|special edition|special episode"; then
    TOPIC=$(echo "$MESSAGE" | sed -E 's/.*(gk ?daily special|special edition|special episode)//I; s/^ *(on|about|regarding|covering|re) +//I; s/^[:,-] *//; s/ *[.!?]* *$//')
    if [ -z "$TOPIC" ]; then
        echo ">> No topic given — using the next one from the queue"
        "$CLAWD_DIR/gkdaily-special.py" --detach --next \
            >> "$HOME/Library/Logs/gkdaily-special.log" 2>&1
    else
        echo ">> GK Daily special on: $TOPIC"
        "$CLAWD_DIR/gkdaily-special.py" --detach --topic "$TOPIC" \
            >> "$HOME/Library/Logs/gkdaily-special.log" 2>&1
    fi
    echo "   Started. Telegram will report each stage (~12-15 min to live)."
    exit 0
fi

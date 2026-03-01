#!/usr/bin/env python3
import argparse
import datetime
import os
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TRACKS_MD = PROJECT_ROOT / "conductor" / "tracks.md"

def update_tracks_date(track_name: str):
    if not TRACKS_MD.exists():
        print(f"Warning: {TRACKS_MD} not found.")
        return

    with open(TRACKS_MD, "r") as f:
        content = f.read()

    today = datetime.date.today().isoformat()
    
    # Simple regex to find the track and update its date
    # Format: - TRACK_NAME: status (DATE)
    pattern = rf"(- {re.escape(track_name)}: \w+)\s*(\(.*\))?"
    replacement = rf"\1 ({today})"
    
    new_content, count = re.subn(pattern, replacement, content)
    
    if count > 0:
        with open(TRACKS_MD, "w") as f:
            f.write(new_content)
        print(f"Updated {track_name} date to {today} in tracks.md")
    else:
        # If track not found, append a new one
        new_line = f"- {track_name}: in_progress ({today})\n"
        with open(TRACKS_MD, "a") as f:
            f.write(new_line)
        print(f"Added new track '{track_name}' to tracks.md")

def main():
    parser = argparse.ArgumentParser(description="Update Conductor tracks after a run")
    parser.add_argument("--track", type=str, required=True, help="Name of the track to update")
    args = parser.parse_args()
    
    update_tracks_date(args.track)

if __name__ == "__main__":
    main()

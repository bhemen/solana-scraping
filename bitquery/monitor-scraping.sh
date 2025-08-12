#!/bin/bash
#The scraping script crashes regularly.
#This script checks if it's running, and if it's not, it restarts it

DIR="/home/brett/sandbox/solana-scraping/bitquery/"

SCRIPT_NAME="pump-creation-subscription.py"

COMMAND=".env/bin/python3 $SCRIPT_NAME"

# Function to check if the script is running
is_running() {
    pgrep -f "$SCRIPT_NAME" > /dev/null
    return $?
}

if is_running; then
	echo "$(date): $SCRIPT_NAME is already running."
else
	echo "$(date): $SCRIPT_NAME is not running. Restarting..."
	pushd "$DIR"
	$COMMAND &
fi


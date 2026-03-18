#!/usr/bin/env bash


# Define the INI file
CONFIG_FILE="${DATAMILL_WORK}/config.ini"


# Function to extract a single value correctly (ignores comments)
get_config_value() {
    awk -F= -v key="$1" '$1 ~ key && $0 !~ /^#/ {print $2}' "$CONFIG_FILE" | sed 's/ *#.*//g' | tr -d '"'
}

# Function to extract lists (arrays), handling spaces, parentheses, and comments
get_list_value() {
    grep -E "^$1=" "$CONFIG_FILE" | sed -E 's/^[^=]+=//; s/[()"]//g; s/ *#.*//g' | tr -s ' '
}

# Function to extract dictionary (JSON-like) values correctly (removes comments and trims whitespace/quotes)
get_dict_value() {
    local key="$1"
    grep -E "^$key=" "$CONFIG_FILE" | sed -E '
        s/^[^=]+=//;  # Remove key and equals sign
        s/ *#.*//g;   # Remove comments
        s/^[ \t"'\'']+|[ \t"'\'']+$//g;  # Trim whitespace and quotes
    '
}

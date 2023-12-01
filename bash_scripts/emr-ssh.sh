#!/bin/zsh

# Set key_file and emr_master_node in environ variables

# user name
user_name="hadoop"

ssh -i "$key_file" "$user_name@$emr_master_node"

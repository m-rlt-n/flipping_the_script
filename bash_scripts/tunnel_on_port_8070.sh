#!/bin/zsh

# Set key_file and emr_master_node in environ variables

# user name
local_port=8070
user_name="hadoop"
remote_port=8070

ssh -i "$key_file" -L "$local_port":"$emr_master_node":"$remote_port" "$user_name@$emr_master_node"

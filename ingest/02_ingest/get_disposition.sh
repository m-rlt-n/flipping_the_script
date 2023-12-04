#!/bin/bash

url="https://datacatalog.cookcountyil.gov/resource/apwk-dzx8.json"
apptoken=$COOK_CNTY_APP_TOKEN # create app token here: https://dev.socrata.com/foundry/datacatalog.cookcountyil.gov/apwk-dzx8

# Set headers to accept JSON and include the app token
headers=(-H "Accept: application/json" -H "X-App-Token: $apptoken")

# Commented out code for EDA
# # set the $limit parameter to 2 to fetch only the first two rows
# set_limit=5
# url="$url?\$limit=$set_limit"
# curl -X GET "$url" "${headers[@]}"

# # Check number of records
# curl -X GET "$url" "${headers[@]}" | jq '. | length'

# Set the number of records to fetch per request
batch_size=100000

# Initialize variables
offset=0
file_counter=1

# Loop through paginated requests to fetch all records
while true; do
    # API call in cURL and format with jq
    response=$(curl -s -X GET "$url?\$limit=$batch_size&\$offset=$offset" "${headers[@]}")
    current_batch_size=$(echo "$response" | jq length)

    # Break if no more records are returned
    if [ "$current_batch_size" -eq 0 ]; then
        break
    fi

    # Increment the offset
    offset=$((offset + batch_size))

    # Save the JSON data to a for every 100k records
    echo "$response" > "data/disposition_$file_counter.json"
    file_counter=$((file_counter + 1))

    echo "$offset"
done

# Save any remaining JSON data to a numbered file
if [ -n "$response" ]; then
    echo "$response" > "data/disposition_$file_counter.json"
fi

# Move all JSON files to HDFS
hadoop fs -copyFromLocal -f data/disposition_*.json mnicolas/
#!/bin/bash

url="https://datacatalog.cookcountyil.gov/resource/tg8v-tm6u.json"
apptoken=$COOK_CNTY_APP_TOKEN # create app token here: https://dev.socrata.com/foundry/datacatalog.cookcountyil.gov/tg8v-tm6u

# Set headers to accept JSON and include the app token
headers=(-H "Accept: application/json" -H "X-App-Token: $apptoken")

# Set the number of records to fetch per request
batch_size=10000

# Initialize variables
offset=0
json_data=""
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

    # Append the current batch of records to json_data
    json_data="${json_data}${response}"

    # Increment the offset
    offset=$((offset + batch_size))

    # Save the JSON data to a for every 200k records
    if [ "$((offset % (20 * batch_size)))" -eq 0 ]; then
        echo "$json_data" > "data/sentencing_$file_counter.json"
        file_counter=$((file_counter + 1))
        json_data=""
    fi

    echo "$offset"
done

# Save any remaining JSON data to a numbered file
if [ -n "$json_data" ]; then
    echo "$json_data" > "data/sentencing_$file_counter.json"
fi

# Move all JSON files to HDFS
hadoop fs -copyFromLocal -f data/sentencing_*.json mnicolas/
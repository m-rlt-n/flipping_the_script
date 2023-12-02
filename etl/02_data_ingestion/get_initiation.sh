#!/bin/bash

url="https://datacatalog.cookcountyil.gov/resource/7mck-ehwz.json"
apptoken=$COOK_CNTY_APP_TOKEN # create app token here: https://dev.socrata.com/foundry/datacatalog.cookcountyil.gov/7mck-ehwz

# Set headers to accept JSON and include the app token
headers=(-H "Accept: application/json" -H "X-App-Token: $apptoken")

# Set the number of records to fetch per request
batch_size=10000

# Initialize variables
offset=0
json_data=""

# Loop through paginated requests until all records are fetched
while true; do
    # Make the API call using cURL and format with jq
    response=$(curl -s -X GET "$url?\$limit=$batch_size&\$offset=$offset" "${headers[@]}")
    current_batch_size=$(echo "$response" | jq length)

    # Break the loop if no more records are returned
    if [ "$current_batch_size" -eq 0 ]; then
        break
    fi

    # Append the current batch of records to the result
    json_data="${json_data}${response}"

    # Increment the offset for the next batch
    offset=$((offset + batch_size))
    echo "$offset"
done

# Save JSON data to a file
echo "$json_data" > data/initiation.json

# Write to data lake
hadoop fs -copyFromLocal -f data/initiation.json mnicolas/initiation.json
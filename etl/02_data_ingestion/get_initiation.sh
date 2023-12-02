#!/bin/bash

url="https://datacatalog.cookcountyil.gov/resource/7mck-ehwz.json"
apptoken=$COOK_CNTY_APP_TOKEN # create app token here: https://dev.socrata.com/foundry/datacatalog.cookcountyil.gov/tg8v-tm6u

# Set headers to accept JSON and include the app token
headers=(-H "Accept: application/json" -H "X-App-Token: $apptoken")

# Make the API call using cURL and format with jq
json_data=$(curl -s -X GET "$url" "${headers[@]}" | jq -r '.[] | flatten | @csv')

# Create a CSV file and write the header and data
echo "case_id,case_participant_id,received_date,offense_category,primary_charge,charge_id,charge_version_id,charge_offense_title,charge_count,chapter,act,section,class,aoic,event,event_date,arraignment_date,bond_date_initial,bond_date_current,bond_type_initial,bond_type_current,age_at_incident,race,gender,incident_begin_date,law_enforcement_agency,arrest_date,felony_review_date,felony_review_result,updated_offense_category" > data/initiation.csv
echo "$json_data" >> data/initiation.csv

# Write to data lake
hadoop fs -copyFromLocal -f data/initiation.csv mnicolas/initiation.csv
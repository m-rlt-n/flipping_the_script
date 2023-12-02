#!/bin/bash

url="https://datacatalog.cookcountyil.gov/resource/tg8v-tm6u.json"
apptoken=$COOK_CNTY_APP_TOKEN # create app token here: https://dev.socrata.com/foundry/datacatalog.cookcountyil.gov/tg8v-tm6u

# Set headers to accept JSON and include the app token
headers=(-H "Accept: application/json" -H "X-App-Token: $apptoken")

# Make the API call using cURL and format with jq
json_data=$(curl -s -X GET "$url" "${headers[@]}" | jq -r '.[] | flatten | @csv')

# Create a CSV file and write the header and data
echo "case_id,case_participant_id,received_date,offense_category,primary_charge,charge_id,charge_version_id,disposition_charged_offense_title,charge_count,disposition_date,disposition_charged_chapter,disposition_charged_class,disposition_charged_aoic,charge_disposition,sentence_judge,court_name,court_facility,sentence_phase,sentence_date,sentence_type,current_sentence,commitment_type,length_of_case_in_days,age_at_incident,race,gender,incident_begin_date,law_enforcement_agency,arrest_date,felony_review_date,felony_review_result,arraignment_date,updated_offense_category" > data/sentencing.csv
echo "$json_data" >> data/sentencing.csv

# Write to data lake
hadoop fs -copyFromLocal -f data/sentencing.csv mnicolas/sentencing.csv
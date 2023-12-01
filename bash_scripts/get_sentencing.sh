#!/bin/bash

url="https://datacatalog.cookcountyil.gov/resource/tg8v-tm6u.json"
apptoken=$COOK_CNTY_API_TOKEN

# Set headers to accept JSON and include the app token
headers=(-H "Accept: application/json" -H "X-App-Token: $apptoken")

# Commented out code for EDA 
# set the $limit parameter to 2 to fetch only the first two rows
# # set_limit=5
# # url="$url?\$limit=$set_limit"

# Check number of records
# # curl -X GET "$url" "${headers[@]}" | jq '. | length'

# Make the API call using cURL and format with jq
json_data=$(curl -s -X GET "$url" "${headers[@]}" | jq -r '.[] | [.case_id, .case_participant_id, .received_date, .offense_category, .primary_charge, .charge_id, .charge_version_id, .disposition_charged_offense_title, .charge_count, .disposition_date, .disposition_charged_chapter, .disposition_charged_class, .disposition_charged_aoic, .charge_disposition, .sentence_judge, .court_name, .court_facility, .sentence_phase, .sentence_date, .sentence_type, .current_sentence, .commitment_type, .length_of_case_in_days, .age_at_incident, .race, .gender, .incident_begin_date, .law_enforcement_agency, .arrest_date, .felony_review_date, .felony_review_result, .arraignment_date, .updated_offense_category] | @csv')

# Create a CSV file and write the header and data
echo "case_id,case_participant_id,received_date,offense_category,primary_charge,charge_id,charge_version_id,disposition_charged_offense_title,charge_count,disposition_date,disposition_charged_chapter,disposition_charged_class,disposition_charged_aoic,charge_disposition,sentence_judge,court_name,court_facility,sentence_phase,sentence_date,sentence_type,current_sentence,commitment_type,length_of_case_in_days,age_at_incident,race,gender,incident_begin_date,law_enforcement_agency,arrest_date,felony_review_date,felony_review_result,arraignment_date,updated_offense_category" > mnicolas_data/sentencing.csv
echo "$json_data" >> mnicolas_data/sentencing.csv
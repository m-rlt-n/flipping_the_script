package org.example;

import org.json.JSONObject;

public class DispositionDataConverter {

    public static DispositionData convertResponseToDispositionData(JSONObject response) {
        try {
            DispositionData dispositionData = new DispositionData();

            // Get values by key from the JSONObject
            dispositionData.setCaseId(response.optString("case_id", ""));
            dispositionData.setChargeCount(response.optString("charge_count", ""));
            dispositionData.setAgeAtIncident(response.optString("age_at_incident", ""));
            dispositionData.setBondAmountCurrent(response.optString("bond_amount_current", ""));
            dispositionData.setJudge(response.optString("judge", ""));
            dispositionData.setUnit(response.optString("unit", ""));
            dispositionData.setGender(response.optString("gender", ""));
            dispositionData.setRace(response.optString("race", ""));
            dispositionData.setCourtName(response.optString("count_name", ""));
            dispositionData.setOffenseCategory(response.optString("offense_category", ""));
            dispositionData.setDispositionChargedOffenseTitle(response.optString("disposition_charged_offense_title", ""));

            return dispositionData;
        } catch (Exception e) {
            e.printStackTrace();
            // Handle the exception according to your application's requirements
            return null;
        }
    }
}

package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DispositionData {

    public String toJsonString() {
        // Build JSON string manually
        return String.format(
                "{\"case_id\":\"%s\",\"charge_count\":\"%s\",\"age_at_incident\":\"%s\",\"bond_amount_current\":\"%s\"," +
                        "\"judge\":\"%s\",\"unit\":\"%s\",\"gender\":\"%s\",\"race\":\"%s\",\"court_name\":\"%s\"," +
                        "\"offense_category\":\"%s\",\"disposition_charged_offense_title\":\"%s\"}",
                caseId, chargeCount, ageAtIncident, bondAmountCurrent, judge, unit, gender, race,
                courtName, offenseCategory, dispositionChargedOffenseTitle);
    }

    @JsonProperty("case_id")
    String caseId;

    @JsonProperty("charge_count")
    String chargeCount;

    @JsonProperty("age_at_incident")
    String ageAtIncident;

    @JsonProperty("bond_amount_current")
    String bondAmountCurrent;

    @JsonProperty("judge")
    String judge;

    @JsonProperty("unit")
    String unit;

    @JsonProperty("gender")
    String gender;

    @JsonProperty("race")
    String race;

    @JsonProperty("court_name")
    String courtName;

    @JsonProperty("offense_category")
    String offenseCategory;

    @JsonProperty("disposition_charged_offense_title")
    String dispositionChargedOffenseTitle;

    // Setter methods
    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }
    public void setChargeCount(String chargeCount) {
        this.chargeCount = chargeCount;
    }

    public void setAgeAtIncident(String ageAtIncident) {
        this.ageAtIncident = ageAtIncident;
    }

    public void setBondAmountCurrent(String bondAmountCurrent) {
        this.bondAmountCurrent = bondAmountCurrent;
    }

    public void setJudge(String judge) {
        this.judge = judge;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setRace(String race) {
        this.race = race;
    }

    public void setCourtName(String courtName) {
        this.courtName = courtName;
    }

    public void setOffenseCategory(String offenseCategory) {
        this.offenseCategory = offenseCategory;
    }

    public void setDispositionChargedOffenseTitle(String dispositionChargedOffenseTitle) {
        this.dispositionChargedOffenseTitle = dispositionChargedOffenseTitle;
    }

    // Getter methods
    public String getCaseId() {
        return this.caseId;
    }

    public String getChargeCount() {
        return chargeCount;
    }

    public String getAgeAtIncident() {
        return ageAtIncident;
    }

    public String getBondAmountCurrent() {
        return bondAmountCurrent;
    }

    public String getJudge() {
        return judge;
    }

    public String getUnit() {
        return unit;
    }

    public String getGender() {
        return gender;
    }

    public String getRace() {
        return race;
    }

    public String getCourtName() {
        return courtName;
    }

    public String getOffenseCategory() {
        return offenseCategory;
    }

    public String getDispositionChargedOffenseTitle() {
        return dispositionChargedOffenseTitle;
    }
}

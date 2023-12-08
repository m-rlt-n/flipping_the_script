'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]), encoding: 'latin1'})

function counterToString(c) {
	return String(Buffer.from(c, 'latin1'));
}
function rowToMap(row) {
    var stats = {};
    // Check if row is not null or undefined before iterating
    if (row) {
        row.forEach(function (item) {
            stats[item['column']] = counterToString(item['$']);
        });
    }
    return stats;
}

hclient.table('nmarlton_cook_county_data_v0').row('example_id').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})

app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
    const case_id=String(req.query['case_id']); 
    console.log(case_id);
	hclient.table('nmarlton_cook_county_data_v0').row(case_id).get(function (err, cells) {

        const caseInfo = rowToMap(cells);
        console.log(caseInfo);

		function col_val(weather) {
            const data = caseInfo[`family:${weather}`];
            return data !== undefined ? data : "N/A";
		}

		if (!err) {
			var print_string = `Predicted risk score for Case ID: ${req.query['case_id']} is ${col_val("predicted_risk_percentile")}`
		} else {
			var print_string = `No records available for Case ID: ${req.query['case_id']}`
		}

		var risk_interpretation = `No interpretation is available for risk score "N/A"`
		if (!err) {
			var pred_risk = Number(col_val("predicted_risk_percentile"))
			var risk_interpretation = `The predicted risk percentile (${pred_risk}) suggests this defendant is at lower risk of excessive sentencing, given legally <br> &nbsp; irrelevant factors, than ${99 - pred_risk}% of people.`
			if (pred_risk > 60.0) {
				var risk_interpretation = `The predicted risk percentile (${pred_risk}) suggest this defendant is at higher risk of excessive sentencing, given legally <br> &nbsp; irrelevant factors, than ${pred_risk - 1}% of people.`
			} if (pred_risk > 90.0) {
				var risk_interpretation = `The predicted risk percentile (${pred_risk}) suggest this defendant is at very high risk of excessive sentencing, given factors that should be legally irrelevant.`
			}
		}

		var risk_string = `
		<b>What does this score mean for the defendant?</b>
			<br>
			&nbsp;&nbsp;${risk_interpretation}
			<br>
			<br>
			&nbsp;&nbsp;<i>For legal council, the defendant can contact:</i>
			<br>
			&nbsp;&nbsp;- <a href="https://www.cookcountypublicdefender.org/" target="_blank">The Law Office of the Cook County Public Defender</a>
			<br>
			&nbsp;&nbsp;- <a href="https://www.law.uchicago.edu/clinics/mandel/juvenile" target="_blank">The University of Chicago Criminal and Juvenile Justice Clinic</a>
			<br>
			<br>
			&nbsp;&nbsp;<i>For mental health resurces:</i>
			<br>
			&nbsp;&nbsp;- <a href="https://www.chicago.gov/city/en/depts/cdph/provdrs/behavioral_health/svcs/2012_mental_healthservices.html" target="_blank">Chicago Department of Public Health, Mental Health Services</a>
		`;

		var methodology_string = `
		<b>What is a predicted risk percentile?</b>
			<br>
			&nbsp;&nbsp;A measure that indicates the relative risk of an individual experiencing a particular outcome. It is <b>NOT</b> a probability. <br>
			&nbsp;&nbsp;So, a score of 50.0 means an individual's risk score is lower than 49% of all risk scores, i.e. in the 50th percentile. <br>
			&nbsp;&nbsp;It does not mean a 50/50 chance of the outcome. <br>
			<br>
			&nbsp;&nbsp;<b>The model used in this application is based on <a href="https://dl.acm.org/doi/abs/10.1145/3531146.3533104" target="_blank"> Flipping the Script on Criminal Justice Risk Assessment.</a></b> (Meyer, et al.)
			<br>
			&nbsp;&nbsp;Drawing on data from sentencing decisions in Cook County, we emulate their work. Their work produced a "risk assessment instrument <br>
			&nbsp;&nbsp;that predicts the likelihood an individual will receive an especially lengthy sentence given factors that should be legally irrelevant." We<br>
			&nbsp;&nbsp;apply their a "two-stage modeling approach." The first model labels sentences as "especially lengthy." The second model predicts an <br>
			&nbsp;&nbsp;individual’s risk of receiving such a sentence
		`;

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			case_id : req.query['case_id'],
			offense_category : col_val("offense_category"),
			offense_title : col_val("offense_title"),
			received_date : col_val("received_date"),
			judge : col_val("judge"),
			predicted_risk_percentile : col_val("predicted_risk_percentile"),
			output_string : print_string,
			risk_interp : risk_string,
			methods_string : methodology_string
		});
		res.send(html);
	});
});


app.listen(port);

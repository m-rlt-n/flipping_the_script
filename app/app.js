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

hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
    const case_id=String(req.query['case_id']); 
    console.log(case_id);
	hclient.table('nmarlton_cook_county_data_v0').row(case_id).get(function (err, cells) {

        const weatherInfo = rowToMap(cells);
        console.log(weatherInfo);

		function col_val(weather) {
            const data = weatherInfo[`family:${weather}`];
            return data !== undefined ? data : "N/A";
		}

		if (!err) {
			var print_string = `Predicted risk score for Case ID: ${req.query['case_id']} is ${col_val("predicted_risk_percentile")}`
		} else {
			var print_string = `No records available for Case ID: ${req.query['case_id']}`
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			case_id : req.query['case_id'],
			offense_category : col_val("offense_category"),
			offense_title : col_val("offense_title"),
			received_date : col_val("received_date"),
			judge : col_val("judge"),
			predicted_risk_percentile : col_val("predicted_risk_percentile"),
			output_string : print_string
		});
		res.send(html);
	});
});


app.listen(port);

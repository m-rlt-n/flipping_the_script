# flipping_the_script
## About the project:
This repo contains a project exploring big data application architecture, in particular the Lambda Architecture. For me, the purpose of the project was to learn the fundamentals of the Lambda Architecture through application. For the content of the application, I chose to build on work produced in the paper 'Flipping the Script on Criminal Justice Risk Assessment' (Meyer, et al.). My goal was to explore how their model might be put into production and used to support better outcomes for criminal justice involved individuals. 

That said, I am using a different data set, and a materially less sophisticated modeling approach, given that the model was not the focus of this project. Interestingly, the authors produced an HBART model, which from my reading would be non-trivial to re-implement in sparkML. To anyone considering extending this work, improving the quality of the SparkML models (`prediction\model*.scala`) is the place I would start. 

## About the Application:
In the words of Meyer, et al.:

- "In the criminal justice system, algorithmic risk assessment instruments are used to predict the risk a defendant poses to society; examples include the risk of recidivating or the risk of failing to appear at future court dates. However, defendants are also at risk of harm from the criminal justice system. To date, there exists no risk assessment instrument that considers the risk the system poses to the individual. We develop a risk assessment instrument that “flips the script.” Using data about U.S. federal sentencing decisions, we build a risk assessment instrument that predicts the likelihood an individual will receive an especially lengthy sentence given factors that should be legally irrelevant to the sentencing decision. To do this, we develop a two-stage modeling approach. Our first-stage model is used to determine which sentences were “especially lengthy.” We then use a second-stage model to predict the defendant’s risk of receiving a sentence that is flagged as especially lengthy given factors that should be legally irrelevant. The factors that should be legally irrelevant include, for example, race, court location, and other socio-demographic information about the defendant. Our instrument achieves comparable predictive accuracy to risk assessment instruments used in pretrial and parole contexts."

This application, explores how these predictive risk scores might be served to legal professionals representing a defendant in a criminal trial. The application is intentionally not exploring how these scores might be communicated to the defendant. In the same way that predictive risk models are used to inform institutional decision-making, the scores produced by this model would be available to support the work of public defenders, etc. Specifically, because the scores estimate the risk of "especially lengthy sentence given factors that should be legally irrelevant," it could be a useful tool to help cast light on cases that might be unusually predisposed to judicial bias. 

One material difference between the output of the model in the paper, and the model in this project, is that this project does not present the output as a probability. That is, the paper serves the output of a logistic regression model as the final result for a given record. This output can be interpreted as the likelihood that an individual receives an "especially lengthy sentence given factors that should be legally irrelevant." This project scales these probabilities into percentiles and serves the percentile to the user, in order to make the result more 'interpretable.' That is, instead of the score representing the probability that an individual is in the 90th percentile for sentence duration; the score represents the relative risk facing a given defendant. In other words, we expect this defendant to be riskier than *n*% of defendants and less risky than (100 - (*n* + 1))% of defendants, where *n* is the risk percentile for a given defendant. In this sense, it is a UX decision. One could debate this choice of methodology, and that would probably be an interesting discussion. 

Meyer, et al. articulate the purpose and context for this tool in their paper which is linked !['here'](https://dl.acm.org/doi/abs/10.1145/3531146.3533104) and below. 


## Example Run:

![application example](/application_example.png "Application Architecture")

access app through browser: http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3085/

## Application Run Instructions:
These instructions assume you have access to the EMR cluster where the application is deployed.

### To interact with the web application:
1. create a tunnel between local port 8070 and remote port 3085 (where the app is hosted) on the emr_master_node used for application hosting.
    `bash ssh -i "$key_file" -L "$local_port":"$emr_master_node":"$remote_port" "$user_name@$emr_master_node"`
2. cd into `nmarlton/app`
3. run `node app.js 3085 ec2-3-131-137-149.us-east-2.compute.amazonaws.com 8070`
4. access the application via a web browser
    `$emr_master_node\3085`

### To turn on the speed layer:
1. ssh tunnel into the emr master node used for data engineering:
    `bash ssh -i "$key_file" "$user_name@$emr_master_node"`
2. turn on the kafka consumer by cding into `nmarlton/consumer/target/` and running
    `spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamCases uber-kafka-case-consumer-1.0-SNAPSHOT.jar b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092`
3. turn on the kafka producer by cding into `nmarlton/producer/target/` and running
    `java -cp uber-kafka-stream-cases-1.0-SNAPSHOT.jar org.example.CaseDataToKafka county_cases "b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092"`
4. you can alway check the outputs by running
    `kafka-console-consumer.sh --bootstrap-server b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092 --topic county_cases --from-beginning`

    After step 4, you will see case_ids (along with other details) streaming by in the terminal. You can see that the speed layer is updating HBase in real time by copying any given case_id from the terminal and inputting it in the web app.

see `ingest\01_emr_connection\` for examples of shell scripts

## Application Architecture:

![application architecture](/application_architecture.png "Application Architecture")

- ### Remote Data Sources
    (Batch Layer) Data from the Cook County State's Attorney Office is pulled via API call from Cook County Government Open Data.
    (Speed Layer) Data from later than 01/01/2023 is queried in real time through the API and used to simulate streaming. 
- ### Data Lake (HDFS)
    Following the sushi principle, these are .json files saved to HDFS, and read into Spark scripts and loaded to Hive. 
    The code for pulling remote data into the data lake is checked into the `ingest` sub-directory. This is everything that needs to happen to get the data into the batch layer. Basically all shell scripts.
- ### Batch Layer (Hive)
    The Batch layer is a set of Hive batch views that are used for ML and feeding the serving layer.

    The code is checked into the `batch_layer` sub-directory. It contains, scripts for data processing to output (a) data for SparkML, (b) the batch view that will be read into the serving layer. The scripts join Dispositions and Sentencing data, save it in Hive and run simple ML models using Spark Jobs
- ### Serving Layer (HBase)
    The serving layer is a single HBase table that is read directly into the application. This layer is very performant because all of the calculations happen on the back end. 
    How I would extend this: There is a lot of information in the Cook County court records. Right now, the application serves a small fraction of it to the user. If I were thinking about extending the UI, this is where I would start. What other information might legal professionals use/need if they were interacting with this app. 
    The code is checked into the `serving_layer` sub-directory. These are the scripts to produce and load the HBase table that the web app reads. 
- ### Speed Layer (Kafka)
    The speed layer consists of the Kafka 'producer' and 'consumer' applications that stream data into the `serving_layer`. 
    How I would extend this: 
        (1) The speed layer is a bit of a hack right now. That is, it taps an API and then streams the data using Kafka. I would like to think through actual real-time data feeds that could inform the model. But for this purpose, I just 'pretend' the data requires streaming. 
        (2) The SparkML models are actually not implemented in the speed layer right now. The speed layer assigns a flat 50.0 `perdicted_risk_percentile` to all streamed records and then writes to HBase. It's an intuitive (and probably fun) place to start, and I didn't prioritize it with the time I had. 
    The code is checked into the `speed_layer` sub-directory.
- ### Spark ML
    I copy the two-part modeling approach used by Meyer, et al. mechanically, but with exceedingly low fidelity. Again, ML was not the focus here. 
    How I would extend this: If I find time to jump back into this repo, this will be the first place I go. It seems like HBART will be non-trivial to implement in SparkML. So, should be fun!
    The code is checked into the `prediction` sub-directory. There are two SparkML models (one for each step) which are called by both the batch layer and the speed layer
- ### Web App
    The web app (described above) is implemented in JavaScript with a few other bolt-ons to handle hmtl rendering. I was basically editing html templates to pull this together. 
    The code is checked into the `app` sub-directory.

## Citations:

This project was inspired by the paper ['Flipping the Script on Criminal Justice Risk Assessment' (Meyer, et al.)](https://dl.acm.org/doi/abs/10.1145/3531146.3533104)

Data was queried from [Cook County Government Open Data](https://datacatalog.cookcountyil.gov/). This project employs the [Dispositions](https://datacatalog.cookcountyil.gov/Courts/Dispositions/apwk-dzx8) and [Sentencing](https://datacatalog.cookcountyil.gov/Courts/Sentencing/tg8v-tm6u) data sets from the Cook County State's Attorney Office. Last updated as of 09/06/2023.

The impetus for this project, and a large amout of the boilerplate code that runs that applications came from [Mike Spertus'](https://cs.uchicago.edu/people/michael-spertus/) [Big Data Application Architecture](https://mpcs-courses.cs.uchicago.edu/2023-24/autumn/courses/mpcs-53014-1) class, which I would highly recommend to anyone with an interest in this subject and access to the resources of the University of Chicago. 

I was able to take a shortcut on some of the EDA and join criteria thanks to the impressive work of the authors of this [Cook County Mental Health Prediciton](https://github.com/kelseymarkey/cook-county-mental-health-prediction/tree/master) repo. [Amber Teng](https://medium.com/@angelamarieteng) covers their methodology in [Analyzing Chicago Court Data with Python](https://towardsdatascience.com/analyzing-chicago-court-data-with-python-8a4bae330dfd).

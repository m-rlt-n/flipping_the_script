# flipping_the_script
## About the project:
This repo contains a project exploring big data application architecture, in particular the Lambda Architecture. For me, the purpose of the project was to learn the fundamentals of the Lambda Architecture through application. For the content of the application, I chose to build on work produced in the paper 'Flipping the Script on Criminal Justice Risk Assessment' (Meyer, et al.). My goal was to explore how their model might be put into production and used to support better outcomes for criminal justice involved individuals. 

That said, I am using a different data set, and a materially less sophisticated modeling approach, given that the model was not the focus of this project. Interestingly, the authors produced an HBART model, which from my reading would be non-trivial to re-implement in sparkML. To anyone considering extending this work, improving the quality of the SparkML models (`prediction\model*.scala`) is the place I would start. 

## About the Application:
In the words of Meyer, et al.:

    "In the criminal justice system, algorithmic risk assessment instruments are used to predict the risk a defendant poses to society; examples include the risk of recidivating or the risk of failing to appear at future court dates. However, defendants are also at risk of harm from the criminal justice system. To date, there exists no risk assessment instrument that considers the risk the system poses to the individual. We develop a risk assessment instrument that “flips the script.” Using data about U.S. federal sentencing decisions, we build a risk assessment instrument that predicts the likelihood an individual will receive an especially lengthy sentence given factors that should be legally irrelevant to the sentencing decision. To do this, we develop a two-stage modeling approach. Our first-stage model is used to determine which sentences were “especially lengthy.” We then use a second-stage model to predict the defendant’s risk of receiving a sentence that is flagged as especially lengthy given factors that should be legally irrelevant. The factors that should be legally irrelevant include, for example, race, court location, and other socio-demographic information about the defendant. Our instrument achieves comparable predictive accuracy to risk assessment instruments used in pretrial and parole contexts."

This application, explores how these predictive risk scores might be served to legal professionals representing a defendant in a criminal trial. The application is intentionally not exploring how these scores might be communicated to the defendant. In the same way that predictive risk models are used to inform institutional decision-making, the scores produced by this model would be available to support the work of pubic defenders, etc. Specifically, because the scores estimate the risk of "especially lengthy sentence given factors that should be legally irrelevant," it could be a useful tool to help cast light on cases that might be unusually predisposed to judicial bias. 

Meyer, et al. articulate the purpose and context for this tool in that paper which is linked !['here'](https://dl.acm.org/doi/abs/10.1145/3531146.3533104) and below. 


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
2. cd into nmarlton/app
3. turn on the kafka consumer by running
    `spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamCases uber-kafka-case-consumer-1.0-SNAPSHOT.jar b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092`
4. turn on the kafka producer by running
    `java -cp uber-kafka-stream-cases-1.0-SNAPSHOT.jar org.example.CaseDataToKafka county_cases "b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092"`
5. you can alway check the outputs by running
    `kafka-console-consumer.sh --bootstrap-server b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092 --topic county_cases --from-beginning`

    After step 4, you will see case_ids (along with other details) streaming by in the terminal. You can see that the speed layer is updating HBase in real time by copying any given case_id from the terminal and inputting it in the web app.

see `ingest\01_emr_connection\` for examples of shell scripts

## Application Architecture:

![application architecture](/application_architecture.png "Application Architecture")

- ### Remote Data Sources
    (Batch Layer) Data from the Cook County State's Attorney Office is pulled via .CSV Download from Cook County Government Open Data.
    (Speed Layer) Data from 01/01/2023 and later is stored separately (IN WHAT?) and streamed into the application in real time
- ### Data Lake
    .CSV data is ingested into the Hadoop HDFS data lake using shell scripts in `etl/02_data_ingestion`
    I wrote this data to CSV because it simplified EDA (which I needed to do in prep for later steps). That said, the data comes out of the Cook County API in JSON and there is no performance reason to mutate it into CSV before pulling it into the batch layer. 
- ### Batch Layer
    Dispositions and Sentencing data are joined and saved in Hive using Spark Jobs
- ###   

## In this repo:

This repository has `n` directories [`app`, `etl`, `...`]

- `app`:
- `etl`:
- `prediction`:

## Citations:

This project was inspired by the paper !['Flipping the Script on Criminal Justice Risk Assessment' (Meyers, et al.)](https://dl.acm.org/doi/abs/10.1145/3531146.3533104)



Data was queried from ![Cook County Government Open Data](https://datacatalog.cookcountyil.gov/). This project employs the ![Dispositions](https://datacatalog.cookcountyil.gov/Courts/Dispositions/apwk-dzx8) and ![Sentencing](https://datacatalog.cookcountyil.gov/Courts/Sentencing/tg8v-tm6u) data sets from the Cook County State's Attorney Office. Last updated as of 09/06/2023.

I was able to take a shortcut on some of the EDA and join criteria thanks to the impressive work of the authors of this ![Cook County Mental Health Prediciton](https://github.com/kelseymarkey/cook-county-mental-health-prediction/tree/master) repo. ![Amber Teng](https://medium.com/@angelamarieteng) covers their methodology in ![Analyzing Chicago Court Data with Python](https://towardsdatascience.com/analyzing-chicago-court-data-with-python-8a4bae330dfd).
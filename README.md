# flipping_the_script
This repo contains a project exploring big data application architecture, in particular the Lambda Architecture. The application serves 'predictive risk scores' based representing the risk of excessive sentencing faced in a given court case by a given defendant. The model implementation is based on the paper 'Flipping the Script on Criminal Justice Risk Assessment' (Meyer, et al.). The authors of this paper articulate the purpose and context for this model in that paper, and their work is linked in the citations section below. 

## Example Run:

![application example](/application_example.png "Application Architecture")

access app through browser: http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3085/

## Application Run Instructions:

To interact with the web application:
    1. create a tunnel between local port 8070 and remote port 3085 (where the app is hosted) on the emr_master_node used for application hosting.
    `bash ssh -i "$key_file" -L "$local_port":"$emr_master_node":"$remote_port" "$user_name@$emr_master_node"`
    2. cd into `nmarlton/app`
    3. run `node app.js 3085 ec2-3-131-137-149.us-east-2.compute.amazonaws.com 8070`
    4. access the application via a web browser
    `$emr_master_node\3085`

To turn on the speed layer:
1. ssh tunnel into the emr master node used for data engineering:
    `bash ssh -i "$key_file" "$user_name@$emr_master_node"`
2. cd into nmarlton/
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
    (Speed Layer) Data from 01/01/2023 and later is stored separeately (IN WHAT?) and streamed into the application in real time
- ### Data Lake
    .CSV data is ingested into the Hadoop HDFS data lake using shell scripts in `etl/02_data_ingestion`
    I wrote this data to CSV because it simplified EDA (which I needed to do in prep for later steps). That said, the data comes out of the Cook County API in JSon and there is no performace reason to mutate it into CSV before pulling it into the batch layer. 
- ### Batch Layer
    Dispositions and Sentencing data are joined and saved in Hive using Spark Jobs
- ###   

## In this repo:

This reposistory has `n` directories [`app`, `etl`, `...`]

- `app`:
- `etl`:
- `prediction`:

## Citations:

This project was inspired by the paper !['Flipping the Script on Criminal Justice Risk Assessment' (Meyers, et al.)](https://dl.acm.org/doi/abs/10.1145/3531146.3533104)



Data was queried from ![Cook County Government Open Data](https://datacatalog.cookcountyil.gov/). This project employs the ![Dispositions](https://datacatalog.cookcountyil.gov/Courts/Dispositions/apwk-dzx8) and ![Sentencing](https://datacatalog.cookcountyil.gov/Courts/Sentencing/tg8v-tm6u) data sets from the Cook County State's Attorney Office. Last updated as of 09/06/2023.

I was able to take a shortcut on some of the EDA and join critera thanks to the impressive work of the authors of this ![Cook County Mental Health Prediciton](https://github.com/kelseymarkey/cook-county-mental-health-prediction/tree/master) repo. ![Amber Teng](https://medium.com/@angelamarieteng) covers their methodology in ![Analyzing Chicago Court Data with Python](https://towardsdatascience.com/analyzing-chicago-court-data-with-python-8a4bae330dfd).
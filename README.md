# flipping_the_script
This repo contains a project exploring big data application architecture, in particular the Lambda Architecture. The application serves 'predictive risk scores' based representing the risk of excessive sentencing faced in a given court case by a given defendant. The model implementation is based on the paper 'Flipping the Script on Criminal Justice Risk Assessment' (Meyer, et al.). The authors of this paper articulate the purpose and context for this model in that paper, and their work is linked in the citations section below. 

## Example Run:

![application example](/application_example.png "Application Architecture")

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
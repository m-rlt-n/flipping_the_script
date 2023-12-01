# flipping_the_script
This repo contains a side-project I used to learn the fundamentals of big data application architecture. The application serves 'predictive risk scores' based representing the risk of excessive sentencing faced in a given court case by a given defendant. The model implementation is based on the paper 'Flipping the Script on Criminal Justice Risk Assessment' (Meyer, et al.). The authors of this paper do a much better job articulating the purpose of this model than I would, and their work is linked in the citations section below. 

## Application Architecture:

![application architecture](/assets/application_architecture.png "Application Architecture")

- ### test

## In this repo:

This reposistory has `n` directories [`assets`, `bash_scripts`, `...`]

- `assets`
- `bash_scripts`

## Citations:

This project was inspired by the paper !['Flipping the Script on Criminal Justice Risk Assessment' (Meyers, et al.)](https://dl.acm.org/doi/abs/10.1145/3531146.3533104)

Data was queried from ![Cook County Government Open Data](https://datacatalog.cookcountyil.gov/). This project employs the ![Dispositions](https://datacatalog.cookcountyil.gov/Courts/Dispositions/apwk-dzx8) and ![Sentencing](https://datacatalog.cookcountyil.gov/Courts/Sentencing/tg8v-tm6u) data sets from the Cook County State's Attorney Office. Last updated as of 09/06/2023.
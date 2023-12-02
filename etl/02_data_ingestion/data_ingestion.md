after ssh tunneling into the cluster, run the scripts in this directory to pull data into HDFS data lake

### Files:

-`get_initiation.sh`: 
    The Initiation results data presented here reflects all of the arrests that came through the door of the State's Attorneys Office (SAO). An initiation is how an arrest turns into a “case” in the courts. Most cases are initiated through a process known as felony review, in which SAO attorneys make a decision whether or not to prosecute. 
-`get_disposition.sh`:
    The disposition data presented in this data reflects the culmination of the fact-finding process that leads to the resolution of a case. Each row represents a charge that has been disposed of.
-`get_sentencing.sh`:
    The sentencing data presented in this report reflects the judgment imposed by the court on people that have been found guilty. Each row represents a charge that has been sentenced. 

### Data Glossary:

https://www.cookcountystatesattorney.org/resources/how-read-data
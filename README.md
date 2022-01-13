# EGEN
## Flight Delay Prediction 

### Introduction 
Flight delays have become a matter of concern for air transportation all over the world because of the associated financial loses that the aviation industry is going through. Personally, I have faced this issue where my flight was delayed due to the ongoing Covid situation which led to series of complications. So, what can be done as a passenger to avoid delayed flights? is it possible to know if your flight will be delayed before it comes up on the departure boards? or before you being inside the plane? The answer to these questions is ‘maybe’. By using Machine Learning (ML) Algorithms we can try to predict if the flight is going to be delayed. I hope through this project information on flight delay will be relayed to the customers so that they can plan or be prepared accordingly. 

Services to be used: GCP PubSub, DataProc, Bigquery, Data Studio, Jupyter Notebook

Data Source: All data files are downloaded from OST website, which stores flights on-time performance from 1987 to present.

Project Idea:
The data is in CSV format which will be stored locally.
Using Python, I will be streaming the locally stored data through Pub Sub creating a topic and then subscribe the data to Bigquery. I have performed some PySpark transformation  which 
will be running on GCP DataProc service.Transformation will include update the data, dropping unwanted columns, Standardizing the data.
Once the transformation is done then the data will be stored in BigQuery. I will be performing some visualization using Data Studio. 
Using Machine Learning Algorithm, the flight delay prediction will be performed.

# apacheSparkPractice
building logistic regression model, testing it and saving the test results in cassandra
use queries.cql to make cassandra database
input data for building and testing the model can be found at
https://www.kaggle.com/c/avazu-ctr-prediction
# All the required ependecies are present in pom.xml
run adModel to create model for prediction(for minimizing the csv file use convertCSVToParquet to change the 6gb data into 3GB)
run adModelTest to make predictions
the predicted output with 1 as click will be stored in the cassandra database
some other test programs are there to start with spark.

File structure in this submitted zip:-


1. Task 1/ directory contains the necessary submission for Task 1, namely:

(i) CommonWords.scala - Completed code file.

(ii) commonwords_output_top15.txt - Top 15 output of the result, using the data files that were provided.

(iii) commonwords_output.txt - All the common words ordered by their frequency in descending order, using the data files that were provided.

(iv) Task1.jar - JAR file that can be used to run the project and verify the result.

(v) Task1/ directory - the Intellij Project that was created for Task 1.


2. Task 2/ directory contains the necessary submission for Task 2, namely:

(i) Assignment2.scala - Completed code file.

(ii) output.txt - The output showing the required results for Task 2, namely the centroid of each cluster, along with their dominant domain, percentage and number of questions in each cluster.

(iii) Task2.jar - JAR file that can be used to run the project and verify the result.

(iv) Task2/ directory - the Intellij Project that was created for Task 2.


3. report.pdf - a report describing both Task 1 and Task 2.


---


Running Task 1 using Spark cluster (built using docker containers):

1. Upload the two input text files and the stopwords file to HDFS.

2. Using the submitted JAR file, run the command "bin/spark-submit --class CommonWords <path to JAR> <path to input1> <path to input2> <path to output> <path to input stopwords>", where all the input and output filepaths specified are those on HDFS. The final output is at <output>.


---


Running Task 2 using Spark cluster (built using docker containers):

1. Upload the input CSV file to HDFS.

2. Using the submitted JAR file, run the command "bin/spark-submit --class Assignment2 <path to JAR> <path to input CSV>". Final output will be printed to standard output. (Note that the usual Spark logs will not be printed because I have set the log level to display "ERROR" messages only in the code.
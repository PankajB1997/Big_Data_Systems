File structure in this submitted zip:-


1. Task 1/ directory contains the necessary submission for Task 1, namely:

(i) CommonWords.java - Completed code file

(ii) commonwords_top15_output.txt - Top 15 output of the result, using the data files that were provided

(iii) commonwords_output.txt - All the common words ordered by their frequency in descending order, using the data files that were provided.

(iv) Task1.jar - JAR file that can be used to run the project and verify the result.


2. Task 2/ directory contains the necessary submission for Task 2, namely:

(i) code/ directory contains all the completed code files, along with the input dataset that was provided for this problem, as a txt file. The original dataset was provided as a csv file, it was converted into a txt file for input to the program.

(ii) recommended_items.txt - The recommended items along with the scores for the user with user ID corresponding to last 3 digits of my NUSNET ID (E0009011 => 11). The result is ordered by the calculated score in descending order.

(iii) all_recommended_items.txt - Same result as (ii), but for ALL user IDs. The result is sorted by user ID in ascending order, and for each user ID, by the calculated score in descending order.

(iv) Task2.jar - JAR file that can be used to run the project and verify the result.

3. report.pdf - a report describing the algorithm in both Task 1 and Task 2.


---


Running Task 1 using HDFS:

1. For this task, please note that the path to the stopwords.txt file has been hardcoded in the code to "hdfs://master:9000/user/root/a1_t1/input/stopwords.txt".

2. Upload the two input text files to HDFS.

3. Using the submitted JAR file, run the command "bin/hadoop jar CommonWords.java CommonWords <input1> <output1> <input2> <output2> <output3> <output4>", where all the input and output filepaths specified are those on HDFS. Output will be stored in the specified output paths and the final output is at <output4>.


---


Running Task 2 using HDFS:

1. For this task, the following rules are applied:

(i) The input dataset path is hardcoded in the code to "data.txt". Which means that the dataset should be entered as a .txt file only and it should be located in the same directory as the JAR file. A .txt version of the input dataset that was provided as CSV has been submitted along with this zip, located at "Task 2/code/data.txt".

(ii) The HDFS URI is fixed to "hdfs://master:9000/user/root/a1_t2".

(iii) Since my NUSNET ID is E0009011, I have hardcoded the last 3 digits as 11 in the code. The result of running this JAR will only include the item recommendations for this user ID.

2. Using the submitted JAR file, run the command "bin/hadoop jar Task2.jar Task2.Recommend". Final output will be stored on the HDFS at "hdfs://master:9000/user/root/a1_t2/recommend/step5/part-r-00000".
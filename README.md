# Hypernym-detection-with-Hadoop-and-OCR

# Introduction 
It is an implementation of Learning syntactic patterns for automatic hypernym discovery by R. Snow, D. Jurafsky and A. Ng. - http://ai.stanford.edu/~rion/papers/hypernym_nips05.pdf
It uses an Amazon EMR cluster to process huge amounts of data of
[Google Syntactic Ngrams]
(http://storage.googleapis.com/books/syntactic-ngrams/index.html).
We processed and trained our classifier on the set English All > Biarcs 00.
The ngrams are parsed, stemmed and then a dependency tree is constructed. We use this tree to extract shortest paths between nouns. We use these shortest paths to emit a file, which contains truth data from a pre-tagged test set. This file is then tun through WEKA to train classifiers. The test set is stored in an S3 bucket.

# Dependencies
Type: Large
Load POM.xml

# Short usage tutorial
1.	Extract the project files into project.
2.	Load the dependency by loading the Pom.xml of each project.
3.	Connect to the AWS account and copy the credentials
4.	Set credentials in the AWS credentials profile file on your local system, located at:
~/.aws/credentials on Linux, macOS, or Unix
C:\Users\USERNAME\.aws\credentials on Windows
5.	Under the AWS classes change the constants concerning REGIONS according to the REGIONS given to you in AWS.
Region we used:
US_EAST_1
6.	For each step, change the 'Main-Class' property under the file MANIFEST.MF to the current step, and create jar file.
7.	Upload the all the jars file to your Bucket in AWS and change the jar paths in the main class.
8.	Upload the  hypernym.txt file the bucket in S3 and set the file path in step1 and step2.
9.	Create a folder called "Logs" in your bucket and change the log path in the main class.
10.	 Install Weka - To use Weka in IntelliJ IDEA, follow these steps:
11.	Download the latest version of Weka from the official website: https://www.cs.waikato.ac.nz/ml/weka/downloading.html
12.	Extract the downloaded .zip file to a location on your computer.
13.	Open IntelliJ IDEA and create a new project or open an existing one.
14.	In the project pane on the left, right-click the name of your project and select "Open Module Settings".
15.	In the "Project Structure" window, click the "Libraries" tab.
16.	Click the "+" button and select "Java".
17.	Navigate to the location of the weka.jar file on your computer and select it.
18.	Click "OK" to add the Weka library to your project.
19.	Now you should be able to use the Weka library in your project.
20.	Run the program!

# System Architecture
The system is composed of 3 elements:
o	Main class
o	2 Steps
Each step is scheduled using Amazon Elastic Map-Reduce (EMR).

# Main –  ה
The purpose of the main is to initialize the EMR program, and in particular to define all the configurations required to run a job that includes several steps. Among other things, define the jar paths from which they will be run, define how many instances will run the program, define the log writing path, etc.
![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/f873f4c5-2010-41bd-8e9b-1fb94ff0204e)

In our assignment we made a comparison between two types of dependent paths:
- Method 1: A path which consists exclusively of the lexical category of the word
- Method 2: A path which consists of the word itself and its lexical category
The results of the classifier for the first method are better. 
In addition, we made comparisons to understand what the ultimate DPMIN is. We found that the ultimate DPMIN is 6.
Now we will compare the results with the methods we performed:

# Step1 – 
The purpose of this step is to -
1. Given an input string, find the shortest dependency paths between any 2 nouns in the input line.
2. If the above nouns are in the list of tagged nouns, pass the nouns together with the shortest dependency path that belongs to them to the reducer.
3. For each dependency Path, count whether more than DPMIN pairs of unique nouns are associated with it.
4. For each pair of nouns - we will count the amount of different dependency paths associated with it.
'Mapper' – 
 ![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/e2cde813-bfa7-4b71-aea5-395c2cd23705)

'Reducer' -  
![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/e135ee35-2ae7-44d3-89e6-db4d03099c37)


# Step2 – 
The purpose of this step is to unite all the dependency paths under one key which is '**'. We do this to ensure that through the mechanism of the reducer - shuffle & sort, this key will arrive first from the list of possible keys, thus we guarantee that when we receive a noun pair together with all the dependency paths relevant to it, we will already have the features vector of all the dependency paths. In order to ensure this and save on another step, we require the use of one reducer. 
- 'Mapper' -
![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/20c31e15-08d5-444e-863d-6d7f8fee8811)

- 'Reducer' - 
 ![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/2b535430-d184-4b09-9fa6-086a6bfc092d)

# Scalability
The program is scalable and uses minimal memory.
This can be seen reflected in the fact that instead of keeping all possible noun pairs in memory together with their corresponding dependency paths, we take advantage of the platform that Map-Reduce gives us and already in the first mapper we match each noun pair with the shortest dependency path that corresponds to it. In the reducer of that step, I add up the number of occurrences of each dependency path for a given noun pair. In this way, already in the second stage, we succeed by utilizing MR's shuffle & sort to send the "vector" of the various dependency paths and in each iteration of the reducer create for a given noun pair the corresponding feature vector.

# Effective use of cloud services
The project significantly saves on cloud services.
o	Because of the design we made that optimizes processes at every stage, we save significantly on communication and perform only 4 stages! 
As a result, to get a result in a reasonable time, the EMR system consumes fewer resources and the user can use fewer instances.
o	The system we built makes use of EMR, this system makes many optimizations, from memory storage to efficient communication of the EC2 computers it runs.
o	The data we read is data that is read sequentially through the S3 interface, so there is no need to upload it to our bucket.

# Security
o	The credentials are not written or hard coded anywhere in the project.
o	The credentials are exchanged and saved each time inside the local computer under a folder that we protected with a password

# Running Times
o	Without local aggregation - 1 hour and 15 minutes.
o	With local aggregation - 1 hour and 10 minutes.

# Communication
![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/8db77333-d654-4c4b-b0fe-b431e0c301ae)

# Statistics and Results
Recall, precision, and F-measure- Are evaluation metrics used to assess the performance of binary classification models.
Recall: Recall is a commonly used evaluation metric in binary classification tasks that measures the ability of a model to correctly identify all positive examples in the dataset. In other words, recall measures the fraction of actual positive samples that are correctly predicted as positive by the model. Mathematically, recall is defined as the ratio of true positives (TP) to the sum of true positives and false negatives (FN): Recall = TP / (TP + FN) A high recall score indicates that the model is good at identifying all positive examples, and can be especially important in applications where missing a positive example could have serious consequences.

- Precision: Precision is a commonly used evaluation metric in binary classification tasks that measures the ability of a model to correctly predict positive examples. In other words, precision measures the fraction of predicted positive samples that are actually positive. Mathematically, precision is defined as the ratio of true positives (TP) to the sum of true positives and false positives (FP): Precision = TP / (TP + FP) A high precision score indicates that the model is accurate in its positive predictions, and can be especially important in applications where false positives are costly.

- F-measure: F-measure is used to evaluate the performance of a binary classification model, which classifies examples into ‘positive’ or ‘negative’. It is a weighted harmonic mean of precision (measures the proportion of true positive predictions among all positive predictions made by the model) and recall (measures the proportion of true positive predictions among all actual positive instances in the data).
The F-measure ranges from 0 to 1, with a higher value indicating better performance. The formula for F-measure is:
F-measure = 2 * (precision * recall) / (precision + recall)
Method 1:
![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/18e75da5-c042-463d-9c77-ecf26670e1d9)

Method 2:
![image](https://github.com/IdanArbiv/Hypernym-detection-with-Hadoop-and-OCR/assets/101040591/3d55721b-d127-4b16-9a68-c531f208ee58)


Based on the results, it is evident that the first method outperformed the second method. The error of the first method was 24.8908, while the second method had an error of 26.2009. Therefore, both the communication and prediction data are more accurate in the first method. In conclusion, the first method is superior to the second method in terms of producing better results for this particular task.

# Analysis
- True positives:
•	babi$child
•	back$bed
•	bachelor$man
•	bank$busi
•	ach$pain

- False positives:
•	bit$sort
•	agent$press
•	camp$aid
•	bone$bag
•	architect$heart

The vector exhibits a high level of information density, characterized by a significant number of non-zero entries. This property suggests that there exist numerous viable pathways within the data, which may contribute to the successful classification outcomes observed.

- True negatives:
•	mari$virgin
•	worker$volunt
•	offic$oper
•	student$scienc
•	fan$music

- False negatives:
•	agent$ticket
•	bit$ear
•	bowl$wash
•	bush$mother
•	behalf$offic

The vector is marked by a low level of information density, indicated by a substantial proportion of zero-valued entries. Despite the relative sparsity of the data, the classifier was able to make accurate classifications, potentially leveraging select informative features among the available pathways.

# Conclusions 
The running of the program can be further improved according to the requirements of the program, in this program we did not assume that almost anything can be stored in memory, but in real programs, it may be that the program we want to run will work with data that has a certain limit and therefore it is possible to store certain parameters in memory and significantly optimize the running time on by reducing the running time needed for communication.

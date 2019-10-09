#COMPSCI532_Project1

This repository is for Project 1. Each students are required to develop a mockup of the core functionality of a Web search engine. For more details, you may refer to the project website https://marcoserafini.github.io/teaching/systems-for-data-science/fall19/project1.html

In this repository, there are 1977 documents. Each of these documents are crawled from the internet, and named as the document id. The file id_URL_pairs.txt is the mapping of document id and its URL. Each line represent one document. Your search engine are required to return the URL of the document. 

For group information, please refer to the form here (https://docs.google.com/spreadsheets/d/1FZshWm0o7_WcKcdyc9sAkdbSnUWrlALn74COMBAkWzA/edit?usp=sharing). Please create a group in GitHub Classroom with the corresponding group name shown in the form above. 

A query test plan will be provided for you to check the correctness of your program soon. 

For submission, please include the inverted index, and all the code needed to run your program. 

#PROJECT DOCUMENTATION  

This project is a mockup of the core functionality of a search engine. The project uses Hadoop, Spark and RocksDB to create and store the inverted index, which is a key value pair of words and their corresponding URLs. A RESTful service running on the localhost takes in phrases as queries and returns a list of URLs mathcing the query. The entire codebase is in Java.
It uses the following components:
1. Hadoop(v3.1.2) - This is used to store the content - the docID and content files as well as the id_url_pairs.txt file
2. Spark(v2.4.4) - This is used to create the inverted index - a key value pair of words and the URLs
3. RocksDB(v6.1.2) - This is used to store the generated inverted index, and retrieve it later when a query is received.
4. Springboot(v2.1.6)- This is used to set up a server on localhost which is able to handle GET requests.

#Project Structure:  
The project has the following structure:  

mini-google-group-2  
|__RESTServer  
|   |__src  
|      |__main  
|         |__java  
|            |__springbootServer  
|	        |__MiniGogleServer.java - This is used to start the server on localhost:8081  
|	        |__SearchController.java - This contains the controller for the GET request  
|__src  
   |__main  
      |__java  
         |__com  
            |__sparkInvertedIndex  
               |__SparkInvertedIndex.java - This is used to create the inverted index and store it in RocksDB.  

#Instructions  
  
1. Creating the Inverted index: To create the inverted index, we need to run the mini-google-group-2/src/main/java/com/sparkInvertedIndex/SparkInvertedIndex.java file. In this file we need to provide the values for 3 variables:  
a) contentPath: Provide your hdfs path where your content files are stored. For example, if you have 2000 files stored in the path hdfs://localhost:8020/docfiles/, you will provide this value in this variable.   
b) id_url_path: Provide your hdfs path for your id_url_pairs.txt file. For example, if your file is stored in hdfs://localhost:8020/id_url_pairs/id_url_pairs.txt, you will provide this value in this variable.  
c) RocksdbPath: Provide the absolute path where you want to store the RocksDB files. For example /Users/aayushgupta/IdeaProjects/data/. Note: Absolute path is needed in this case.  
Once we set the value of these variables, we can run the main() function in the SparkInvertedIndex.java file to create the inverted index, which will be stored in RocksDB as key value pairs.   

2. Starting the service: Once the inverted index in created, we can start our server. To do this, we need to set the value of 1 variable in the file mini-google-group-2/RESTServer/src/main/java/springbootServer/SearchController.java:  
a) RocksdbPath : Provide the absolute path of the dir where your RocksDB files are stored. Note: this needs to be an absolute path - the same which we gave earlier while creating the inverted index.  
Once we have provided the value for this variable, we can now start the service. To do this, run the main() function in mini-google-group-2/RESTServer/src/main/java/springbootServer/MiniGoogleServer.java.This should start the server. Note: The server will run on the port 8081 by default. To change this, modift the port number in the file mini-google-group-2/RESTServer/src/main/resources/application.properties file.  

3. Querying: Once the service is up and running, we can now get query results. The format of the query is: localhost:8081/search?query=your query. For example, if the query is "obese person", the request will be localhost:8081/search?query=obese person. Note that the query is not surrounded by quotes. Running this in your browser or Postman will retrieve a list of URLs which is the result.  

#Followup

1. In our inverted index, there is functionality to store the frequency of words per URL, so that we can retrieve the URLs with the maximum frequency. However, this was not in the scope of the project and was not implemented.  
2. While creating the inverted index, we are removing all the special characters to improve the quality of search results.  
   

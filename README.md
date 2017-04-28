# HDInsight Top-N Products using MapReduce, C#

This repository deals with retrieving Top N Products by Ratings (mentioned in detail in the problem description below).
The reviews are present in dataset reviews.csv and its meta is present in metadata.csv

## Datasets
The meta of files in the dataset is as follows

In reviews.csv, each line contains comma-separated values in the following order:
- User_Id:
- Product_Id:
- User_Name:
- Up_votes: Total number of up-votes given to the review
- Overall_votes: Total votes including up-votes and down-votes given to the review
- Review_Text: Review written by the user
- Rating: Rating given to the product by the user
- Summary: Title given to the review by the user
- Unix_Review_Time: Time at which review was written in UNIX time format
- Review_Date: Date on which review was submitted

In metadata.csv, each line contains comma-separated values in the following order:
- Product_Id: Id of the product
- Title: Name of the product
- Price: Price of product in US dollars
- imUrl: url for the product image
- Sales_Rank:
- Brand:
- Category: Category of the product

## Problem Statement
We will be finding the top-N products in each category with the
highest weighted ratings. (For the programming part, N should be user given. In output folder the top-3 from each category are reported.)
To calculate the weighted ratings for a product, first calculate the weighted rating for each review of given product using the formula:

	weighted_review_rating = ((1+upvotes)*rating + downvotes*(6 - rating))/(1+total_votes)

Now perform the average of weighted_review_rating for all reviews of the product to get the weighted_product_rating.
For each category, output top-N product with highest weighted_product_rating. Output should be in the format: 

	<<category, Product_Id, Title>, weighted_product_rating>

Now, category and title attributes are present in Metadata.csv while votes and rating are present in Reviews.csv. So, input is read from multiple files. You can see the example of reading input from multiple files from here.


## Setup

Spinup the HDInsight Cluster on Azure You can check for reference <a href="https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-provision-linux-clusters" target="_blank">here</a>. I have created this cluster on Linux VM. Choose Azure Storage as Default Storage.

Compile and build the projects in TopNProducts.sln and (in either release or debug mode but I prefer release mode for production purposes).

Now upload TopNMapper.exe and TopNReducer.exe to the default azure storage location configured with HDInsight using the Server Explorer. Also upload Reviews.csv file to directory of your choice. For time being I upload it to /reviews/input/Reviews.csv
For beginners follow this <a href="https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-upload-data" target="_blank">link</a> to upload files to HDInsight which provides various interfaces to upload data to an HDInsight cluster.

## Commands
	yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar -files wasbs:///TopNMapper.exe,wasbs:///TopNReducer.exe -mapper TopNMapper.exe -reducer "TopNReducer.exe 3" -input /reviews/input/reviews.csv -output /reviews/output

### Details of Command
The command sends various arguemnts to hadoop-streaming.jar file with yarn as interface that processes the map reduce streaming job.

-files takes map and reduce executables indicating there location on wasbs (Windows Azure Storage Blob).

-mapper with the name of the executable of mapper process. 

-reducer takes the name of the executable of reducer process. Note that here we are also passing integer arguemnt 3 to get top 3 Products in every category. If not argument passed then default N value will be taken as 3

-input is the location of the data to be processed.

-output is the desired location to store the processed data. This needs to be a fresh location every time we run the map reduce process.

After running the command the output folder will contain a text file named part-00000

## Output

To speed up the process we are reading the metadata.csv directly in the reducer to get the product details like category, title etc.,
The output of the mapreduce process gives a file named part-00000 and is located in the output folder.

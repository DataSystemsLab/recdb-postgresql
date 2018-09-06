#Examples

This directory contains scripts along-with the .csv files of all datasets of RecDB's examples. In all, we have written scripts on four large datasets i.e MoiveTweet, MoiveLens 100k, MoiveLens 1M and yelps academic dataset. Following is the brief discription of the datasets:

1. MovieTweet: a rich data set consisting of ratings on movies that were contained in well-structured tweets on twitter. This data set consists 11495 ratings for 4730 movies rated from 5788 users. (1) users (userid, name):It consists all information of the registered user. (2) movies (movieid, name, year, genre): Information about movies are stored in this table. (3) ratings (userid, itemid, rating): Each tuple in the ratings table represents how much a user liked a movie after watching it. For further information about MovieTweet, please visit the following [https://github.com/sidooms/MovieTweetings](link).

2. Yelps Academic Dataset:  a real data set containing user ratings for different business taken from the Yelp recommender system. This data set contains 1048574 ratings for 61184 businesses rated from 366715 users. It consists of three tables 1) users (userid, name, age): It stores the details about registered users. 2) business (businessid, name, location): It maintains all information of businesses which are rated by users. 3) ratings (userid, businessid, ratings): It represents the like and dislike of users for different businesses. For further information about Yelps Academic, please visit the following [https://www.yelp.com/academic_dataset](link). 

3. MoiveLens 100K: MovieLens recommendation systems, contains 100K rating for 1682 movies by 943 users. The data set leveraged by these application consists of three tables: (1) users (userid, name): This table contains all the information of registered users. Each tuple consists of a userid and name. (2) movies (movieid, name, genre): Information about movies are stored in this table. (3)ratings (userid, itemid, rating): Each tuple in the ratings table represents how much a user liked a movie after watching the movie. For further information about MoiveLens 100k, please visit the following [https://grouplens.org/datasets/movielens/](link).

4. MoiveLens 1M: a rich data set consists of user ratings for items taken from the popular MoiveLens recommender system. This dataset consists of 1M ratings for 6040 movies rated by users 3883. The data set leveraged by the system consists of three tables: (1) users (userid, name): This table contains all the information of registered users. Each tuple consists of a userid and name. (2) movies (movieid, name, genre): Information about movies are stored in this table. (3) ratings (userid, itemid, rating): Each tuple in the ratings table represents how much a user liked a movie after watching it. For further information about MoiveLens 1M, please visit the following [https://grouplens.org/datasets/movielens/](link).

##How to run these examples?

Before running, kindly make sure that you have Python 2.7 or above along-with with the package called “psycopg2” which is required to establish connection between Python and RecDB. To `install psycopg2`, run following command:
```
	sudo apt-get install python-psycopg2
```

There are two ways of running these examples.

1. Running all datasets concurrently:

Once you are done with the pre·requisite, execute python file, called script.py, placed in the example directory. Following is the command to execute the `script.py`:

```
	Python <absolute filepath>/<filename>.py
```

It will ask the following user inputs:

1. Host address of the PostgreSQL	
2. Database Name
3. UserName of the PostgreSQL
4. Password of the PostgreSQL

Note:  Before feeding database name, you are required to create the database in PostgreSQL. Following is the command to create a database:

```
	Create database <databasename>;
```

After this, It will start executing the queries for all the datasets one by one. While   executing, It will ask for the absolute path of the data file for every dataset which are placed in the sub-directories of example directory. 

2. Running datasets separately:

Once you are done with the pre·requisite, execute python file, called “script.py”, of each dataset separately placed in the sub-directories of example directory. Following is the command to execute the script.py file:
```
	Python <absolute filepath><filename>.py
```
It will ask for seven user input and following are the user inputs:

1. Host address of the PostgreSQL
2. Database Name
3. Username of the PostgreSQL
4. Password of the PostgreSQL
5. Abs path for user data file
6. Abs path for item data file
7. Abs path for ratings data file

All data file are placed in their respective dataset directory.
Note:  Before feeding database name, you are required to create the database in PostgreSQL. Following is the command to create a database:

```
	Create database <databasename>;
```

Note: Download yelp dataset from the link: [https://drive.google.com/folderview?id=0Bx1NPu82wk9HfkZudHRaT1lTcWl0YmxaNkFvU0U5alJneXNPdEV4REJmdEwxSUVzNDVUUVU&usp=sharing](link) and copy the files to ./examples/yelp/ . 



   

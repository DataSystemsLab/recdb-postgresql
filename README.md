# Welcome to RecDB
RecDB is an Open Source Recommendation Engine Built Entirely Inside PostgreSQL 9.2. RecDB allows application developers to build recommendation applications in a heartbeat through a wide variety of built-in recommendation algorithms like user-user collaborative filtering, item-item collaborative filtering, singular value decomposition. Applications powered by RecDB can produce online and flexible personalized recommendations to end-users. 

![RecDB Logo](http://www-users.cs.umn.edu/~sarwat/RecDB/pics/recdblogo.png)          current version: ```v0.2-alpha```


## How to Get Source Code

You can check out the code, as follows:

```
$ git clone https://github.com/Sarwat/recdb-postgresql.git
```


## Recommended Machine Specifications

RecDB is designed to be run on a Unix operating system. At least 1GB of RAM is recommended for most queries, though when working with very large data sets more RAM may be desirable, especially when you are not working with apriori (materialized) recommenders.

## Building and Installation

Once you've synced with GitHub, the folder should contain the source code for PostgreSQL, as well as several Perl scripts in a directory named "./PostgreSQL/scripts/". If you are familiar with installing PostgreSQL from source, RecDB is installed the exact same way; for the sake of simplicity, however, we have included these scripts that will simplify the process. Note that the installation and remake-related scripts MUST be run from within the PostgreSQL folder in order for them to work correctly.

1) Run the installation script install.pl.

```
perl install.pl [abs_path]
```

[abs_path] is the absolute path to the directory where you want PostgreSQL installed. The directory should exist before running this script. This will also create a folder "data" in the PostgreSQL folder; this is where the database will be located.

2) Run the database server script pgbackend.pl.

```
perl pgbackend.pl
```

The install.pl script stores the install path in a separate file, so there shouldn't be any need to specify it.

3) In a second terminal, run the database interaction script pgfrontend.pl.

```
perl pgfrontend.pl [db_name] [server_host]
```

[db_name] is the name of the database that you intend to use.
[server_host] is the address of the host server running the PostgreSQL backend. If this option is not specified, the script assumes it to be "localhost".

--------------------------------

If you need to rebuild PostgreSQL, there are two options.

If you have not modified the grammar, you can do a quick rebuild with remake.pl.

```
perl remake.pl
```

If you have modified the grammar, you will need to do a longer rebuild with remakefull.pl.

```
perl remakefull.pl [abs_path]
```

[abs_path] is the absolute path to the directory where you want PostgreSQL installed. The directory should exist before running this script.

If you ever want to eliminate the current database , use the clean.pl script.

```
perl clean.pl [db_name] [server_host]
```

## How RecDB Works

### Loading Data
We provide the MovieLens data to build a "Hello-World" movie recommendation application using RecDB. You can load the data using the sql script called "initmovielens1mdatabase.sql" stored in "./PostgreSQL" directory. We provide the dataset at "./PostgreSQL/moviedata / MovieLens1M/" directory.

### Recommendation Query
In the recommendation query, the user needs to specify the ratings table and also specify where the user, item, and rating value columns are in that table. Moreover, the user has to designate the recommendation algorithm to be used to predict item ratings. For example, if MovieRatings(userid,itemid,ratingval) represents the ratings table in a movie recommendation application, then to recommend top-10 movies based on the rating predicted using Item-Item Collaborative filtering (applying cosine similarity measure) algorithm to user 1, the user writes the following SQL:

```
SELECT * FROM MovieRatings R
RECOMMEND R.itemid TO R.userid ON R.ratingval
USING ItemCosCF
WHERE R.userid = 1
ORDER BY R.ratingval
LIMIT 10
```

When you issue a query such as this, the only interesting data will come from the three columns specified in the RECOMMEND clause. Any other columns that exist in the specified ratings tables will be set to 0.

Currently, the available recommendation algorithms that could be passed to the USING clause are the following:

```ItemCosCF``` Item-Item Collaborative Filtering using Cosine Similarity measure.

```ItemPearCF``` Item-Item Collaborative Filtering using Pearson Correlation Similarity measure.

```UserCosCF``` User-User Collaborative Filtering using Cosine Similarity measure. 

```UserPearCF``` User-User Collaborative Filtering using Cosine Similarity measure. 

```SVD``` Simon Funk Singular Value Decomposition. 

Note that if you do not specify which user(s) you want recommendations for, it will generate recommendations for all users, which can take an extremely long time to finish.

### Materializing Recommenders
Users may create recommenders apriori so that when a recommendation query is issued may be answer with less latency.

```
CREATE RECOMMENDER MovieRec ON MovieRatings
USERS FROM userid
ITEMS FROM itemid
EVENTS FROM ratingval
USING ItemCosCF
```
Similarly, materialized recommenders can be removed with the following command:

```
DROP RECOMMENDER MovieRec
```

Note that if you query a materialized recommender, the three columns listed above will be the only ones returned, and attempting to reference any additional columns will result in an error.

### More Complex Queries
The main benefit of implementing the recommendation functionality inside a database enine (PostgreSQL) is to allow for integration with traditional database operations, e.g., selection, projection, join. 
For example, the following query recommends the top 10 Comedy movies to user 1. 
In order to do that, the query joins the recommendation with the Movies table and apply a filter on the movies genre column (genre LIKE '%Comedy%').


```
SELECT * FROM MovieRatings R, Movies M
RECOMMEND R.itemid TO R.userid ON R.ratingval
USING ItemCosCF
WHERE R.userid = 1 AND M.movieid = R.itemid AND M.genre LIKE '%Comedy%'
ORDER BY R.ratingval
LIMIT 10
```

## Authors
* Mohamed Sarwat <http://www-users.cs.umn.edu/~sarwat/>
* James Avery
* Mohamed F. Mokbel <http://www-users.cs.umn.edu/~mokbel/>

## Support or Contact
Having trouble with RecDB ? contact sarwat@cs.umn.edu and we’ll help you sort it out.

Follow [@Rec_DB](https://twitter.com/Rec_DB) on Twitter

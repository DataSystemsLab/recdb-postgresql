### Welcome to RecDB.
An Open Source Recommendation Engine Built Entirely Inside PostgreSQL 9.2. RecDB allows application developers to build recommendation applications in a heartbeat through a wide variety of built-in recommendation algorithms like user-user collaborative filtering, item-item collaborative filtering, singular value decomposition. Applications powered by RecDB can produce online and flexible personalized recommendations to end-users. You can check out the code, as follows:

```
$ git clone https://github.com/Sarwat/recdb-postgresql.git
```


If you're using the GitHub for Mac, simply sync your repository and you'll see the new branch.

### Installation

Once you've synced with GitHub, the folder should contain the source code for PostgreSQL, as well as several Perl scripts in a directory named "./PostgreSQL/scripts/".

1. Run the installation script install.pl.

```
perl install.pl [abs_path]
```

[abs_path] is the absolute path to the directory where you want PostgreSQL installed. The directory should exist before running this script.

2. Run the database server script pgstart.pl.

```
perl pgbackend.pl
```

The install.pl script stores the install path, so there shouldn't be any need to specify it.

3. In a second terminal, run the database interaction script dbstart.pl.

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


### Loading Data
We provide the MovieLens data to build a "Hello-World" movie recommendation application using RecDB. You can load the data using the sql script called "initmovielens1mdatabase.sql" stored in "./PostgreSQL" directory. We provide the dataset at "./PostgreSQL/moviedata / MovieLens1M/" directory.

### Recommendation Query
In the recommendation query, the user needs to specify the ratings table and also specify where the user, item, and rating value columns are in that table. Moreover, the user has to designate the recommendation algorithm to be used to predict item ratings. For example, if MovieRatings(userid,itemid,ratingval) represents the ratings table in a movie recommendation application, then to recommend top-10 movies based the rating prediceted using Item-Item Collaborative filtering (applying cosine similarity measure) algorithm to user 1, the user writes the following SQL:

```
SELECT * FROM MovieRatings R
RECOMMEND R.itemid TO R.userid ON R.ratingval
USING ItemCosCF
WHERE R.userid = 1
OREDER BY R.ratingval
LIMIT 10
```

### Materializing Recommenders
Users may create recommenders apriori so that when a recommendation query is issued may be answer with less latency.

```
CREATE RECOMMENDER ON MovieRatings
USERS FROM userid
ITEMS FROM itemsid
EVENTS FROM ratingid
USING ItemCosCF

```


### Support or Contact
Having trouble with RecDB ? contact sarwat@cs.umn.edu and we’ll help you sort it out.

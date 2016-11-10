####Movie Lens 1m###
#Tests for fixed bugs (Explain, Explain Analyze, ..)#


import psycopg2
import sys
import time 
import os
def main():
	host = raw_input("Enter the host address of postgresql: ")

	dbname = raw_input("Enter the database name: ")

	user = raw_input("Enter the username of postgresql: ")

	password = raw_input("Enter the password of postgresql: ")
    
        #First data set
	UserDataPath = raw_input("Enter the abs path for the first set of user data(.dat file): ")

	ItemDataPath = raw_input("Enter the abs path for the first set of item data(.dat file): ")

	RatingDataPath = raw_input("Enter the abs path for the first set of ratings data(.dat file): ")
    
        #Second recommender data set path
    
        RatingDataPath2 = raw_input("Enter the abs path for the MoiveTweets ratings data(.csv file): ")
        
    
	#Define our connection string
	conn_string = "host='"+host+"' dbname='"+dbname+"' user='"+user+"' password='"+password+"'"
 
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (conn_string)
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
      
	print "Data copying from .csv file to postgresql database"

	import os
 	path = os.getcwd()	

	executionStart = time.time()

	cursor.execute(" set client_encoding = LATIN1;");
	conn.commit() 
	  	
	cursor.execute("create table if not exists users( userid int, age varchar, gender varchar, job varchar, zipcode varchar);");
	conn.commit()
	executionTime = time.time() - executionStart
	print "\n Execution time is  :-" 
	print executionTime
	
	executionStart = time.time()	
	query =	"COPY users(userid,gender,age,job,zipcode) from "+UserDataPath+"DELIMITERS ';';"
	cursor.execute(query);
	conn.commit()	
	executionTime = time.time() - executionStart
	print "\n Execution time is  :-" 
	print executionTime

	cursor.execute("create table if not exists moive( itemid int, name varchar, genre varchar);");
	conn.commit()	
	
	query = "COPY moive(itemid,name,genre) from "+ItemDataPath+" DELIMITERS ';';"
	cursor.execute(query);
	conn.commit()
		
			
	cursor.execute("create table if not exists ratings ( userid int, itemid int, rating real ,garbage varchar);");
	conn.commit()

	query = "COPY ratings(userid,itemid,rating,garbage) from "+RatingDataPath+" DELIMITERS ';';"
	cursor.execute(query);
	conn.commit()

        #Second recommender informations
        cursor.execute("create table if not exists gsratings (userid int, itemid int, rating int);");
        conn.commit()
        executionTime = time.time() - executionStart
               
        query =	"copy gsratings(userid, itemid, rating) from "+RatingDataPath2+" DELIMITER ':';"
        cursor.execute(query);
        conn.commit()




	print "Connected!\n"

        ###############
        print "\n \n Creating Recommender for join of two recommendation queries.."
        executionStart = time.time()
        cursor.execute("CREATE RECOMMENDER mtitemcos on gsratings Users FROM userid Items FROM itemid Events FROM rating Using ItemCosCF;");
        conn.commit()
        executionTime = time.time() - executionStart
        print "\n"
        print (" Execution time is  :-", executionTime)
        
    
        ###############
	
	print "Recommendation query being shooted with ItemCosCF technique"

	###############

	print "\n \n Creating Recommender.."	
	executionStart = time.time()	
	cursor.execute("CREATE RECOMMENDER mlRecItemCos on ratings Users FROM userid Items FROM itemid Events FROM rating Using ItemCosCF;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n Explain the selection of movie for single user.."
	executionStart = time.time()	
	cursor.execute("explain select itemid from ratings RECOMMEND itemid to userid ON rating Using ItemCosCF where userid =21;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

        print "\n \n Explain Analyze for the selection of movie for single user.."
        executionStart = time.time()
        cursor.execute("explain analyze select itemid from ratings RECOMMEND itemid to userid ON rating Using ItemCosCF where userid =21;");
        conn.commit()
        executionTime = time.time() - executionStart
        print " Execution time is  :-"
        print executionTime
        
	################

	print "\n \n Explain the Join Query.."
	executionStart = time.time()	
	cursor.execute("explain select r.itemid, i.name, i,genre, r.rating , r.userid, b.age from ratings r, moive i, users b Recommend r.itemid to r.userid On r.rating Using ItemCosCF where r.userid = 1 and r.userid = b.userid and r.itemid = i.itemid AND i.genre ILIKE '%action%' ;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################
    
        print "\n \n Explain Analyze the Join Query.."
        executionStart = time.time()
        cursor.execute("explain analyze select r.itemid, i.name, i,genre, r.rating , r.userid, b.age from ratings r, moive i, users b Recommend r.itemid to r.userid On r.rating Using ItemCosCF where r.userid = 1 and r.userid = b.userid and r.itemid = i.itemid AND i.genre ILIKE '%action%' ;");
        conn.commit()
        executionTime = time.time() - executionStart
        print " Execution time is  :-"
        print executionTime

	

	################
    
        print "\n \n Join of Two Recommendations.."
        executionStart = time.time()
        cursor.execute("select  t1.itemid, t1.userid, t2.itemid, t2.userid FROM (select * from ratings r  Recommend r.itemid TO r.userid ON r.rating USING ItemCosCF where r.userid =15 limit 10) t1  join (select * from gsratings g  RECOMMEND g.itemid TO g.userid ON g.rating USING ItemCosCF where g.userid between 10 and 20 AND g.itemid between 90000 and 100000) t2   ON t1.userid =t2.userid limit 10;");
        conn.commit()
        executionTime = time.time() - executionStart
        print " Execution time is  :-"
        print executionTime
    
	###############

	print "\n \n Union of Two Recommendations.."
	executionStart = time.time()	
	cursor.execute("(select r.userid from ratings  r  RECOMMEND r.itemid TO r.userid ON r.rating USING ItemCosCF where r.userid =1 )  union (select g.userid from gsratings g   RECOMMEND g.itemid TO g.userid ON g.rating USING ItemCosCF where g.userid =21);");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Recommender in subselect"
	executionStart = time.time()	
	cursor.execute("select u.age, sub.rating from users u, (select * FROM ratings r        RECOMMEND r.itemid TO r.userid ON r.rating USING ItemCosCF WHERE r.itemid in (1,2,3) limit 10) sub WHERE u.userid = sub.userid;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Recommender and IN"
	executionStart = time.time()
	cursor.execute("select * from moive m where m.itemid  IN (SELECT r.itemid FROM ratings r   RECOMMEND r.itemid TO r.userid ON r.rating USING ItemCosCF WHERE r.userid=1  limit 10);");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	cursor.execute("drop table ratings;");
	conn.commit()

	cursor.execute("drop table users;");
	conn.commit()

	cursor.execute("drop table moive;");
	conn.commit()

	cursor.execute("drop table gsratings;");
	conn.commit()


	
 
if __name__ == "__main__":
	main()

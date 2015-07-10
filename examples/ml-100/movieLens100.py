####Movie Lens 100k###

import psycopg2
import sys
import time 
import os
def main():

	host = raw_input("Enter the host address of postgresql: ")

	dbname = raw_input("Enter the database name: ")

	user = raw_input("Enter the username of postgresql: ")

	password = raw_input("Enter the password of postgresql: ")

	UserDataPath = raw_input("Enter the abs path for user data(.CSV file): ")

	ItemDataPath = raw_input("Enter the abs path for item data(.CSV file):: ")

	RatingDataPath = raw_input("Enter the abs path for ratings data(.csv file): ")


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
	  	
	cursor.execute("create table if not exists moive (itemid int, name varchar, date varchar, url varchar);");
	conn.commit()
	executionTime = time.time() - executionStart
	print "\n Execution time is  :-" 
	print executionTime
	
	executionStart = time.time()	
	query =	"copy moive(itemid, name, date,url) from "+ItemDataPath+" with CSV header;"
	cursor.execute(query);
	conn.commit()	
	executionTime = time.time() - executionStart
	print "\n Execution time is  :-" 
	print executionTime

	cursor.execute("create table if not exists users(userid int, age int, sex varchar, job varchar, zipcode varchar);");
	conn.commit()	
	
	query = "copy users(userid, age, sex, job,zipcode) from "+UserDataPath+" with CSV Header;"
	cursor.execute(query);
	conn.commit()
		
			
	cursor.execute("create table if not exists ratings (userid int , itemid int , rating int , timestamp varchar);");
	conn.commit()

	query = "copy ratings(userid, itemid, rating, timestamp) from "+RatingDataPath+";"
	cursor.execute(query);
	conn.commit()

	print "Connected!\n"
	
	print "Recommendation query being shooted with ItemCosCF technique"

	###############

	print "\n \n Creating Recommender.."	
	executionStart = time.time()	
	cursor.execute("CREATE RECOMMENDER ml100itemcos on ratings Users FROM userid Items FROM itemid Events FROM rating Using ItemCosCF;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n Selection of movie for single user.."		
	executionStart = time.time()	
	cursor.execute("SELECT itemid FROM ratings RECOMMEND itemid TO userid ON rating  USING ItemCosCF where userid =21;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Single Join Query.."		
	executionStart = time.time()	
	cursor.execute("SELECT r.itemid, i.name, r.rating  FROM ratings r, moive i RECOMMEND r.itemid to r.userid On r.rating Using ItemCosCF where r.itemid = i.itemid and r.userid = 1;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	################

	print "\n \n Second Join Query.."	
	executionStart = time.time()	
	cursor.execute("select r.itemid, i.name, r.rating , r.userid, b.age  from ratings r, moive i, users b Recommend r.itemid to r.userid On r.rating using ItemCosCF where r.userid = 21 and r.userid = b.userid and r.itemid = i.itemid ;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################


	print "\n \n Order by 10 .."	
	executionStart = time.time()	
	cursor.execute("SELECT itemid FROM ratings RECOMMEND itemid TO userid ON rating  USING ItemCosCF where userid =21 order by rating DESC limit 10;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 50 .."	
	executionStart = time.time()	
	cursor.execute("SELECT itemid FROM ratings RECOMMEND itemid TO userid ON rating  USING ItemCosCF where userid =21 order by rating DESC limit 50;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 100.."	
	executionStart = time.time()	
	cursor.execute("SELECT itemid FROM ratings RECOMMEND itemid TO userid ON rating  USING ItemCosCF where userid =21 order by rating DESC limit 100;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	################
	################

	print "\n Recommendation query being shooted with ItemPearCF technique"

	###############

	print "\n \n Creating Recommender.."	
	executionStart = time.time()	
	cursor.execute("CREATE RECOMMENDER ml100itempear on ratings Users FROM userid Items FROM itemid Events FROM rating Using ItemPearCF;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Selection of movie for single user.."		
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using ItemPearCF where userid =21;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Single Join Query.."		
	executionStart = time.time()	
	cursor.execute("select r.itemid, i.name, r.rating  from ratings r, moive I Recommend r.itemid to r.userid On r.rating Using ItemPearCF where r.itemid = i.itemid and r.userid = 1;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Second Join Query.."	
	executionStart = time.time()	
	cursor.execute("select r.itemid, i.name, r.rating , r.userid, b.age from ratings r, moive i, users b Recommend r.itemid to r.userid On r.rating using ItemPearCF where r.userid = 21 and r.userid = b.userid and r.itemid = i.itemid;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 10 .."	
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using ItemPearCF where userid =21 order by rating DESC limit 10;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 50 .."	
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using ItemPearCF where userid =21 order by rating DESC limit 50;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 100.."	
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using ItemPearCF where userid =21 order by rating DESC limit 100;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################
	################

	print "\n Recommendation query being shooted with SVD technique"

	###############

	print "\n \n Creating Recommender.."	
	executionStart = time.time()	
	cursor.execute("CREATE RECOMMENDER ml100svd on ratings Users FROM userid Items FROM itemid Events FROM rating Using SVD;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print " \n \n Selection of movie for single user.."		
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using SVD where userid =21;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print " \n \n Single Join Query.."		
	executionStart = time.time()	
	cursor.execute("select r.itemid, i.name, r.rating from ratings r, moive i Recommend r.itemid to r.userid On r.rating Using SVD where r.itemid = i.itemid and r.userid = 1;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Second Join Query.."	
	executionStart = time.time()	
	cursor.execute("select r.itemid, i.name, r.rating , r.userid, b.age from ratings r, moive i, users b Recommend r.itemid to r.userid On r.rating using SVD where r.userid = 21 and r.userid = b.userid and r.itemid = i.itemid ;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 10 .."	
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using SVD where userid =21 order by rating DESC limit 10;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 50 .."	
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using SVD where userid =21 order by rating DESC limit 50;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 100.."	
	executionStart = time.time()	
	cursor.execute("select itemid from ratings RECOMMEND itemid to userid ON rating Using SVD where userid =21 order by rating DESC limit 100;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	################


	
 
if __name__ == "__main__":
	main()

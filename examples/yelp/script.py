###Yelp###

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
	  	
	cursor.execute("create table if not exists business(attributes_Accepts varchar, Credit_Cards varchar, attributes_Accepts_Insurance varchar, attributes_Ages_Allowed varchar, attributes_Alcohol varchar, attributes_Ambience varchar,attributes_Attire varchar, attributes_BYOB varchar,attributes_BYOB_Corkage varchar, attributes_By_Appointment_Only varchar ,attributes_Caters varchar, attributes_Coat_Check varchar, attributes_Corkage varchar,attributes_Delivery varchar,attributes_Dietary_Restrictions varchar, attributes_Dogs_Allowed varchar,attributes_Drive_Thru varchar, attributes_Good_For varchar,attributes_Good_For_Dancing varchar, attributes_Good_For_Groups varchar, attributes_Good_For_Kids varchar, attributes_Good_for_Kid varchar, attributes_Hair_Types_Specialized_In varchar, attributes_Happy_Hour varchar,attributes_Has_TV varchar, attributes_Music varchar,attributes_Noise_Level varchar, attributes_Open_24_Hours varchar, attributes_Order_at_Counter varchar, attributes_Outdoor_Seating varchar, attributes_Parking varchar, attributes_Payment_Types varchar, attributes_Price_Range varchar, attributes_Smoking varchar, attributes_Take_out varchar, attributes_Takes_Reservations varchar,attributes_Waiter_Service varchar , attributes_Wheelchair_Accessible varchar, attributes_Wi_Fi varchar, business_id varchar ,categories varchar, city varchar, full_address varchar, hours_Friday varchar, hours_Monday varchar, hours_Saturday varchar,hours_Sunday varchar, hours_Thursday varchar, hours_Tuesday varchar, hours_Wednesday varchar,latitude varchar, longitude varchar, name varchar, neighborhoods varchar, open varchar, review_count varchar, stars varchar , statetype varchar ,rowid int );");
	conn.commit()
	executionTime = time.time() - executionStart
	print "\n Execution time is  :-" 
	print executionTime
	
	executionStart = time.time()	
	query =	"copy business from "+ItemDataPath+" with CSV header;"
	cursor.execute(query);
	conn.commit()	
	executionTime = time.time() - executionStart
	print "\n Execution time is  :-" 
	print executionTime

	cursor.execute("create table if not exists users(average_stars numeric, compliments_cool int,compliments_cute int ,compliments_funny int ,compliments_hot int, compliments_list int ,compliments_more int, compliments_note int, compliments_photos int , compliments_plain int, compliments_profile int, compliments_writer int , elite varchar, fans int , friends varchar, name varchar, review_count int , type varchar, user_id varchar, votes_cool int, votes_funny int,  votes_useful int, yelping_since varchar, rowid int); ");
	conn.commit()	
	
	query = "copy users from "+UserDataPath+" with CSV header;"
	cursor.execute(query);
	conn.commit()
		
			
	##############################
	##############################
	cursor.execute("alter table business rename column attributes_wi_fi to businessid;");
	conn.commit()
	cursor.execute("alter table business drop column business_id;");
	conn.commit()
	

	##############################
	##############################	


	cursor.execute(" create table if not exists ratings (userid varchar, itemid varchar , rating int );");
	conn.commit()

	query = "copy ratings from "+RatingDataPath+" with CSV header;"
	cursor.execute(query);
	conn.commit()

	##############################
	##############################
	cursor.execute("alter table ratings add column user_id int;");
	conn.commit()
	cursor.execute("alter table ratings add column item_id int;");
	conn.commit()
	cursor.execute("update ratings set item_id = business.rowid from business where ratings.itemid  = business.businessid;");
	conn.commit()
	cursor.execute("update ratings set user_id = users.rowid from users where ratings.userid  = users.user_id; ");
	conn.commit()
	cursor.execute("alter table ratings drop column userid;");
	conn.commit()
	cursor.execute("alter table ratings drop column itemid;");
	conn.commit()

	##############################
	##############################	



	print "Connected!\n"
	
	print "Recommendation query being shooted with ItemCosCF technique"

	###############

	print "\n \n Creating Recommender.."	
	executionStart = time.time()	
	cursor.execute("CREATE RECOMMENDER yelp3itemcos on ratings Users FROM user_id Items FROM item_id Events FROM rating Using ItemCosCF;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n Selection of movie for single user.."		
	executionStart = time.time()	
	cursor.execute("select item_id, user_id, rating from ratings RECOMMEND item_id to user_id ON rating Using ItemCosCF where user_id =21;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Single Join Query.."		
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating from ratings r, business b Recommend r.item_id to r.user_id On r.rating Using ItemCosCF where r.item_id = b.rowid and r.user_id = 1;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	################

	print "\n \n Second Join Query.."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemCosCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid ;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################


	print "\n \n Order by 10 .."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemCosCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 10;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 50 .."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemCosCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 50;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 100.."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemCosCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 100;");
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
	cursor.execute("CREATE RECOMMENDER yelpitemPear on ratings Users FROM user_id Items FROM item_id Events FROM rating Using ItemPearCF;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Selection of movie for single user.."		
	executionStart = time.time()	
	cursor.execute("select item_id, user_id, rating from ratings RECOMMEND item_id to user_id ON rating Using ItemPearCF where user_id =21;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print "\n \n  Single Join Query.."		
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating from ratings r, business b Recommend r.item_id to r.user_id On r.rating Using ItemPearCF where r.item_id = b.rowid and r.user_id = 1;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Second Join Query.."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemPearCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid ;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 10 .."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemPearCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 10;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 50 .."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemPearCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 50;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 100.."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using ItemPearCF where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 100;");
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
	cursor.execute("CREATE RECOMMENDER yelpsvd on ratings Users FROM user_id Items FROM item_id Events FROM rating Using SVD; ");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print " \n \n Selection of movie for single user.."		
	executionStart = time.time()	
	cursor.execute("select item_id, user_id, rating from ratings RECOMMEND item_id to user_id ON rating Using SVD where user_id =21;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	###############

	print " \n \n Single Join Query.."		
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating from ratings r, business b Recommend r.item_id to r.user_id On r.rating Using SVD where r.item_id = b.rowid and r.user_id = 1;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Second Join Query.."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using SVD where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid ;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 10 .."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using SVD where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 10;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 50 .."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using SVD where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 50;");
	conn.commit()
	executionTime = time.time() - executionStart
	print " Execution time is  :-" 
	print executionTime

	################

	print "\n \n Order by 100.."	
	executionStart = time.time()	
	cursor.execute("select r.item_id, b.name, r.rating , r.user_id, u.name from ratings r, business b, users u Recommend r.item_id to r.user_id On r.rating using SVD where r.user_id = 21 and r.user_id = u.rowid and r.item_id = b.rowid order by r.rating limit 100;");
	conn.commit()
	executionTime = time.time() - executionStart
	print "Execution time is  :-" 
	print executionTime

	################


	
 
if __name__ == "__main__":
	main()

#!/usr/bin/perl

#Dataset Loading Script

use Cwd;
my $pwd = getcwd;

print "Creating scripts for sample datasets.\n";
open FILE, ">", "initmovielens1mdatabase.sql" or die $!;
print FILE "drop table ml_users;\ndrop table ml_items;\ndrop table ml_ratings;\n\nCREATE TABLE ml_users (systemid serial, PRIMARY KEY (systemId), userid int not null, age varchar, gender varchar, job varchar, zipcode varchar);\nCREATE TABLE ml_items (systemid serial, PRIMARY KEY (systemId), itemid int not null, name varchar, genre varchar);\nCREATE TABLE ml_ratings (ratingid serial, PRIMARY KEY (ratingId), userid int not null, itemid int not null, ratingval real, ratingts real);\n\nset client_encoding = LATIN1;\n";
print FILE "\nCOPY ml_users(userid,gender,age,job,zipcode) from '";
print FILE $pwd;
print FILE "/moviedata/MovieLens1M/users.dat' DELIMITERS ';';\n";
print FILE "COPY ml_items(itemid,name,genre) from '";
print FILE $pwd;
print FILE "/moviedata/MovieLens1M/movies.dat' DELIMITERS ';';\n";
print FILE "COPY ml_ratings(userid,itemid,ratingval,ratingts) from '";
print FILE $pwd;
print FILE "/moviedata/MovieLens1M/ratings.dat' DELIMITERS ';';";
print FILE "\n\ncreate index userid_index on ml_users(userid);\ncreate index itemid_index on ml_items(itemid);\ncreate index userrating_index on ml_ratings(userid);\ncreate index itemrating_index on ml_ratings(itemid);";
close FILE or die $!;

open FILE, ">", "initgeosocialdatabase.sql" or die $!;
print FILE "drop table gs_users;\ndrop table gs_venues;\ndrop table gs_ratings;\n\nCREATE TABLE gs_users (systemid serial, PRIMARY KEY (systemId), userid int not null);\nCREATE TABLE gs_venues (systemid serial, PRIMARY KEY (systemId), venueid int not null, latitude real, longitude real);\nCREATE TABLE gs_ratings (ratingid serial, PRIMARY KEY (ratingId), userid int not null, venueid int not null, ratingval real, ratingts serial);\n\nset client_encoding = LATIN1;\n";
print FILE "\nCOPY gs_users(userid) from '";
print FILE $pwd;
print FILE "/GeoSocial/users.dat' DELIMITERS ';';\n";
print FILE "COPY gs_venues(venueid,latitude,longitude) from '";
print FILE $pwd;
print FILE "/GeoSocial/venues.dat' DELIMITERS ';';\n";
print FILE "COPY gs_ratings(userid,venueid,ratingval) from '";
print FILE $pwd;
print FILE "/GeoSocial/ratings.dat' DELIMITERS ';';";
print FILE "\n\ncreate index userid_index on gs_users(userid);\ncreate index venueid_index on gs_venues(venueid);\ncreate index userid_rating_index on gs_ratings(userid);\ncreate index venueid_rating_index on gs_ratings(venueid);";
close FILE or die $!;

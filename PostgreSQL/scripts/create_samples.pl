#!/usr/bin/perl

use Cwd;
my $pwd = getcwd;

print "Creating scripts for sample datasets.\n";
open FILE, ">", "initmovielens1mdatabase.sql" or die $!;
print FILE "drop table ml_users;\ndrop table ml_items;\ndrop table ml_ratings;\n\nCREATE TABLE ml_users (systemid serial, PRIMARY KEY (systemId), userid int not null, age varchar, gender varchar, job varchar, zipcode varchar);\nCREATE TABLE ml_items (systemid serial, PRIMARY KEY (systemId), itemid int not null, name varchar, genre varchar);\nCREATE TABLE ml_ratings (ratingid serial, PRIMARY KEY (ratingId), userid int not null, itemid int not null, ratingval real, ratingts real);\n\ncreate index userid_index on ml_users(userid);\ncreate index itemid_index on ml_items(itemid);\ncreate index userrating_index on ml_ratings(userid);\ncreate index itemrating_index on ml_ratings(itemid);\n\nset client_encoding = LATIN1;\n";
print FILE "\nCOPY ml_users(userid,gender,age,job,zipcode) from '";
print FILE $pwd;
print FILE "/moviedata/MovieLens1M/users.dat' DELIMITERS ';';\n";
print FILE "COPY ml_items(itemid,name,genre) from '";
print FILE $pwd;
print FILE "/moviedata/MovieLens1M/movies.dat' DELIMITERS ';';\n";
print FILE "COPY ml_ratings(userid,itemid,ratingval,ratingts) from '";
print FILE $pwd;
print FILE "/moviedata/MovieLens1M/ratings.dat' DELIMITERS ';';";
close FILE or die $!;

open FILE, ">", "initfoursquaredatabase.sql" or die $!;
print FILE "drop table fs_users;\ndrop table fs_venues;\ndrop table fs_ratings;\n\nCREATE TABLE fs_users (systemid serial, PRIMARY KEY (systemId), userid int not null, latitude real, longitude real);\nCREATE TABLE fs_venues (systemid serial, PRIMARY KEY (systemId), venueid int not null, latitude real, longitude real);\nCREATE TABLE fs_ratings (ratingid serial, PRIMARY KEY (ratingId), userid int not null, venueid int not null, ratingval real, ratingts serial);\n\ncreate index userid_index on users(userid);\ncreate index venueid_index on venues(venueid);\ncreate index userid_rating_index on ratings(userid);\ncreate index venueid_rating_index on ratings(venueid);\n\nset client_encoding = LATIN1;\n";
print FILE "\nCOPY fs_users(userid,latitude,longitude) from '";
print FILE $pwd;
print FILE "/FourSquare/users.dat' DELIMITERS ';';\n";
print FILE "COPY fs_venues(itemid,latitude,longitude) from '";
print FILE $pwd;
print FILE "/FourSquare/venues.dat' DELIMITERS ';';\n";
print FILE "COPY fs_ratings(userid,venueid,ratingval) from '";
print FILE $pwd;
print FILE "/FourSquare/ratings.dat' DELIMITERS ';';";
close FILE or die $!;

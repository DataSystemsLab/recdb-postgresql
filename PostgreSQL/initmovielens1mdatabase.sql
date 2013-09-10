drop table users;
drop table items;
drop table ratings;
drop table recathonheavyusers;

CREATE TABLE users (systemid serial, PRIMARY KEY (systemId), userid int not null, age varchar, gender varchar, job varchar, zipcode varchar);
CREATE TABLE items (systemid serial, PRIMARY KEY (systemId), itemid int not null, name varchar, genre varchar);
CREATE TABLE ratings (ratingid serial, PRIMARY KEY (ratingId), userid int not null, itemid int not null, ratingval real, ratingts real);

create index userid_index on users(userid);
create index itemid_index on items(itemid);
create index userrating_index on ratings(userid);
create index itemrating_index on ratings(itemid);

set client_encoding = LATIN1;

COPY users(userid,gender,age,job,zipcode) from '/home/administrator/javery/recdb-postgresql/PostgreSQL/moviedata/MovieLens1M/users.dat' DELIMITERS ';';
COPY items(itemid,name,genre) from '/home/administrator/javery/recdb-postgresql/PostgreSQL/moviedata/MovieLens1M/movies.dat' DELIMITERS ';';
COPY ratings(userid,itemid,ratingval,ratingts) from '/home/administrator/javery/recdb-postgresql/PostgreSQL/moviedata/MovieLens1M/ratings.dat' DELIMITERS ';';

CREATE TABLE recathonheavyusers (userid integer primary key);
insert into recathonheavyusers select t.userid from (select userid,count(*) as rating from ratings group by userid order by rating desc) t limit 100;

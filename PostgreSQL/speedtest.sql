create table timer (id serial primary key, time timestamp);
insert into timer (time) select localtimestamp;

create recommender usercoscfrec
users from users key userid
items from items key itemid
ratings from ratings key ratingid
using userCosCF;

insert into timer (time) select localtimestamp;
select T2.time-T1.time from (select time from timer where id=1) T1, (select time from timer where id=2) T2;
drop table timer;

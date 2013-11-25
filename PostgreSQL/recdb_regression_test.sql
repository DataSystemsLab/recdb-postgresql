/* Regression test cases.*/



/* ItemCosCF. */
CREATE RECOMMENDER MovieRec ON ml_ratings USERS FROM userid ITEMS FROM itemid EVENTS FROM ratingval USING itemcoscf;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING itemcoscf WHERE userid = 1;
DROP RECOMMENDER MovieRec;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING itemcoscf WHERE userid = 1;

/* ItemPearCF. */
CREATE RECOMMENDER MovieRec ON ml_ratings USERS FROM userid ITEMS FROM itemid EVENTS FROM ratingval USING itempearcf;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING itempearcf WHERE userid = 1;
DROP RECOMMENDER MovieRec;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING itempearcf WHERE userid = 1;

/* UserCosCF. */
CREATE RECOMMENDER MovieRec ON ml_ratings USERS FROM userid ITEMS FROM itemid EVENTS FROM ratingval USING usercoscf;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING usercoscf WHERE userid = 1;
DROP RECOMMENDER MovieRec;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING usercoscf WHERE userid = 1;

/* UserPearCF. */
CREATE RECOMMENDER MovieRec ON ml_ratings USERS FROM userid ITEMS FROM itemid EVENTS FROM ratingval USING userpearcf;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING userpearcf WHERE userid = 1;
DROP RECOMMENDER MovieRec;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING userpearcf WHERE userid = 1;

/* SVD. */
CREATE RECOMMENDER MovieRec ON ml_ratings USERS FROM userid ITEMS FROM itemid EVENTS FROM ratingval USING svd;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING svd WHERE userid = 1;
DROP RECOMMENDER MovieRec;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING svd WHERE userid = 1;

/* Miscellaneous. */
CREATE RECOMMENDER MovieRec ON ml_ratings USERS FROM userid ITEMS FROM itemid EVENTS FROM ratingval USING itemcoscf;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING itemcoscf WHERE userid IN (1,2,3,5,9) AND itemid < 7;
SELECT r.itemid,r.ratingval,i.name,i.genre FROM ml_ratings r, ml_items i RECOMMEND r.itemid TO r.userid ON r.ratingval USING itemcoscf WHERE r.userid = 1 AND r.itemid = i.itemid AND i.genre ILIKE '%drama%';
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING itemcoscf WHERE userid = 1 ORDER BY ratingval DESC LIMIT 10;
SELECT r.itemid,r.ratingval,i.name,i.genre FROM ml_ratings r, ml_items i RECOMMEND r.itemid TO r.userid ON r.ratingval USING itemcoscf WHERE r.userid = 1 AND r.itemid = i.itemid AND i.genre ILIKE '%action%' ORDER BY ratingval DESC LIMIT 5;
SELECT * FROM ml_ratings RECOMMEND itemid TO userid ON ratingval USING itemcoscf WHERE userid = 1 AND ratingval >= 4.5;
DROP RECOMMENDER MovieRec;

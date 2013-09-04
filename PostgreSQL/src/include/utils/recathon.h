/*-------------------------------------------------------------------------
 *
 * recathon.h
 *
 *
 * Portions Copyright (c) 2012-2013, University of Minnesota
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/recathon.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECATHON_H
#define RECATHON_H

#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "tcop/tcopprot.h"
#include "utils/snapmgr.h"

/* An enum to list all of our recommendation methods. */
typedef enum {
	itemCosCF,
	itemPearCF,
	userCosCF,
	userPearCF,
	SVD
} recMethod;

/* A structure to store a table name (as a RangeVar*) and a
 * column name (as a char*) as a linked list. */
struct attr_node_t {
	RangeVar*		relation;
	char*			colname;
	struct attr_node_t	*next;
};
typedef struct attr_node_t* attr_node;

/* Structures for a linked list of similatity cells. */
struct sim_node_t {
	int			id;
	float			rating;
	struct sim_node_t	*next;
};
typedef struct sim_node_t* sim_node;

/* Structures for a linked list of neighbor nodes.
 * Used when we have a specific neighborhood size. */
struct nbr_node_t {
	int			item1;
	int			item2;
	float			similarity;
	struct nbr_node_t	*next;
};
typedef struct nbr_node_t* nbr_node;

/* Structure to hold rating information for SVD
 * training. Includes space for residual information. */
struct svd_node_t {
	int	userid;
	int	itemid;
	float	rating;
	float	residual;
};
typedef struct svd_node_t* svd_node;

/* A structure for a linked list of cells, for dropping
 * a recommender. */
struct cell_node_t {
	char*			modelname1;
	char*			modelname2;
	char*			viewname;
	struct cell_node_t	*next;
};
typedef struct cell_node_t* cell_node;

/* Similarity node maintenance. */
extern sim_node createSimNode(int userid, float rating);
extern sim_node simInsert(sim_node target, sim_node newnode);
extern void freeSimList(sim_node head);

/* Neighbor node maintenance. */
extern nbr_node createNbrNode(int item1, int item2, float similarity);
extern nbr_node nbrInsert(nbr_node target, nbr_node newnode, int maxsize);
extern void freeNbrList(nbr_node head);

/* Functions for executing queries within the source code. */
extern QueryDesc* recathon_queryStart(char *query_string, MemoryContext *recathoncontext);
extern void recathon_queryEnd(QueryDesc *queryDesc, MemoryContext recathoncontext);
extern void recathon_queryExecute(char *query_string);
extern void recathon_utilityExecute(char *query_string);

/* Functions for building Recommend plans. */
extern RecScan* make_rec_from_scan(Scan *subscan, Node *recommender);
extern RecJoin* make_rec_from_join(Join *subjoin);

/* Functions for extracting data from tuples. */
extern int count_rows(char *tablename);
extern int getTupleInt(TupleTableSlot *slot, char *attname);
extern float getTupleFloat(TupleTableSlot *slot, char *attname);
extern char* getTupleString(TupleTableSlot *slot, char *attname);

/* Functions for checking for existence. */
extern bool relationExists(RangeVar* relation);
extern bool columnExistsInRelation(char *colname, RangeVar *relation);
extern bool recommenderExists(char *recname);

/* Functions for getting recommender data. */
extern void getRecInfo(char *recindexname, char **ret_usertable, char **ret_itemtable,
		char **ret_ratingtable,	char **ret_userkey, char **ret_itemkey,
		char **ret_ratingval, char **ret_method, int *ret_numatts);
extern char** getAttNames(char *recindexname, int numatts, int method);
extern char** getAttValues(char *usertable, char *userkey, char **attvalues, int numatts, int userid);

/* Functions for parsing CreateRStmt data. */
extern attr_node convertAttributes(List* attributes, int *numatts);
extern void freeAttributes(attr_node head);
extern attr_node validateCreateRStmt(CreateRStmt *recStmt, recMethod *method, int *numatts);

/* Functioning for converting a string to a RecMethod. */
extern recMethod getRecMethod(char *method);

/* Function for updating a RecIndex based on an insert. */
extern void updateCellCounter(char *ratingtable, TupleTableSlot *insertslot);

/* Functions for building a recommender based on itemCosCF. */
extern int binarySearch(int *array, int value, int lo, int hi);
extern int *getAllUsers(int numusers, char* usertable);
extern float *vector_lengths(char *tablename, char *key, char *ratingtable, char *ratingval,
	int *totalNum, int **IDlist);
extern float dotProduct(sim_node item1, sim_node item2);
extern float cosineSimilarity(sim_node item1, sim_node item2, float length1, float length2);
extern int updateItemCosModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *itemIDs, float *itemLengths, int numItems, bool update);

/* Functions for building a recommender based on itemPearCF. */
extern void pearson_info(char *tablename, char *key, char *ratingtable,
		char *ratingval, int *totalNum, int **IDlist, float **avgList, float **pearsonList);
extern float pearsonDotProduct(sim_node item1, sim_node item2, float avg1, float avg2);
extern float pearsonSimilarity(sim_node item1, sim_node item2, float avg1, float avg2,
		float pearson1, float pearson2);
extern int updateItemPearModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *itemIDs, float *itemAvgs, float *itemPearsons, int numItems, bool update);

/* Functions for building a user-based recommender. */
extern int updateUserCosModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *userIDs, float *userLengths, int numUsers, bool update);
extern int updateUserPearModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *userIDs, float *userAvgs, float *userPearsons, int numUsers, bool update);

/* Functions for building a SVD recommender. */
extern svd_node createSVDnode(TupleTableSlot *slot, char *userkey, char *itemkey, char *ratingval,
		int *userIDs, int *itemIDs, int numUsers, int numItems);
extern void SVDlists(char *usertable, char *userkey, char *itemtable, char *itemkey,
		int numatts, char **attnames, char **attvalues,
		int **ret_userIDs, int **ret_itemIDs, int *ret_numUsers, int *ret_numItems);
extern void SVDaverages(char *usertable, char *userkey, char *itemtable, char *itemkey, char *ratingtable,
		char *ratingval, int *userIDs, int *itemIDs, int numUsers, int numItems,
		int numatts, char **attnames, char **attvalues,
		float **ret_itemAvgs, float **ret_userOffsets);
extern float predictRating(int featurenum, int numFeatures, int userid, int itemid,
		float **userFeatures, float **itemFeatures, float redisual);
extern int SVDsimilarity(char *usertable, char *userkey, char *itemtable, char *itemkey, char *ratingtable,
		char *ratingval, char *usermodelname, char *itemmodelname,
		char **attnames, char **attvalues, int numatts, bool update);

/* Functions for calculating a rating prediction. */
extern GenHash* hashCreate(int totalItems);
extern void hashAdd(GenHash *table, GenRating *item);
extern GenRating* hashFind(GenHash *table, int itemID);
extern float itemCFpredict(RecScanState *recnode, char *itemmodel, int itemid);
extern float userCFpredict(RecScanState *recnode, char *ratingval, int itemid);
extern float SVDpredict(RecScanState *recnode, char *itemmodel, int itemid);
extern void applyRecScore(RecScanState *recnode, TupleTableSlot *slot, int itemid);

#endif   /* RECATHON_H */

/*-------------------------------------------------------------------------
 *
 * recathon.c
 *	  routines to handle common Recathon functions
 *
 * Portions Copyright (c) 2013, University of Minnesota
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/recathon.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		recathon_queryStart - prepare a query for returning tuples
 *		recathon_queryEnd - clean up after recathon_queryStart
 *		recathon_queryExecute - fully execute a query
 *		recathon_utilityExecute - fully execute a utility statement
 *		make_rec_from_scan - creates a RecScan from a Scan object
 *		relationExists - see if a table exists
 *		columnExistsInRelation - see if a table has a column
 *		binarySearch - simple integer binary search
 *		item_vector_lengths - obtains vector lengths of all items
 *
 *	 NOTES
 *		The query functions are designed to operate on queries
 *		that only generate a single plan tree.
 */

#include <math.h>
#include <unistd.h>
#include "postgres.h"
#include "access/sdir.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/plannodes.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/recathon.h"

#define NBRHOOD 0

static float getUpdateThreshold();
static int getUserFromInsert(TupleTableSlot *insertslot, char *userkey);

/* ----------------------------------------------------------------
 *		createSimNode
 *
 *		Creates a sim_node, used for recommenders.
 * ----------------------------------------------------------------
 */
sim_node
createSimNode(int userid, float rating) {
	sim_node newnode;

	newnode = (sim_node) palloc(sizeof(struct sim_node_t));
	newnode->id = userid;
	newnode->rating = rating;
	newnode->next = NULL;

	return newnode;
}

/* ----------------------------------------------------------------
 *		simInsert
 *
 *		Add one sim_node into a sorted list, via insertion
 *		sort. Sorted by id.
 * ----------------------------------------------------------------
 */
sim_node
simInsert(sim_node target, sim_node newnode) {
	sim_node tempnode;

	// Base case 1: target is empty.
	if (!target) return newnode;

	// Base case 2: target belongs at the head of the list.
	tempnode = target;
	if (newnode->id <= tempnode->id) {
		newnode->next = tempnode;
		return newnode;
	}

	// Normal case.
	while (tempnode->next) {
		if (newnode->id <= tempnode->next->id) break;
		tempnode = tempnode->next;
	}

	newnode->next = tempnode->next;
	tempnode->next = newnode;

	return target;
}

/* ----------------------------------------------------------------
 *		freeSimList
 *
 *		Free a list of sim_nodes.
 * ----------------------------------------------------------------
 */
void
freeSimList(sim_node head) {
	sim_node temp;
	while (head) {
		temp = head->next;
		pfree(head);
		head = temp;
	}
}

/* ----------------------------------------------------------------
 *		createNbrNode
 *
 *		Creates a nbr_node, used for limited-neighborhood
 *		recommenders.
 * ----------------------------------------------------------------
 */
nbr_node
createNbrNode(int item1, int item2, float similarity) {
	nbr_node newnode;

	newnode = (nbr_node) palloc(sizeof(struct nbr_node_t));
	newnode->item1 = item1;
	newnode->item2 = item2;
	newnode->similarity = similarity;
	newnode->next = NULL;

	return newnode;
}

/* ----------------------------------------------------------------
 *		nbrInsert
 *
 *		Add one nbr_node into a sorted list, via insertion
 *		sort. Sorted by similarity.
 * ----------------------------------------------------------------
 */
nbr_node
nbrInsert(nbr_node target, nbr_node newnode, int maxsize) {
	int i;
	nbr_node tempnode;
	bool inserted = false;

	// Base case 1: target is empty.
	if (!target) return newnode;

	// Base case 2: target belongs at the head of the list.
	if (newnode->similarity >= target->similarity) {
		newnode->next = target;
		target = newnode;
		inserted = true;
	}

	tempnode = target;
	i = 1;

	// Normal case.
	while (tempnode->next && i < maxsize) {
		if (newnode->similarity >= tempnode->next->similarity) {
			if (!inserted) {
				newnode->next = tempnode->next;
				tempnode->next = newnode;
				inserted = true;
			}
		}
		tempnode = tempnode->next;
		i++;
	}

	// If we've run out of room on our list.
	if (tempnode->next && i >= maxsize) {
		pfree(tempnode->next);
		tempnode->next = NULL;
	}

	return target;
}

/* ----------------------------------------------------------------
 *		freeNbrList
 *
 *		Free a list of nbr_nodes.
 * ----------------------------------------------------------------
 */
void
freeNbrList(nbr_node head) {
	nbr_node temp;
	while (head) {
		temp = head->next;
		pfree(head);
		head = temp;
	}
}

/* ----------------------------------------------------------------
 *		recathon_queryStart
 *
 *		Takes a query string and applies the specific functions
 *		that execute the query up to the point where we obtain
 *		specific tuples. Borrows code from copy.c.
 *
 *		Returns a query descriptor that tuples can be obtained from.
 * ----------------------------------------------------------------
 */
QueryDesc *
recathon_queryStart(char *query_string, MemoryContext *recathoncontext) {
	List *parsetree_list, *querytree_list, *plantree_list;
	Node *parsetree;
	QueryDesc *queryDesc;
	MemoryContext newcontext, oldcontext;

	// First we'll create a new memory context to operate in.
	newcontext = AllocSetContextCreate(CurrentMemoryContext,
						"RecathonQuery",
						ALLOCSET_DEFAULT_MINSIZE,
						ALLOCSET_DEFAULT_INITSIZE,
						ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(newcontext);

	// Now we parse the query and get a parse tree.
	parsetree_list = pg_parse_query(query_string);

	// There should be only one item in the parse tree.
	parsetree = lfirst(parsetree_list->head);

	// Now we generate plan trees.
	querytree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0);
	plantree_list = pg_plan_queries(querytree_list, 0, NULL);

	// Now we need to update the current snapshot.
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	// We need to do the ExecProcNode stage of the query, which means that we
	// need an intact planstate. The following code just creates this state.
	queryDesc = CreateQueryDesc((PlannedStmt*) linitial(plantree_list),
					query_string,
					GetActiveSnapshot(),
					InvalidSnapshot,
					None_Receiver, NULL, 0);
	ExecutorStart(queryDesc, 0);

	// Return the newly created memory context.
	MemoryContextSwitchTo(oldcontext);
	(*recathoncontext) = newcontext;

	return queryDesc;
}

/* ----------------------------------------------------------------
 *		recathon_queryEnd
 *
 *		Cleans up after recathon_queryStart.
 * ----------------------------------------------------------------
 */
void
recathon_queryEnd(QueryDesc *queryDesc, MemoryContext recathoncontext) {
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(recathoncontext);

	// End the query.
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);

	// Pop our snapshot.
	PopActiveSnapshot();

	// Delete our memory context.
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(recathoncontext);
}

/* ----------------------------------------------------------------
 *		recathon_queryExecute
 *
 *		Totally executes a given query string.
 * ----------------------------------------------------------------
 */
void
recathon_queryExecute(char *query_string) {
	QueryDesc *queryDesc;
	MemoryContext recathoncontext, oldcontext;

	// We do the query start and end, and sandwich ExecutorRun in the middle.
	queryDesc = recathon_queryStart(query_string, &recathoncontext);

	oldcontext = MemoryContextSwitchTo(recathoncontext);
	ExecutorRun(queryDesc, ForwardScanDirection, 0);
	MemoryContextSwitchTo(oldcontext);

	recathon_queryEnd(queryDesc, recathoncontext);
}

/* ----------------------------------------------------------------
 *		recathon_utilityExecute
 *
 *		Queries and utilities are not executed the same way,
 *		so we have another function for things like DROP
 *		statements.
 * ----------------------------------------------------------------
 */
void
recathon_utilityExecute(char *query_string) {
	List *parsetree_list, *querytree_list, *plantree_list;
	MemoryContext recathoncontext, oldcontext;
	Node *parsetree, *utilStmt;

	// We do this inside another memory context
	// so we can rid ourselves of this memory easily.
	recathoncontext = AllocSetContextCreate(CurrentMemoryContext,
						"RecathonExecute",
						ALLOCSET_DEFAULT_MINSIZE,
						ALLOCSET_DEFAULT_INITSIZE,
						ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(recathoncontext);

	// Now we parse the query and get a parse tree.
	parsetree_list = pg_parse_query(query_string);

	// There should be only one item in the parse tree.
	parsetree = lfirst(parsetree_list->head);

	// Now we generate plan trees.
	querytree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0);
	plantree_list = pg_plan_queries(querytree_list, 0, NULL);

	// Isolate the statement.
	utilStmt = (Node*) lfirst(list_head(plantree_list));

	// Execute the query.
	ProcessUtility(utilStmt, query_string, NULL, true, None_Receiver, NULL);

	// Nothing left to do.
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(recathoncontext);
}

/* ----------------------------------------------------------------
 *		make_rec_from_scan
 *
 *		Given a Scan object, create a corresponding
 *		RecScan object. The main tag is RecScan, but the
 *		subscan object has its own, more specific tag.
 * ----------------------------------------------------------------
 */
RecScan*
make_rec_from_scan(Scan *subscan, Node *recommender) {
	RecScan *recscan;

	recscan = (RecScan*) makeNode(RecScan);
	recscan->scan.plan = subscan->plan;
	recscan->scan.scanrelid = subscan->scanrelid;
	recscan->scan.plan.type = T_RecScan;
	recscan->subscan = subscan;
	recscan->recommender = recommender;

	return recscan;
}

/* ----------------------------------------------------------------
 *		make_rec_from_join
 *
 *		Given a Join object, create a corresponding
 *		RecJoin object. The main tag is RecJoin, but the
 *		subjoin object has its own, more specific tag.
 * ----------------------------------------------------------------
 */
RecJoin*
make_rec_from_join(Join *subjoin) {
	RecJoin *recjoin;

	recjoin = (RecJoin*) makeNode(RecJoin);
	recjoin->join.plan = subjoin->plan;
	recjoin->join.jointype = subjoin->jointype;
	recjoin->join.joinqual = subjoin->joinqual;
	recjoin->join.plan.type = T_RecJoin;
	recjoin->subjoin = subjoin;

	return recjoin;
}

/* ----------------------------------------------------------------
 *		count_rows
 *
 *		Given the name of a table, we count the number of
 *		rows in that table. Useful in several places.
 * ----------------------------------------------------------------
 */
int
count_rows(char *tablename) {
	int i, numItems, natts;
	// Objects for querying.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// We start with a simple query to get the number of items.
	querystring = (char*) palloc(256*sizeof(char));
	sprintf(querystring,"SELECT COUNT(*) FROM %s;",tablename);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	if (TupIsNull(slot)) {
		recathon_queryEnd(queryDesc,recathoncontext);
		pfree(querystring);
		return -1;
	}
	slot_getallattrs(slot);
	natts = slot->tts_tupleDescriptor->natts;

	// Silence the compiler.
	numItems = 0;

	for (i = 0; i < natts; i++) {
		if (!slot->tts_isnull[i]) {
			Datum slot_result;
			unsigned int data_type;

			slot_result = slot->tts_values[i];
			data_type = slot->tts_tupleDescriptor->attrs[i]->atttypid;

			switch (data_type) {
				case INT8OID:
					numItems = (int) DatumGetInt64(slot_result);
					break;
				case INT2OID:
					numItems = (int) DatumGetInt16(slot_result);
					break;
				case INT4OID:
					numItems = (int) DatumGetInt32(slot_result);
					break;
				default:
					ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("fatal error in count_rows()")));
			}
		}
	}

	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	return numItems;
}

/* ----------------------------------------------------------------
 *		getTupleInt
 *
 *		Obtain a certain int from a TupleTableSlot. This
 *		will also convert floats into ints.
 * ----------------------------------------------------------------
 */
int
getTupleInt(TupleTableSlot *slot, char *attname) {
	int i, natts;

	slot_getallattrs(slot);
	natts = slot->tts_tupleDescriptor->natts;

	for (i = 0; i < natts; i++) {
		if (!slot->tts_isnull[i]) {
			char *col_name;
			Datum slot_result;

			col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
			slot_result = slot->tts_values[i];

			if (strcmp(col_name, attname) == 0) {
				unsigned int data_type = slot->tts_tupleDescriptor->attrs[i]->atttypid;
				// The data type will tell us what to do with it.
				switch (data_type) {
					case INT8OID:
						return (int) DatumGetInt64(slot_result);
					case INT2OID:
						return (int) DatumGetInt16(slot_result);
					case INT4OID:
						return (int) DatumGetInt32(slot_result);
					case FLOAT4OID:
						return (int) DatumGetFloat4(slot_result);
					case FLOAT8OID:
						return (int) DatumGetFloat8(slot_result);
					default:
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("type mismatch in getTupleInt()")));
						break;
				}
			}
		}
	}

	return -1;
}

/* ----------------------------------------------------------------
 *		getTupleFloat
 *
 *		Obtain a certain float from a TupleTableSlot. This
 *		will also convert ints to floats.
 * ----------------------------------------------------------------
 */
float
getTupleFloat(TupleTableSlot *slot, char *attname) {
	int i, natts;

	slot_getallattrs(slot);
	natts = slot->tts_tupleDescriptor->natts;

	for (i = 0; i < natts; i++) {
		if (!slot->tts_isnull[i]) {
			char *col_name;
			Datum slot_result;

			col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
			slot_result = slot->tts_values[i];

			if (strcmp(col_name, attname) == 0) {
				unsigned int data_type = slot->tts_tupleDescriptor->attrs[i]->atttypid;
				// The data type will tell us what to do with it.
				switch (data_type) {
					case FLOAT8OID:
						return (float) DatumGetFloat8(slot_result);
					case FLOAT4OID:
						return (float) DatumGetFloat4(slot_result);
					case INT8OID:
						return (float) DatumGetInt64(slot_result);
					case INT2OID:
						return (float) DatumGetInt16(slot_result);
					case INT4OID:
						return (float) DatumGetInt32(slot_result);
					default:
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("type mismatch in getTupleFloat()")));
						break;
				}
			}
		}
	}

	return -1.0;
}

/* ----------------------------------------------------------------
 *		getTupleString
 *
 *		Obtain a certain string from a TupleTableSlot. The
 *		underlying data type can be an integer, a float,
 *		a boolean or a variety of string types; we'll
 *		convert any of them to a string.
 * ----------------------------------------------------------------
 */
char*
getTupleString(TupleTableSlot *slot, char *attname) {
	int i, natts;
	// Possible return cases.
	int string_int;
	float string_float;
	bool string_bool;
	char *rtn_string;

	slot_getallattrs(slot);
	natts = slot->tts_tupleDescriptor->natts;

	for (i = 0; i < natts; i++) {
		if (!slot->tts_isnull[i]) {
			char *col_name;
			Datum slot_result;

			col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
			slot_result = slot->tts_values[i];

			if (strcmp(col_name, attname) == 0) {
				unsigned int data_type = slot->tts_tupleDescriptor->attrs[i]->atttypid;

				// The data type will tell us what to do with it.
				switch(data_type) {
					case INT2OID:
						string_int = (int) DatumGetInt16(slot_result);
						rtn_string = (char*) palloc(32*sizeof(char));
						sprintf(rtn_string,"%d",string_int);
						return rtn_string;
					case INT4OID:
						string_int = (int) DatumGetInt32(slot_result);
						rtn_string = (char*) palloc(32*sizeof(char));
						sprintf(rtn_string,"%d",string_int);
						return rtn_string;
					case INT8OID:
						string_int = (int) DatumGetInt64(slot_result);
						rtn_string = (char*) palloc(32*sizeof(char));
						sprintf(rtn_string,"%d",string_int);
						return rtn_string;
					case FLOAT4OID:
						string_float = (float) DatumGetFloat4(slot_result);
						rtn_string = (char*) palloc(128*sizeof(char));
						snprintf(rtn_string,128,"%f",string_float);
						return rtn_string;
					case FLOAT8OID:
						string_float = (float) DatumGetFloat8(slot_result);
						rtn_string = (char*) palloc(128*sizeof(char));
						snprintf(rtn_string,128,"%f",string_float);
						return rtn_string;
					case BOOLOID:
						string_bool = DatumGetBool(slot_result);
						rtn_string = (char*) palloc(8*sizeof(char));
						if (string_bool)
							sprintf(rtn_string,"true");
						else
							sprintf(rtn_string,"false");
						return rtn_string;
					case VARCHAROID:
					case TEXTOID:
					case BPCHAROID:
					case BYTEAOID:
						return TextDatumGetCString(slot_result);
					default:
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unsupported type in getTupleString()")));
				}
			}
		}
	}

	return NULL;
}

/* ----------------------------------------------------------------
 *		relationExists
 *
 *		Function to check if a given relation exists in
 *		the database. Will not work for CTEs.
 * ----------------------------------------------------------------
 */
bool
relationExists(RangeVar* relation) {
	Oid testOid;
	testOid = RangeVarGetRelid(relation,0,true);
	return OidIsValid(testOid);
}

/* ----------------------------------------------------------------
 *		columnExistsInRelation
 *
 *		A function to check if a given column exists in a
 *		given relation. Assumes the table is not a CTE.
 * ----------------------------------------------------------------
 */
bool
columnExistsInRelation(char *colname, RangeVar *relation) {
	Oid relOid;
	Relation newRel;
	RangeTblEntry *rte;
	ListCell *c;
	bool foundColumn;

	// Step 1: build a proper RTE to use.
	relOid = RangeVarGetRelid(relation,0,true);
	// Double-check to make sure the table exists.
	if (!OidIsValid(relOid)) return false;
	newRel = relation_open(relOid,NoLock);
	rte = addRangeTableEntryForRelation(NULL,newRel,NULL,false,false);

	// Step 2: cross-reference the relation columns and
	// our provided column name.
	foundColumn = false;
	foreach(c, rte->eref->colnames)
	{
		if (strcmp(strVal(lfirst(c)), colname) == 0)
		{
			if (foundColumn) {
				perror("Ambiguous column request.\n");
				return false;
			} else {
				foundColumn = true;
			}
		}
	}
	// Close the relation to avoid leaks.
	relation_close(newRel,NoLock);
	pfree(rte);
	return foundColumn;
}

/* ----------------------------------------------------------------
 *		recommenderExists
 *
 *		Function to check if a given recommender has been
 *		created, since we don't maintain a straight list.
 *		Assumes RecModelsCatalogue exists.
 * ----------------------------------------------------------------
 */
bool
recommenderExists(char *recname) {
	char *recindexname;
	bool result;
	// Information for query.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	recindexname = (char*) palloc((strlen(recname)+6)*sizeof(char));
	sprintf(recindexname,"%sIndex",recname);

	// We're going to do a query just to make sure that a result is
	// returned.
	querystring = (char*) palloc(512*sizeof(char));
	sprintf(querystring,"SELECT recommenderId FROM RecModelsCatalogue WHERE recommenderIndexName = '%s';",
				recindexname);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	// Is a slot is returned, then we have a recommender with this name.
	slot = ExecProcNode(planstate);
	result = !TupIsNull(slot);

	// Cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);
	pfree(recindexname);

	return result;
}

/* ----------------------------------------------------------------
 *		getRecInfo
 *
 *		This function looks in the RecModelsCatalogue and
 *		gets some key information. It returns all its
 *		values by reference. If any of the provided return
 *		variables are NULL, we don't return that. This lets
 *		us customize what information we want.
 * ----------------------------------------------------------------
 */
void
getRecInfo(char *recindexname, char **ret_usertable, char **ret_itemtable,
		char **ret_ratingtable,	char **ret_userkey, char **ret_itemkey,
		char **ret_ratingval, char **ret_method, int *ret_numatts) {
	char *usertable, *itemtable, *ratingtable, *userkey, *itemkey, *ratingval, *method;
	// Information for query.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT * FROM RecModelsCatalogue WHERE recommenderindexname = '%s';",
		recindexname);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	// This should never happen.
	if (TupIsNull(slot))
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("fatal error in getRecInfo()")));

	// Obtain each of the values needed.
	if (ret_usertable) {
		usertable = getTupleString(slot,"usertable");
		(*ret_usertable) = usertable;
	}
	if (ret_itemtable) {
		itemtable = getTupleString(slot,"itemtable");
		(*ret_itemtable) = itemtable;
	}
	if (ret_ratingtable) {
		ratingtable = getTupleString(slot,"ratingtable");
		(*ret_ratingtable) = ratingtable;
	}
	if (ret_userkey) {
		userkey = getTupleString(slot,"userkey");
		(*ret_userkey) = userkey;
	}
	if (ret_itemkey) {
		itemkey = getTupleString(slot,"itemkey");
		(*ret_itemkey) = itemkey;
	}
	if (ret_ratingval) {
		ratingval = getTupleString(slot,"ratingval");
		(*ret_ratingval) = ratingval;
	}
	if (ret_method) {
		method = getTupleString(slot,"method");
		(*ret_method) = method;
	}
	if (ret_numatts)
		(*ret_numatts) = getTupleInt(slot,"contextattributes");

	// Cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);
}

/* ----------------------------------------------------------------
 *		getAttNames
 *
 *		Obtain the names of all the context attributes for
 *		a given recommender.
 * ----------------------------------------------------------------
 */
char**
getAttNames(char *recindexname, int numatts, int method) {
	int i, natts, skipatts;
	char **attnames;
	// Objects for our query.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// Allocate memory.
	attnames = (char**) palloc(numatts*sizeof(char*));
	for (i = 0; i < numatts; i++)
		attnames[i] = (char*) palloc(256*sizeof(char));

	// Firstly, how we go about this depends on the recommendation
	// method being used.
	if (((recMethod) method) == SVD)
		skipatts = 10;
	else
		skipatts = 9;

	// We execute a query to obtain the column names. We used to do
	// this by opening a relation manually, but that way is not
	// reliable.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT * FROM %s;",recindexname);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	if (TupIsNull(slot)) {
		recathon_queryEnd(queryDesc, recathoncontext);
		pfree(querystring);
		return NULL;
	}
	slot_getallattrs(slot);
	natts = slot->tts_tupleDescriptor->natts;

	// Error check. Not sure when this would happen, unless the
	// admin is messing with index tables.
	if (natts-skipatts != numatts) {
		recathon_queryEnd(queryDesc, recathoncontext);
		pfree(querystring);
		return NULL;
	}

	// The first nine/ten columns are standard stuff.
	for (i = skipatts; i < natts; i++) {
		if (!slot->tts_isnull[i]) {
			char *col_name;

			col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
			sprintf(attnames[i-skipatts],"%s",col_name);
		}
	}

	recathon_queryEnd(queryDesc, recathoncontext);
	pfree(querystring);

	return attnames;
}

/* ----------------------------------------------------------------
 *		getAttValues
 *
 *		Given a user ID, obtain the values that correspond
 *		to the provided attributes.
 * ----------------------------------------------------------------
 */
char**
getAttValues(char *usertable, char *userkey, char **attnames, int numatts, int userid) {
	int i, j, natts;
	char **attvalues;
	bool match_found[numatts];
	// Query information.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	if (!attnames) return NULL;

	// Allocate memory.
	attvalues = (char**) palloc(numatts*sizeof(char**));
	for (i = 0; i < numatts; i++)
		match_found[i] = false;

	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT * FROM %s WHERE %s = %d;",usertable,userkey,userid);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	if (TupIsNull(slot)) {
		recathon_queryEnd(queryDesc,recathoncontext);
		pfree(querystring);
		for (i = 0; i < numatts; i++)
			pfree(attvalues[i]);
		pfree(attvalues);
		return NULL;
	}
	slot_getallattrs(slot);
	natts = slot->tts_tupleDescriptor->natts;

	// Look through this tuple and pull out attribute values.
	for (i = 0; i < natts; i++) {
		if (!slot->tts_isnull[i]) {
			char *col_name;
			Datum slot_result;
			// What we do depends on the column name.
			col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
			slot_result = slot->tts_values[i];

			// Does this column match one of our attributes?
			for (j = 0; j < numatts; j++) {
				if  (strcmp(col_name,attnames[j]) == 0) {
					attvalues[j] = TextDatumGetCString(slot_result);
					match_found[j] = true;
				}
			}
		}
	}

	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// Check to make sure we obtained all attributes.
	for (i = 0; i < numatts; i++) {
		if (match_found[i] == false) {
			// Error, return NULL.
			for (j = 0; j < numatts; j++)
				pfree(attvalues[j]);
			pfree(attvalues);
			return NULL;
		}
	}

	return attvalues;
}

/* ----------------------------------------------------------------
 *		convertAttributes
 *
 *		A function to take a target_list of attributes and
 *		convert it into a linked list of attr_nodes.
 * ----------------------------------------------------------------
 */
attr_node
convertAttributes(List* attributes, int *numatts) {
	ListCell *templist;
	attr_node head_attr, tail_attr;
	int n = 0;

	if (attributes == NULL) {
		(*numatts) = n;
		return NULL;
	}
	templist = attributes->head;
	head_attr = NULL;
	tail_attr = NULL;
	for (;templist;templist = templist->next) {
		attr_node temp_attr;
		List *templist2;
		ListCell *tempcell;

		temp_attr = (attr_node) palloc(sizeof(struct attr_node_t));
		temp_attr->relation = NULL;
		temp_attr->colname = NULL;
		temp_attr->next = NULL;
		if (head_attr == NULL) {
			head_attr = temp_attr;
			tail_attr = temp_attr;
		} else {
			if (tail_attr == NULL)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("critical failure in convertAttributes()")));
			tail_attr->next = temp_attr;
			tail_attr = temp_attr;
		}

		templist2 = (List*) templist->data.ptr_value;
		tempcell = templist2->head;
		for (;tempcell;tempcell = tempcell->next) {
			Value  *tempvalue;
			char *tempstring;

			tempvalue = (Value*) tempcell->data.ptr_value;
			tempstring = (char*) tempvalue->val.str;
			// Test to see how many periods we have in this term.
			if (temp_attr->relation == NULL)
				temp_attr->relation = makeRangeVar(NULL, tempstring, 0);
			else if (temp_attr->colname == NULL)
				temp_attr->colname = tempstring;
			else
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("improper argument to ATTRIBUTES")));
		}

		// Increment the number.
		n++;
	}

	(*numatts) = n;
	return head_attr;
}

/* ----------------------------------------------------------------
 *		freeAttributes
 *
 *		A function to free a list of attr_nodes.
 * ----------------------------------------------------------------
 */
void
freeAttributes(attr_node head) {
	attr_node temp;
	while (head) {
		temp = head->next;
		pfree(head->relation);
		pfree(head);
		head = temp;
	}
}

/* ----------------------------------------------------------------
 *		validateCreateRStmt
 *
 *		We need to implement a series of sanity checks, to
 *		make sure the data is actually usable.
 * ----------------------------------------------------------------
 */
attr_node
validateCreateRStmt(CreateRStmt *recStmt, recMethod *ret_method, int *ret_numatts) {
	int numatts;
	char *recindexname;
	RangeVar *indexrv;
	recMethod method;
	attr_node head_node, temp_head;

	// Our first test is to make sure the three attribute
	// tables have unique names.
	if (strcmp(recStmt->usertable->relname,recStmt->itemtable->relname) == 0)
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_TABLE),
			 errmsg("user table and item table are the same")));
	if (strcmp(recStmt->usertable->relname,recStmt->ratingtable->relname) == 0)
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_TABLE),
			 errmsg("user table and rating table are the same")));
	if (strcmp(recStmt->itemtable->relname,recStmt->ratingtable->relname) == 0)
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_TABLE),
			 errmsg("item table and rating table are the same")));

	// We next test the to-be-created relation
	// to make sure it doesn't exist.
	recindexname = (char*) palloc((strlen(recStmt->relation->relname)+6)*sizeof(char));
	sprintf(recindexname,"%sindex",recStmt->relation->relname);
	indexrv = makeRangeVar(NULL,recindexname,-1);
	if (relationExists(indexrv))
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_TABLE),
			 errmsg("required table \"%s\" already exists, choose a different recommender name",
				recindexname)));
	pfree(indexrv);
	pfree(recindexname);

	// We then make sure the other relations DO exist.
	// Test: user table exists.
	if (!relationExists(recStmt->usertable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
			 errmsg("relation \"%s\" does not exist",
			 	 recStmt->usertable->relname)));
	// Test: item table exists.
	if (!relationExists(recStmt->itemtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
			 errmsg("relation \"%s\" does not exist",
				recStmt->itemtable->relname)));
	// Test: rating table exists.
	if (!relationExists(recStmt->ratingtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
			 errmsg("relation \"%s\" does not exist",
				recStmt->ratingtable->relname)));

	// We next need to test that the provided columns
	// exist in the appropriate tables.
	// Test: user key is in user table.
	if (!columnExistsInRelation(recStmt->userkey,recStmt->usertable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->userkey,recStmt->usertable->relname)));
	// Test: item key is in item table.
	if (!columnExistsInRelation(recStmt->itemkey,recStmt->itemtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->itemkey,recStmt->itemtable->relname)));
	// Test: rating value is in rating table.
	if (!columnExistsInRelation(recStmt->ratingval,recStmt->ratingtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->ratingval,recStmt->ratingtable->relname)));
	// Test: user key is in rating table.
	if (!columnExistsInRelation(recStmt->userkey,recStmt->ratingtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->userkey,recStmt->ratingtable->relname)));
	// Test: item key is in rating table.
	if (!columnExistsInRelation(recStmt->itemkey,recStmt->ratingtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->itemkey,recStmt->ratingtable->relname)));

	// Test: list of attributes corresponds to real columns and relations
	head_node = convertAttributes(recStmt->attributes, &numatts);

	// For each element in our linked list of attr_node, we need
	// to ensure that the provided column exists in the provided
	// relation. We also need to make sure the specified tables
	// are one of our key three tables.
	for (temp_head = head_node; temp_head; temp_head = temp_head->next) {
		// Extra test case: were we only provided with a
		// column name, and not a table to pair it with?
		// If so, throw this error.
		if (temp_head->colname == NULL)
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("relation not specified for column \"%s\"",
					temp_head->relation->relname)));
		// Now test to make sure the relation exists.
		if (!relationExists(temp_head->relation))
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s\" does not exist",
				 	temp_head->relation->relname)));
		// Next, we test to see if the provided relation is one
		// of the key two relations.
		if ((strcmp(temp_head->relation->relname,recStmt->usertable->relname) != 0) &&
			(strcmp(temp_head->relation->relname,recStmt->itemtable->relname) != 0)) {
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("attribute \"%s.%s\" does not belong to appropriate relation",
				 	temp_head->relation->relname,temp_head->colname)));
		}
		// Lastly, test to see if the column exists in
		// the relation.
		if (!columnExistsInRelation(temp_head->colname,temp_head->relation))
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist in relation \"%s\"",
					temp_head->colname,temp_head->relation->relname)));
	}

	// Now we convert our method name.
	method = itemCosCF;
	// To handle the case where no USING clause was provided.
	if (recStmt->method) {
		method = getRecMethod(recStmt->method);
		if (method < 0)
			ereport(ERROR,
				(errcode(ERRCODE_CASE_NOT_FOUND),
				 errmsg("recommendation method %s not recognized",
					recStmt->method)));
	}

	// Return values.
	(*ret_method) = method;
	(*ret_numatts) = numatts;
	return head_node;
}

/* ----------------------------------------------------------------
 *		getRecMethod
 *
 *		Get the method in recMethod form.
 * ----------------------------------------------------------------
 */
recMethod
getRecMethod(char *method) {
	if (!method) return -1;

	if (strcmp("itemcoscf",method) == 0)
		return itemCosCF;
	else if (strcmp("itempearcf",method) == 0)
		return itemPearCF;
	else if (strcmp("usercoscf",method) == 0)
		return userCosCF;
	else if (strcmp("userpearcf",method) == 0)
		return userPearCF;
	else if (strcmp("svd",method) == 0)
		return SVD;
	else
		return -1;
}

/* ----------------------------------------------------------------
 *		getUpdateThreshold
 *
 *		Get the update threshold.
 * ----------------------------------------------------------------
 */
static float
getUpdateThreshold() {
	float threshold = -1;
	RangeVar *testrv;
	// Query information.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	testrv = makeRangeVar(NULL,"recathonproperties",0);
	if (!relationExists(testrv)) {
		pfree(testrv);
		return -1;
	}
	pfree(testrv);

	querystring = (char*) palloc(128*sizeof(char));
	sprintf(querystring,"SELECT update_threshold FROM recathonproperties;");
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	if (TupIsNull(slot)) {
		recathon_queryEnd(queryDesc,recathoncontext);
		pfree(querystring);
		return -1;
	}

	threshold = getTupleFloat(slot,"update_threshold");

	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	return threshold;
}

/* ----------------------------------------------------------------
 *		getUserFromInsert
 *
 *		Given an insert into a ratings table, identify the
 *		user ID it applies to.
 * ----------------------------------------------------------------
 */
static int
getUserFromInsert(TupleTableSlot *insertslot, char *userkey) {
	int i, natts;

	slot_getallattrs(insertslot);
	natts = insertslot->tts_tupleDescriptor->natts;

	for (i = 0; i < natts; i++) {
		if (!insertslot->tts_isnull[i]) {
			char *col_name;
			Datum slot_result;
			// What we do depends on the column name.
			col_name = insertslot->tts_tupleDescriptor->attrs[i]->attname.data;
			slot_result = insertslot->tts_values[i];

			// Is this our user key column?
			if (strcmp(col_name,userkey) == 0) {
				unsigned int data_type = insertslot->tts_tupleDescriptor->attrs[i]->atttypid;
				// The data type will tell us what to do with it.
				switch (data_type) {
					case INT8OID:
						return (int) DatumGetInt64(slot_result);
					case INT2OID:
						return (int) DatumGetInt16(slot_result);
					case INT4OID:
						return (int) DatumGetInt32(slot_result);
					default:
						// Error, try the next att. (Unlikely.)
						break;
				}
			}
		}
	}

	// Error, can't adjust cells.
	return -1;
}

/* ----------------------------------------------------------------
 *		updateCellCounter
 *
 *		Happens whenever an INSERT occurs. If the insert
 *		is for a ratings table that we've built a
 *		recommender on, we need to update the counters of
 *		the appropriate cells.
 * ----------------------------------------------------------------
 */
void
updateCellCounter(char *ratingtable, TupleTableSlot *insertslot) {
	float update_threshold;
	RangeVar *cataloguerv;
	// Query information.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// If this fails, there's no RecModelsCatalogue, so
	// there are no recommenders.
	cataloguerv = makeRangeVar(NULL,"recmodelscatalogue",0);
	if (!relationExists(cataloguerv)) {
		pfree(cataloguerv);
		return;
	}
	pfree(cataloguerv);

	// Obtain the update threshold.
	update_threshold = getUpdateThreshold();

	// Now that we've confirmed the RecModelsCatalogue
	// exists, let's query it to find the necessary
	// information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT * FROM RecModelsCatalogue WHERE ratingtable = '%s';",
		ratingtable);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		// In case of SVD, recmodelname is the user model, and the other is the
		// item model. Otherwise, recmodelname2 is nothing.
		char *recindexname, *recmodelname, *recmodelname2;
		char *usertable, *itemtable;
		char *userkey, *itemkey, *ratingval, *strmethod;
		recMethod method;
		char **attnames, **attvalues;
		int i, userid;
		int numatts = -1;
		int updatecounter = -1;
		int ratingtotal = -1;
		// Query information for our internal query.
		char *countquerystring;
		QueryDesc *countqueryDesc;
		PlanState *countplanstate;
		TupleTableSlot *countslot;
		MemoryContext countcontext;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		// Acquire the data for this recommender.
		recindexname = getTupleString(slot,"recommenderindexname");
		usertable = getTupleString(slot,"usertable");
		itemtable = getTupleString(slot,"itemtable");
		userkey = getTupleString(slot,"userkey");
		itemkey = getTupleString(slot,"itemkey");
		ratingval = getTupleString(slot,"ratingval");
		strmethod = getTupleString(slot,"method");
		numatts = getTupleInt(slot,"contextattributes");

		// Get the recMethod.
		method = getRecMethod(strmethod);
		pfree(strmethod);

		// Failure case, continue to next tuple.
		if (numatts < 0 || method < 0) {
			pfree(recindexname);
			pfree(usertable);
			pfree(itemtable);
			pfree(userkey);
			pfree(itemkey);
			pfree(ratingval);
			continue;
		}

		// Now that we have the recommender using this table, we need to
		// obtain the context attributes, if there are any, and query
		// the user table for the values.
		if (numatts > 0) {
			// We need to use the insertslot we got from the INSERT
			// command to determine the user ID.
			userid = getUserFromInsert(insertslot, userkey);

			// Failure condition.
			if (userid < 0) {
				pfree(recindexname);
				pfree(usertable);
				pfree(itemtable);
				pfree(userkey);
				pfree(itemkey);
				pfree(ratingval);
				continue;
			}

			attnames = getAttNames(recindexname, numatts, method);
			attvalues = getAttValues(usertable, userkey, attnames, numatts, userid);

			// Failure condition again.
			if (!attnames || !attvalues) {
				if (attnames) {
					for (i = 0; i < numatts; i++)
						pfree(attnames[i]);
					pfree(attnames);
				}
				if (attvalues) {
					for (i = 0; i < numatts; i++)
						pfree(attvalues[i]);
					pfree(attvalues);
				}
				pfree(recindexname);
				pfree(usertable);
				pfree(itemtable);
				pfree(userkey);
				pfree(itemkey);
				pfree(ratingval);
				continue;
			}
		} else {
			attnames = NULL;
			attvalues = NULL;
		}

		// We now have all the information necessary to update this
		// recommender's cell counter. First we need to acquire it,
		// and we might as well get the model name while we're at it.
		countquerystring = (char*) palloc(1024*sizeof(char));
		if (method == SVD)
			sprintf(countquerystring,"SELECT recusermodelname, recitemmodelname, updatecounter, ratingtotal FROM %s",
				recindexname);
		else
			sprintf(countquerystring,"SELECT recmodelname, updatecounter, ratingtotal FROM %s",
				recindexname);
		// Add in attributes, if necessary.
		if (numatts > 0) {
			strncat(countquerystring," WHERE ",7);
			for (i = 0; i < numatts; i++) {
				char addition[256];
				sprintf(addition,"%s = '%s'",attnames[i],attvalues[i]);
				strncat(countquerystring,addition,strlen(addition));
				if (i+1 < numatts)
					strncat(countquerystring," AND ",5);
			}
		}
		strncat(countquerystring,";",1);

		countqueryDesc = recathon_queryStart(countquerystring,&countcontext);
		countplanstate = countqueryDesc->planstate;

		// Go through what should be the only tuple and obtain the data.
		countslot = ExecProcNode(countplanstate);
		if (TupIsNull(countslot)) {
			// More failure conditions. We can't just error out
			// because the INSERT still needs to happen.
			recathon_queryEnd(countqueryDesc,countcontext);
			pfree(countquerystring);
			for (i = 0; i < numatts; i++)
				pfree(attnames[i]);
			pfree(attnames);
			for (i = 0; i < numatts; i++)
				pfree(attvalues[i]);
			pfree(attvalues);
			pfree(recindexname);
			pfree(usertable);
			pfree(itemtable);
			pfree(userkey);
			pfree(itemkey);
			pfree(ratingval);
			continue;
		}

		// Get the relevant data.
		if (method == SVD) {
			recmodelname = getTupleString(countslot,"recusermodelname");
			recmodelname2 = getTupleString(countslot,"recitemmodelname");
		} else {
			recmodelname = getTupleString(countslot,"recmodelname");
			recmodelname2 = NULL;
		}
		updatecounter = getTupleInt(countslot,"updatecounter");
		ratingtotal = getTupleInt(countslot,"ratingtotal");

		recathon_queryEnd(countqueryDesc,countcontext);
		pfree(countquerystring);

		// Even more failure conditions.
		if (updatecounter < 0) {
			pfree(recmodelname);
			if (recmodelname2)
				pfree(recmodelname2);
			for (i = 0; i < numatts; i++)
				pfree(attnames[i]);
			pfree(attnames);
			for (i = 0; i < numatts; i++)
				pfree(attvalues[i]);
			pfree(attvalues);
			pfree(recindexname);
			pfree(usertable);
			pfree(itemtable);
			pfree(userkey);
			pfree(itemkey);
			continue;
		}

		// With that done, we check the original counter. If the
		// number of new ratings is greater than threshold * the
		// number of ratings currently used in the model, we need
		// to trigger an update. Otherwise, just increment.
		updatecounter++;

		if (updatecounter >= (int) (update_threshold * ratingtotal)) {
			int numRatings = 0;

			// What we do depends on the recommendation method.
			switch (method) {
				case itemCosCF:
					{
					// Before we update the similarity model, we need to obtain
					// a few item-related things.
					int numItems;
					int *IDs;
					float *lengths;

					lengths = vector_lengths(itemtable, itemkey, ratingtable, ratingval,
						&numItems, &IDs);

					// Now update the similarity model.
					numRatings = updateItemCosModel(usertable, itemtable, ratingtable, userkey,
						itemkey, ratingval, recmodelname, numatts, attnames, attvalues,
						IDs, lengths, numItems, true);
					}
					break;
				case itemPearCF:
					{
					// Before we update the similarity model, we need to obtain
					// a few item-related things.
					int numItems;
					int *IDs;
					float *avgs, *pearsons;

					pearson_info(itemtable, itemkey, ratingtable, ratingval, &numItems,
							&IDs, &avgs, &pearsons);

					// Now update the similarity model.
					numRatings = updateItemPearModel(usertable, itemtable, ratingtable, userkey,
						itemkey, ratingval, recmodelname, numatts, attnames, attvalues,
						IDs, avgs, pearsons, numItems, true);
					}
					break;
				case userCosCF:
					{
					// Before we update the similarity model, we need to obtain
					// a few user-related things.
					int numUsers;
					int *IDs;
					float *lengths;

					lengths = vector_lengths(usertable, userkey, ratingtable, ratingval,
						&numUsers, &IDs);

					// Now update the similarity model.
					numRatings = updateUserCosModel(usertable, itemtable, ratingtable, userkey,
						itemkey, ratingval, recmodelname, numatts, attnames, attvalues,
						IDs, lengths, numUsers, true);
					}
					break;
				case userPearCF:
					{
					// Before we update the similarity model, we need to obtain
					// a few user-related things.
					int numUsers;
					int *IDs;
					float *avgs, *pearsons;

					pearson_info(usertable, userkey, ratingtable, ratingval, &numUsers,
							&IDs, &avgs, &pearsons);

					// Now update the similarity model.
					numRatings = updateUserPearModel(usertable, itemtable, ratingtable, userkey,
						itemkey, ratingval, recmodelname, numatts, attnames, attvalues,
						IDs, avgs, pearsons, numUsers, true);
					}
					break;
				case SVD:
					// No additional functions, just update the model.
					numRatings = SVDsimilarity(usertable, userkey, itemtable, itemkey,
						ratingtable, ratingval, recmodelname, recmodelname2, attnames,
						attvalues, numatts, true);
					break;
				default:
					break;
			}

			// Finally, we update the cell to indicate how many ratings were used
			// to build it. We'll also reset the updatecounter.
			countquerystring = (char*) palloc(1024*sizeof(char));
			sprintf(countquerystring,"UPDATE %s SET updatecounter = 0, ratingtotal = %d",
							recindexname,numRatings);
			// Add in attributes, if necessary.
			if (numatts > 0) {
				strncat(countquerystring," WHERE ",7);
				for (i = 0; i < numatts; i++) {
					char addition[256];
					sprintf(addition,"%s = '%s'",attnames[i],attvalues[i]);
					strncat(countquerystring,addition,strlen(addition));
					if (i+1 < numatts)
						strncat(countquerystring," AND ",5);
				}
			}
			strncat(countquerystring,";",1);

			// Execute normally, we don't need to see results.
			recathon_queryExecute(countquerystring);
			pfree(countquerystring);
		} else {
			// Just increment.
			countquerystring = (char*) palloc(1024*sizeof(char));
			sprintf(countquerystring,"UPDATE %s SET updatecounter = updatecounter+1",
							recindexname);
			// Add in attributes, if necessary.
			if (numatts > 0) {
				strncat(countquerystring," WHERE ",7);
				for (i = 0; i < numatts; i++) {
					char addition[256];
					sprintf(addition,"%s = '%s'",attnames[i],attvalues[i]);
					strncat(countquerystring,addition,strlen(addition));
					if (i+1 < numatts)
						strncat(countquerystring," AND ",5);
				}
			}
			strncat(countquerystring,";",1);
			// Execute normally, we don't need to see results.
			recathon_queryExecute(countquerystring);
			pfree(countquerystring);
		}

		// Final cleanup.
		pfree(recmodelname);
		if (recmodelname2)
			pfree(recmodelname2);
		for (i = 0; i < numatts; i++)
			pfree(attnames[i]);
		pfree(attnames);
		for (i = 0; i < numatts; i++)
			pfree(attvalues[i]);
		pfree(attvalues);
		pfree(recindexname);
		pfree(usertable);
		pfree(itemtable);
		pfree(userkey);
		pfree(itemkey);
		pfree(ratingval);
	}

	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);
}

/* ----------------------------------------------------------------
 *		binarySearch
 *
 *		A quick binary search algorithm, for use with the
 *		CREATE RECOMMENDER query.
 * ----------------------------------------------------------------
 */
int
binarySearch(int *array, int value, int lo, int hi) {
	int mid;

	mid = (hi + lo) / 2;
	if (array[mid] == value) return mid;
	// Edge case.
	if (mid == lo) return -1;
	// Normal recursive case.
	if (array[mid] < value)
		return binarySearch(array, value, mid, hi);
	else
		return binarySearch(array, value, lo, mid);
}

/* ----------------------------------------------------------------
 *		vector_lengths
 *
 *		Looks at all the items in a certain table and
 *		calculates their vector lengths. Used to determine
 *		cosine similarity. Also returns the number of items
 *		or users as a side effect, and a list of them.
 * ----------------------------------------------------------------
 */
float*
vector_lengths(char *tablename, char *key, char *ratingtable, char *ratingval, int *totalNum, int **IDlist) {
	int *IDs;
	float *lengths;
	int i, j, numItems;
	// Objects for querying.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// We start by getting the number of items, which is the number of
	// rows in the item table.
	numItems = count_rows(tablename);

	// Now that we have the number of items, we can create an array or two.
	IDs = (int*) palloc(numItems*sizeof(int));
	lengths = (float*) palloc(numItems*sizeof(float));
	for (j = 0; j < numItems; j++)
		lengths[j] = 0.0;

	// Now we need to populate the array, first with item IDs.
	querystring = (char*) palloc(256*sizeof(char));
	sprintf(querystring,"SELECT DISTINCT %s FROM %s ORDER BY %s;",
		key,tablename,key);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	i = 0;

	// This query grabs all item IDs, so we can store them. Later we'll calculate
	// vector lengths.
	for (;;) {
		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		IDs[i] = getTupleInt(slot,key);

		// Increment and continue.
		i++;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// Now that we have all of the item IDs, a third query will
	// allow us to calculate vector lengths.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT * FROM %s;", ratingtable);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	// We scan through the entire rating table once, sorting the ratings
	// based on which user/item they apply to.
	for (;;) {
		int itemnum, itemindex;
		float rating;

		itemnum = 0; rating = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		itemnum = getTupleInt(slot,key);
		rating = getTupleFloat(slot,ratingval);

		// We have the item number and rating value from this tuple.
		// Now we need to update scores.
		itemindex = binarySearch(IDs, itemnum, 0, numItems);
		if (itemindex < 0) continue;
		lengths[itemindex] += rating*rating;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// Now that we've totally queried the ratings table, we need to
	// take the square root of each length and we're done.
	for (i = 0; i < numItems; i++)
		lengths[i] = sqrtf(lengths[i]);

	// Return data.
	(*totalNum) = numItems;
	(*IDlist) = IDs;

	return lengths;
}

/* ----------------------------------------------------------------
 *		dotProduct
 *
 *		Function to compare two items and compute their dot
 *		product. Used for determining cosine similarity.
 *		The lists are necessarily sorted, so we can compare
 *		in linear time.
 * ----------------------------------------------------------------
 */
float
dotProduct(sim_node item1, sim_node item2) {
	sim_node temp1, temp2;
	float similarity;

	if (item1 == NULL || item2 == NULL) return 0;

	similarity = 0.0;

	// Check every rating for the first item, and see how
	// many of those users also rated the second item.
	temp1 = item1; temp2 = item2;
	while (temp1 && temp2) {
		if (temp1->id == temp2->id) {
			similarity += temp1->rating * temp2->rating;
			temp1 = temp1->next;
			temp2 = temp2->next;
		} else if (temp1->id > temp2->id) {
			temp2 = temp2->next;
		} else {
			temp1 = temp1->next;
		}
	}

	return similarity;
}

/* ----------------------------------------------------------------
 *		cosineSimilarity
 *
 *		Function to compare two items and compute their
 *		cosine similarity.
 * ----------------------------------------------------------------
 */
float
cosineSimilarity(sim_node item1, sim_node item2, float length1, float length2) {
	float numerator;
	float denominator;

	// Short-circuit check. If one of the items has no ratings,
	// no point checking similarity. This also avoids a possible
	// divide-by-zero error.
	denominator = length1 * length2;
	if (denominator <= 0) return 0;

	numerator = dotProduct(item1,item2);
	if (numerator <= 0) return 0;
	else return numerator / denominator;
}

/* ----------------------------------------------------------------
 *		updateItemCosModel
 *
 *		Given a single cell of a recommender, this
 *		function rebuilds the recModel for that cell, using
 *		item-based collaborative filtering with cosine
 *		similarity. Returns the number of ratings used.
 * ----------------------------------------------------------------
 */
int
updateItemCosModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *itemIDs, float *itemLengths, int numItems, bool update) {
	int i, j;
	int numRatings = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *itemRatings;
	// Information for other queries.
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;
	// Information for writing to file.
	FILE *fp;
	temprecfile = (char*) palloc(256*sizeof(char));
	sprintf(temprecfile,"recathon_temp_%s.dat",modelname);

	// If this is us updating a cell as opposed to building
	// a recommender, we need to drop the existing entries.
	if (update) {
		char *dropstring;

		dropstring = (char*) palloc(256*sizeof(char));
		sprintf(dropstring,"DELETE FROM %s;",modelname);
		recathon_queryExecute(dropstring);
		pfree(dropstring);
	}

	// With the precomputation done, we need to derive the actual item
	// similarities. We can do this in a way that's linear in the number
	// of I/Os and also the amount of storage. The complexity is relegated
	// to in-memory calculations, which is the most affordable. We need to
	// use this data structure here.
	itemRatings = (sim_node*) palloc(numItems*sizeof(sim_node));
	for (i = 0; i < numItems; i++)
		itemRatings[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the ratings table
	// in order to get the key information. We'll also keep track of the number
	// of ratings used, since we need to store that information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r",
		userkey,itemkey,ratingval,ratingtable);
	// An optional join.
	if (numatts > 0) {
		char addition[512];
		sprintf(addition,", %s u WHERE u.%s=r.%s",usertable,userkey,userkey);
		strncat(querystring,addition,strlen(addition));
	}
	// Add in attribute information.
	for (i = 0; i < numatts; i++) {
		char addition[512];
		sprintf(addition," AND u.%s = '%s'",attnames[i],attvalues[i]);
		strncat(querystring,addition,strlen(addition));
	}

	// Add the final "ORDER BY".
	strncat(querystring," ORDER BY r.",12);
	strncat(querystring,userkey,strlen(userkey));
	strncat(querystring,";",1);
	// Begin extracting data.
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;

	for (;;) {
		int simuser, simitem, simindex;
		float simrating;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simrating = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simrating = getTupleFloat(simslot,ratingval);

		// We now have the user, item, and rating for this tuple.
		// We insert the results as a sim_node into the
		// itemRatings table; we'll do calculations later.
		newnode = createSimNode(simuser, simrating);
		simindex = binarySearch(itemIDs, simitem, 0, numItems);
		// It's unlikely, but possible, that the binary search will
		// return nothing. This means that we've found a rating on an
		// item that isn't actually in our item list. It's unclear how
		// this might happen, but it has before, so we need to address
		// it.
		if (simindex < 0)
			pfree(newnode);
		else {
			itemRatings[simindex] = simInsert(itemRatings[simindex], newnode);
			numRatings++;
		}
	}

	// Query cleanup.
	recathon_queryEnd(simqueryDesc, simcontext);

	// We're going to write out the results to file.
	if ((fp = fopen(temprecfile,"w")) == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("failed to open temporary file")));
	insertstring = (char*) palloc(128*sizeof(char));

	// Now we do the similarity calculations. Note that we
	// don't include duplicate entries, to save time and space.
	// The first item ALWAYS has a lower value than the second.
	for (i = 0; i < numItems; i++) {
		float length_i;
		sim_node item_i;
		nbr_node temp_nbr;
		nbr_node nbr_list = NULL;

		item_i = itemRatings[i];
		if (!item_i) continue;
		length_i = itemLengths[i];

		for (j = i+1; j < numItems; j++) {
			float length_j;
			sim_node item_j;
			int item1, item2;
			float similarity;

			item_j = itemRatings[j];
			if (!item_j) continue;
			length_j = itemLengths[j];

			similarity = cosineSimilarity(item_i, item_j, length_i, length_j);
			if (similarity <= 0) continue;
			item1 = itemIDs[i];
			item2 = itemIDs[j];

			// Now we write.
			if (NBRHOOD <= 0) {
				sprintf(insertstring,"%d;%d;%f\n",
					item1,item2,similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			} else {
				nbr_node newnbr = createNbrNode(item1,item2,similarity);
				nbr_list = nbrInsert(nbr_list,newnbr,NBRHOOD);
			}
		}

		// If we have a limited neighborhood, we write the results here.
		if (NBRHOOD > 0) {
			for (temp_nbr = nbr_list; temp_nbr; temp_nbr = temp_nbr->next) {
				sprintf(insertstring,"%d;%d;%f\n",temp_nbr->item1,
					temp_nbr->item2,temp_nbr->similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			}
			freeNbrList(nbr_list);
		}

		CHECK_FOR_INTERRUPTS();
	}

	pfree(insertstring);
	fclose(fp);

	// If we are updating an existing similarity model,
	// we will want to drop the existing primary key
	// constraint before doing the copy, to save time.
	if (update) {
		sprintf(querystring,"ALTER TABLE %s DROP CONSTRAINT %s_pkey;",
					modelname,modelname);
		recathon_utilityExecute(querystring);
	}

	// With all the data written out, we're going to
	// issue a COPY FROM command to bulk load the data
	// into the database.
	sprintf(querystring,"COPY %s FROM '%s' DELIMITERS ';';",
		modelname,temprecfile);
	recathon_utilityExecute(querystring);

	// Now we add the primary key constraint. It's
	// faster to add it after adding the data than
	// having it incrementally updated.
	sprintf(querystring,"ALTER TABLE %s ADD PRIMARY KEY (item1, item2)",modelname);
	recathon_utilityExecute(querystring);
	pfree(querystring);

	// We'll delete the temporary file here, to not hold onto
	// memory longer than necessary.
	if (unlink(temprecfile) < 0)
		ereport(WARNING,
			(errcode(ERRCODE_WARNING),
			 errmsg("failed to delete temporary file")));

	// Free up the lists of sim_nodes and start again.
	for (i = 0; i < numItems; i++) {
		freeSimList(itemRatings[i]);
		itemRatings[i] = NULL;
	}

	// Return the number of ratings we used.
	return numRatings;
}

/* ----------------------------------------------------------------
 *		pearson_info
 *
 *		Looks at all the items in our users/items table and
 *		calculates their average ratings, as well as
 *		another data item useful for Pearson correlation,
 *		which I'm just calling a Pearson because I'm not
 *		sure it has a name. It can be pre-calculated, so we
 *		will. Also returns a list of user/item IDs.
 * ----------------------------------------------------------------
 */
void
pearson_info(char *tablename, char *key, char *ratingtable, char *ratingval,
		int *totalNum, int **IDlist, float **avgList, float **pearsonList) {
	int *IDs, *counts;
	float *avgs, *pearsons;
	int i, j, numItems;
	// Objects for querying.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// We start by getting the number of items, which is the number of
	// rows in the item table.
	numItems = count_rows(tablename);

	// Now that we have the number of items, we can create an array or two.
	IDs = (int*) palloc(numItems*sizeof(int));
	counts = (int*) palloc(numItems*sizeof(int));
	for (j = 0; j < numItems; j++)
		counts[j] = 0;
	avgs = (float*) palloc(numItems*sizeof(float));
	for (j = 0; j < numItems; j++)
		avgs[j] = 0.0;
	pearsons = (float*) palloc(numItems*sizeof(float));
	for (j = 0; j < numItems; j++)
		pearsons[j] = 0.0;

	// Now we need to populate the array, first with item IDs.
	querystring = (char*) palloc(256*sizeof(char));
	sprintf(querystring,"SELECT DISTINCT %s FROM %s ORDER BY %s;",
		key,tablename,key);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	i = 0;

	// This query grabs all item IDs, so we can store them. Later we'll calculate
	// averages and Pearsons.
	for (;;) {
		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		IDs[i] = getTupleInt(slot,key);

		// Increment and continue.
		i++;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// Now that we have all of the item IDs, a third query will
	// allow us to calculate averages.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT * FROM %s;", ratingtable);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	// We scan through the entire rating table once, sorting the ratings
	// based on which item they apply to.
	for (;;) {
		int itemnum, itemindex;
		float rating;

		itemnum = 0; rating = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		itemnum = getTupleInt(slot,key);
		rating = getTupleFloat(slot,ratingval);

		// We have the item number and rating value from this tuple.
		// Now we need to update averages.
		itemindex = binarySearch(IDs, itemnum, 0, numItems);
		if (itemindex < 0) continue;
		counts[itemindex] += 1;
		avgs[itemindex] += rating;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);

	// Now that we've totally queried the ratings table, we need to
	// obtain the actual averages for each item.
	for (i = 0; i < numItems; i++) {
		if (counts[i] > 0)
			avgs[i] /= ((float)counts[i]);
	}
	pfree(counts);

	// We can reuse the same query to obtain the ratings again, and
	// calculate Pearsons.
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	// We scan through the entire rating table once, sorting the ratings
	// based on which item they apply to.
	for (;;) {
		int itemnum, itemindex;
		float rating, difference;

		itemnum = 0; rating = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		itemnum = getTupleInt(slot,key);
		rating = getTupleFloat(slot,ratingval);

		// We have the item number and rating value from this tuple.
		// Now we need to update Pearsons.
		itemindex = binarySearch(IDs, itemnum, 0, numItems);
		if (itemindex < 0) continue;
		difference = rating - avgs[itemindex];
		pearsons[itemindex] += difference*difference;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// Now that we've totally queried the ratings table, we need to
	// take the square root of each Pearson and we're done.
	for (i = 0; i < numItems; i++)
		pearsons[i] = sqrtf(pearsons[i]);

	// Return data.
	(*totalNum) = numItems;
	(*IDlist) = IDs;
	(*avgList) = avgs;
	(*pearsonList) = pearsons;
}

/* ----------------------------------------------------------------
 *		pearsonDotProduct
 *
 *		Function to compare two items and compute a version
 *		of their dot product, for Pearson similarity.
 *		The lists are necessarily sorted, so we can compare
 *		in linear time.
 * ----------------------------------------------------------------
 */
float
pearsonDotProduct(sim_node item1, sim_node item2, float avg1, float avg2) {
	sim_node temp1, temp2;
	float similarity;

	if (item1 == NULL || item2 == NULL) return 0.0;

	similarity = 0.0;

	// Check every rating for the first item, and see how
	// many of those users also rated the second item.
	temp1 = item1; temp2 = item2;
	while (temp1 && temp2) {
		if (temp1->id == temp2->id) {
			similarity += (temp1->rating - avg1) * (temp2->rating - avg2);
			temp1 = temp1->next;
			temp2 = temp2->next;
		} else if (temp1->id > temp2->id) {
			temp2 = temp2->next;
		} else {
			temp1 = temp1->next;
		}
	}

	return similarity;
}

/* ----------------------------------------------------------------
 *		pearsonSimilarity
 *
 *		Function to compare two items and compute their
 *		Pearson similarity.
 * ----------------------------------------------------------------
 */
float
pearsonSimilarity(sim_node item1, sim_node item2, float avg1, float avg2,
			float pearson1, float pearson2) {
	float numerator;
	float denominator;

	// Short-circuit check. If one of the items has no ratings,
	// no point checking similarity. This also avoids a possible
	// divide-by-zero error.
	denominator = pearson1 * pearson2;
	if (denominator == 0.0) return 0.0;

	numerator = pearsonDotProduct(item1,item2,avg1,avg2);
	if (numerator == 0.0) return 0.0;
	else return numerator / denominator;
}

/* ----------------------------------------------------------------
 *		updateItemPearModel
 *
 *		Given a single cell of a recommender, this
 *		function rebuilds the recModel for that cell, using
 *		item-based collaborative filtering with Pearson
 *		similarity. Returns the number of ratings used.
 * ----------------------------------------------------------------
 */
int
updateItemPearModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *itemIDs, float *itemAvgs, float *itemPearsons, int numItems, bool update) {
	int i, j;
	int numRatings = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *itemRatings;
	// Information for other queries.
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;
	// Information for writing to file.
	FILE *fp;
	temprecfile = (char*) palloc(256*sizeof(char));
	sprintf(temprecfile,"recathon_temp_%s.dat",modelname);

	// If this is us updating a cell as opposed to building
	// a recommender, we need to drop the existing entries.
	if (update) {
		char *dropstring;

		dropstring = (char*) palloc(256*sizeof(char));
		sprintf(dropstring,"DELETE FROM %s;",modelname);
		recathon_queryExecute(dropstring);
		pfree(dropstring);
	}

	// With the precomputation done, we need to derive the actual item
	// similarities. We can do this in a way that's linear in the number
	// of I/Os and also the amount of storage. The complexity is relegated
	// to in-memory calculations, which is the most affordable. We need to
	// use this data structure here.
	itemRatings = (sim_node*) palloc(numItems*sizeof(sim_node));
	for (i = 0; i < numItems; i++)
		itemRatings[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the ratings table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r",
		userkey,itemkey,ratingval,ratingtable);
	// An optional join.
	if (numatts > 0) {
		char addition[512];
		sprintf(addition,", %s u WHERE u.%s=r.%s",usertable,userkey,userkey);
		strncat(querystring,addition,strlen(addition));
	}
	// Add in attribute information.
	for (i = 0; i < numatts; i++) {
		char addition[512];
		sprintf(addition," AND u.%s = '%s'",attnames[i],attvalues[i]);
		strncat(querystring,addition,strlen(addition));
	}

	// Add the final "ORDER BY".
	strncat(querystring," ORDER BY r.",12);
	strncat(querystring,userkey,strlen(userkey));
	strncat(querystring,";",1);

	// Begin extracting data.
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;

	for (;;) {
		int simuser, simitem, simindex;
		float simrating;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simrating = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simrating = getTupleFloat(simslot,ratingval);

		// We now have the user, item, and rating for this tuple.
		// We insert the results as a sim_node into the
		// itemRatings table; we'll do calculations later.
		newnode = createSimNode(simuser, simrating);
		simindex = binarySearch(itemIDs, simitem, 0, numItems);
		// It's unlikely, but possible, that the binary search will
		// return nothing. This means that we've found a rating on an
		// item that isn't actually in our item list. It's unclear how
		// this might happen, but it has before, so we need to address
		// it.
		if (simindex < 0)
			pfree(newnode);
		else {
			itemRatings[simindex] = simInsert(itemRatings[simindex], newnode);
			numRatings++;
		}
	}

	// Query cleanup.
	recathon_queryEnd(simqueryDesc, simcontext);
	pfree(querystring);

	// We're going to write out the results to file.
	if ((fp = fopen(temprecfile,"w")) == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("failed to open temporary file")));
	insertstring = (char*) palloc(128*sizeof(char));

	// Now we do the similarity calculations. Note that we
	// don't include duplicate entries, to save time and space.
	// The first item ALWAYS has a lower value than the second.
	for (i = 0; i < numItems; i++) {
		float avg_i, pearson_i;
		sim_node item_i;
		nbr_node temp_nbr;
		nbr_node nbr_list = NULL;

		item_i = itemRatings[i];
		if (!item_i) continue;
		avg_i = itemAvgs[i];
		pearson_i = itemPearsons[i];

		for (j = i+1; j < numItems; j++) {
			float avg_j, pearson_j;
			sim_node item_j;
			int item1, item2;
			float similarity;

			item_j = itemRatings[j];
			if (!item_j) continue;
			avg_j = itemAvgs[j];
			pearson_j = itemPearsons[j];

			similarity = pearsonSimilarity(item_i, item_j, avg_i, avg_j, pearson_i, pearson_j);
			if (similarity == 0.0) continue;
			item1 = itemIDs[i];
			item2 = itemIDs[j];

			// Now we write.
			if (NBRHOOD <= 0) {
				sprintf(insertstring,"%d;%d;%f\n",item1,item2,similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			} else {
				nbr_node newnbr = createNbrNode(item1,item2,similarity);
				nbr_list = nbrInsert(nbr_list,newnbr,NBRHOOD);
			}
		}

		// If we have a limited neighborhood, we write the results here.
		if (NBRHOOD > 0) {
			for (temp_nbr = nbr_list; temp_nbr; temp_nbr = temp_nbr->next) {
				sprintf(insertstring,"%d;%d;%f\n",temp_nbr->item1,
					temp_nbr->item2,temp_nbr->similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			}
			freeNbrList(nbr_list);
		}

		CHECK_FOR_INTERRUPTS();
	}

	pfree(insertstring);
	fclose(fp);

	// If we are updating an existing similarity model,
	// we will want to drop the existing primary key
	// constraint before doing the copy, to save time.
	if (update) {
		insertstring = (char*) palloc(1024*sizeof(char));
		sprintf(insertstring,"ALTER TABLE %s DROP CONSTRAINT %s_pkey;",
					modelname,modelname);
		recathon_utilityExecute(insertstring);
		pfree(insertstring);
	}

	// With all the data written out, we're going to
	// issue a COPY FROM command to bulk load the data
	// into the database.
	insertstring = (char*) palloc(1024*sizeof(char));
	sprintf(insertstring,"COPY %s FROM '%s' DELIMITERS ';';",
		modelname,temprecfile);
	recathon_utilityExecute(insertstring);
	pfree(insertstring);

	// Now we add the primary key constraint. It's
	// faster to add it after adding the data than
	// having it incrementally updated.
	insertstring = (char*) palloc(1024*sizeof(char));
	sprintf(insertstring,"ALTER TABLE %s ADD PRIMARY KEY (item1, item2)",modelname);
	recathon_utilityExecute(insertstring);
	pfree(insertstring);

	// We'll delete the temporary file here, to not hold onto
	// memory longer than necessary.
	if (unlink(temprecfile) < 0)
		ereport(WARNING,
			(errcode(ERRCODE_WARNING),
			 errmsg("failed to delete temporary file")));

	// Free up the lists of sim_nodes and start again.
	for (i = 0; i < numItems; i++) {
		freeSimList(itemRatings[i]);
		itemRatings[i] = NULL;
	}

	// Return the number of ratings we used.
	return numRatings;
}

/* ----------------------------------------------------------------
 *		updateUserCosModel
 *
 *		Given a single cell of a recommender, this
 *		function rebuilds the recModel for that cell, using
 *		user-based collaborative filtering with cosine
 *		similarity. Returns the number of ratings used.
 * ----------------------------------------------------------------
 */
int
updateUserCosModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *userIDs, float *userLengths, int numUsers, bool update) {
	int i, j;
	int numRatings = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *userRatings;
	// Information for other queries.
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;
	// Information for writing to file.
	FILE *fp;
	temprecfile = (char*) palloc(256*sizeof(char));
	sprintf(temprecfile,"recathon_temp_%s.dat",modelname);

	// If this is us updating a cell as opposed to building
	// a recommender, we need to drop the existing entries.
	if (update) {
		char *dropstring;

		dropstring = (char*) palloc(256*sizeof(char));
		sprintf(dropstring,"DELETE FROM %s;",modelname);
		recathon_queryExecute(dropstring);
		pfree(dropstring);
	}

	// With the precomputation done, we need to derive the actual user
	// similarities. We can do this in a way that's linear in the number
	// of I/Os and also the amount of storage. The complexity is relegated
	// to in-memory calculations, which is the most affordable. We need to
	// use this data structure here.
	userRatings = (sim_node*) palloc(numUsers*sizeof(sim_node));
	for (i = 0; i < numUsers; i++)
		userRatings[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all user pairs. We need to query the ratings table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r",
		userkey,itemkey,ratingval,ratingtable);
	// An optional join.
	if (numatts > 0) {
		char addition[512];
		sprintf(addition,", %s u WHERE u.%s=r.%s",usertable,userkey,userkey);
		strncat(querystring,addition,strlen(addition));
	}
	// Add in attribute information.
	for (i = 0; i < numatts; i++) {
		char addition[512];
		sprintf(addition," AND u.%s = '%s'",attnames[i],attvalues[i]);
		strncat(querystring,addition,strlen(addition));
	}

	// Add the final "ORDER BY".
	strncat(querystring," ORDER BY r.",12);
	strncat(querystring,userkey,strlen(userkey));
	strncat(querystring,";",1);

	// Begin extracting data.
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;

	for (;;) {
		int simuser, simitem, simindex;
		float simrating;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simrating = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simrating = getTupleFloat(simslot,ratingval);

		// We now have the user, item, and rating for this tuple.
		// We insert the results as a sim_node into the
		// userRatings table; we'll do calculations later.
		newnode = createSimNode(simitem, simrating);
		simindex = binarySearch(userIDs, simuser, 0, numUsers);
		// It's unlikely, but possible, that the binary search will
		// return nothing. This means that we've found a rating on an
		// item that isn't actually in our item list. It's unclear how
		// this might happen, but it has before, so we need to address
		// it.
		if (simindex < 0)
			pfree(newnode);
		else {
			userRatings[simindex] = simInsert(userRatings[simindex], newnode);
			numRatings++;
		}
	}

	// Query cleanup.
	recathon_queryEnd(simqueryDesc, simcontext);
	pfree(querystring);

	// We're going to write out the results to file.
	if ((fp = fopen(temprecfile,"w")) == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("failed to open temporary file")));
	insertstring = (char*) palloc(128*sizeof(char));

	// Now we do the similarity calculations. Note that we
	// don't include duplicate entries, to save time and space.
	// The first user ALWAYS has a lower value than the second.
	for (i = 0; i < numUsers; i++) {
		float length_i;
		sim_node user_i;
		nbr_node temp_nbr;
		nbr_node nbr_list = NULL;

		user_i = userRatings[i];
		if (!user_i) continue;
		length_i = userLengths[i];

		for (j = i+1; j < numUsers; j++) {
			float length_j;
			sim_node user_j;
			int user1, user2;
			float similarity;

			user_j = userRatings[j];
			if (!user_j) continue;
			length_j = userLengths[j];

			similarity = cosineSimilarity(user_i, user_j, length_i, length_j);
			if (similarity <= 0) continue;
			user1 = userIDs[i];
			user2 = userIDs[j];

			// Now we write.
			if (NBRHOOD <= 0) {
				sprintf(insertstring,"%d;%d;%f\n",user1,user2,similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			} else {
				nbr_node newnbr = createNbrNode(user1,user2,similarity);
				nbr_list = nbrInsert(nbr_list,newnbr,NBRHOOD);
			}
		}

		// If we have a limited neighborhood, we write the results here.
		if (NBRHOOD > 0) {
			for (temp_nbr = nbr_list; temp_nbr; temp_nbr = temp_nbr->next) {
				sprintf(insertstring,"%d;%d;%f\n",temp_nbr->item1,
					temp_nbr->item2,temp_nbr->similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			}
			freeNbrList(nbr_list);
		}

		CHECK_FOR_INTERRUPTS();
	}

	pfree(insertstring);
	fclose(fp);

	// If we are updating an existing similarity model,
	// we will want to drop the existing primary key
	// constraint before doing the copy, to save time.
	if (update) {
		insertstring = (char*) palloc(1024*sizeof(char));
		sprintf(insertstring,"ALTER TABLE %s DROP CONSTRAINT %s_pkey;",
					modelname,modelname);
		recathon_utilityExecute(insertstring);
		pfree(insertstring);
	}

	// With all the data written out, we're going to
	// issue a COPY FROM command to bulk load the data
	// into the database.
	insertstring = (char*) palloc(1024*sizeof(char));
	sprintf(insertstring,"COPY %s FROM '%s' DELIMITERS ';';",
		modelname,temprecfile);
	recathon_utilityExecute(insertstring);
	pfree(insertstring);

	// Now we add the primary key constraint. It's
	// faster to add it after adding the data than
	// having it incrementally updated.
	insertstring = (char*) palloc(1024*sizeof(char));
	sprintf(insertstring,"ALTER TABLE %s ADD PRIMARY KEY (user1, user2)",modelname);
	recathon_utilityExecute(insertstring);
	pfree(insertstring);

	// We'll delete the temporary file here, to not hold onto
	// memory longer than necessary.
	if (unlink(temprecfile) < 0)
		ereport(WARNING,
			(errcode(ERRCODE_WARNING),
			 errmsg("failed to delete temporary file")));

	// Free up the lists of sim_nodes and start again.
	for (i = 0; i < numUsers; i++) {
		freeSimList(userRatings[i]);
		userRatings[i] = NULL;
	}

	// Return the number of ratings we used.
	return numRatings;
}

/* ----------------------------------------------------------------
 *		updateUserPearModel
 *
 *		Given a single cell of a recommender, this
 *		function rebuilds the recModel for that cell, using
 *		user-based collaborative filtering with Pearson
 *		similarity. Returns the number of ratings used.
 * ----------------------------------------------------------------
 */
int
updateUserPearModel(char *usertable, char *itemtable, char *ratingtable, char *userkey, char *itemkey,
		char *ratingval, char *modelname, int numatts, char **attnames, char **attvalues,
		int *userIDs, float *userAvgs, float *userPearsons, int numUsers, bool update) {
	int i, j;
	int numRatings = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *userRatings;
	// Information for other queries.
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;
	// Information for writing to file.
	FILE *fp;
	temprecfile = (char*) palloc(256*sizeof(char));
	sprintf(temprecfile,"recathon_temp_%s.dat",modelname);

	// If this is us updating a cell as opposed to building
	// a recommender, we need to drop the existing entries.
	if (update) {
		char *dropstring;

		dropstring = (char*) palloc(256*sizeof(char));
		sprintf(dropstring,"DELETE FROM %s;",modelname);
		recathon_queryExecute(dropstring);
		pfree(dropstring);
	}

	// With the precomputation done, we need to derive the actual item
	// similarities. We can do this in a way that's linear in the number
	// of I/Os and also the amount of storage. The complexity is relegated
	// to in-memory calculations, which is the most affordable. We need to
	// use this data structure here.
	userRatings = (sim_node*) palloc(numUsers*sizeof(sim_node));
	for (i = 0; i < numUsers; i++)
		userRatings[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the ratings table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r",
		userkey,itemkey,ratingval,ratingtable);
	// An optional join.
	if (numatts > 0) {
		char addition[512];
		sprintf(addition,", %s u WHERE u.%s=r.%s",usertable,userkey,userkey);
		strncat(querystring,addition,strlen(addition));
	}
	// Add in attribute information.
	for (i = 0; i < numatts; i++) {
		char addition[512];
		sprintf(addition," AND u.%s = '%s'",attnames[i],attvalues[i]);
		strncat(querystring,addition,strlen(addition));
	}

	// Add the final "ORDER BY".
	strncat(querystring," ORDER BY r.",12);
	strncat(querystring,userkey,strlen(userkey));
	strncat(querystring,";",1);

	// Begin extracting data.
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;

	for (;;) {
		int simuser, simitem, simindex;
		float simrating;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simrating = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simrating = getTupleFloat(simslot,ratingval);

		// We now have the user, item, and rating for this tuple.
		// We insert the results as a sim_node into the
		// itemRatings table; we'll do calculations later.
		newnode = createSimNode(simitem, simrating);
		simindex = binarySearch(userIDs, simuser, 0, numUsers);
		// It's unlikely, but possible, that the binary search will
		// return nothing. This means that we've found a rating on an
		// item that isn't actually in our item list. It's unclear how
		// this might happen, but it has before, so we need to address
		// it.
		if (simindex < 0)
			pfree(newnode);
		else {
			userRatings[simindex] = simInsert(userRatings[simindex], newnode);
			numRatings++;
		}
	}

	// Query cleanup.
	recathon_queryEnd(simqueryDesc, simcontext);
	pfree(querystring);

	// We're going to write out the results to file.
	if ((fp = fopen(temprecfile,"w")) == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("failed to open temporary file")));
	insertstring = (char*) palloc(128*sizeof(char));

	// Now we do the similarity calculations. Note that we
	// don't include duplicate entries, to save time and space.
	// The first item ALWAYS has a lower value than the second.
	for (i = 0; i < numUsers; i++) {
		float avg_i, pearson_i;
		sim_node user_i;
		nbr_node temp_nbr;
		nbr_node nbr_list = NULL;

		user_i = userRatings[i];
		if (!user_i) continue;
		avg_i = userAvgs[i];
		pearson_i = userPearsons[i];

		for (j = i+1; j < numUsers; j++) {
			float avg_j, pearson_j;
			sim_node user_j;
			int user1, user2;
			float similarity;

			user_j = userRatings[j];
			if (!user_j) continue;
			avg_j = userAvgs[j];
			pearson_j = userPearsons[j];

			similarity = pearsonSimilarity(user_i, user_j, avg_i, avg_j, pearson_i, pearson_j);
			if (similarity == 0.0) continue;
			user1 = userIDs[i];
			user2 = userIDs[j];

			// Now we write.
			if (NBRHOOD <= 0) {
				sprintf(insertstring,"%d;%d;%f\n",user1,user2,similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			} else {
				nbr_node newnbr = createNbrNode(user1,user2,similarity);
				nbr_list = nbrInsert(nbr_list,newnbr,NBRHOOD);
			}
		}

		// If we have a limited neighborhood, we write the results here.
		if (NBRHOOD > 0) {
			for (temp_nbr = nbr_list; temp_nbr; temp_nbr = temp_nbr->next) {
				sprintf(insertstring,"%d;%d;%f\n",temp_nbr->item1,
					temp_nbr->item2,temp_nbr->similarity);
				fwrite(insertstring,1,strlen(insertstring),fp);
			}
			freeNbrList(nbr_list);
		}

		CHECK_FOR_INTERRUPTS();
	}

	pfree(insertstring);
	fclose(fp);

	// If we are updating an existing similarity model,
	// we will want to drop the existing primary key
	// constraint before doing the copy, to save time.
	if (update) {
		insertstring = (char*) palloc(1024*sizeof(char));
		sprintf(insertstring,"ALTER TABLE %s DROP CONSTRAINT %s_pkey;",
					modelname,modelname);
		recathon_utilityExecute(insertstring);
		pfree(insertstring);
	}

	// With all the data written out, we're going to
	// issue a COPY FROM command to bulk load the data
	// into the database.
	insertstring = (char*) palloc(1024*sizeof(char));
	sprintf(insertstring,"COPY %s FROM '%s' DELIMITERS ';';",
		modelname,temprecfile);
	recathon_utilityExecute(insertstring);
	pfree(insertstring);

	// Now we add the primary key constraint. It's
	// faster to add it after adding the data than
	// having it incrementally updated.
	insertstring = (char*) palloc(1024*sizeof(char));
	sprintf(insertstring,"ALTER TABLE %s ADD PRIMARY KEY (user1, user2)",modelname);
	recathon_utilityExecute(insertstring);
	pfree(insertstring);

	// We'll delete the temporary file here, to not hold onto
	// memory longer than necessary.
	if (unlink(temprecfile) < 0)
		ereport(WARNING,
			(errcode(ERRCODE_WARNING),
			 errmsg("failed to delete temporary file")));

	// Free up the lists of sim_nodes and start again.
	for (i = 0; i < numUsers; i++) {
		freeSimList(userRatings[i]);
		userRatings[i] = NULL;
	}

	// Return the number of ratings we used.
	return numRatings;
}

/* ----------------------------------------------------------------
 *		createSVDnode
 *
 *		This function creates a new SVD node out of a
 *		TupleTableSlot.
 * ----------------------------------------------------------------
 */
svd_node createSVDnode(TupleTableSlot *slot, char *userkey, char *itemkey, char *ratingval,
				int *userIDs, int *itemIDs, int numUsers, int numItems) {
	int userid, itemid;
	svd_node new_svd;

	// Quiet the compiler.
	userid = -1;
	itemid = -1;

	new_svd = (svd_node) palloc(sizeof(struct svd_node_t));
	// Default values.
	new_svd->userid = -1;
	new_svd->itemid = -1;
	new_svd->rating = -1;
	new_svd->residual = 0.0;

	userid = getTupleInt(slot,userkey);
	itemid = getTupleInt(slot,itemkey);
	new_svd->rating = getTupleFloat(slot,ratingval);

	// If we convert IDs to indexes in our arrays, it will make
	// our lives easier.
	new_svd->userid = binarySearch(userIDs,userid,0,numUsers);
	new_svd->itemid = binarySearch(itemIDs,itemid,0,numItems);

	return new_svd;
}

/* ----------------------------------------------------------------
 *		SVDlists
 *
 *		This function generates user and item lists for use
 *		in SVD.
 * ----------------------------------------------------------------
 */
void
SVDlists(char *usertable, char *userkey, char *itemtable, char *itemkey,
		int numatts, char **attnames, char **attvalues, int **ret_userIDs, int **ret_itemIDs,
		int *ret_numUsers, int *ret_numItems) {
	int i, numUsers, numItems;
	int *userIDs, *itemIDs;
	char *querystring;
	// Information for other queries.
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	querystring = (char*) palloc(1024*sizeof(char));

	// First, let's get the list of users. We need to count how many
	// we're dealing with.
	if (numatts > 0) {
		sprintf(querystring,"SELECT COUNT(DISTINCT %s) FROM %s WHERE ",
			userkey,usertable);
		// Add in attribute information.
		for (i = 0; i < numatts; i++) {
			char addition[512];
			sprintf(addition,"%s = '%s'",attnames[i],attvalues[i]);
			strncat(querystring,addition,strlen(addition));
			if (i+1 < numatts)
				strncat(querystring," AND ",5);
		}
		strncat(querystring,";",1);

		queryDesc = recathon_queryStart(querystring, &recathoncontext);
		planstate = queryDesc->planstate;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot))
			numUsers = 0;
		else
			numUsers = getTupleInt(slot,"count");
		recathon_queryEnd(queryDesc, recathoncontext);
	} else
		numUsers = count_rows(usertable);
	userIDs = (int*) palloc(numUsers*sizeof(int));

	sprintf(querystring,"SELECT DISTINCT %s FROM %s",
		userkey,usertable);
	// An optional join.
	if (numatts > 0)
		strncat(querystring," WHERE ",7);
	// Add in attribute information.
	for (i = 0; i < numatts; i++) {
		char addition[512];
		sprintf(addition,"%s = '%s'",attnames[i],attvalues[i]);
		strncat(querystring,addition,strlen(addition));
		if (i+1 < numatts)
			strncat(querystring," AND ",5);
	}

	// Add the final "ORDER BY".
	strncat(querystring," ORDER BY ",10);
	strncat(querystring,userkey,strlen(userkey));
	strncat(querystring,";",1);

	queryDesc = recathon_queryStart(querystring, &recathoncontext);
	planstate = queryDesc->planstate;

	i = 0;
	for (;;) {
		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		userIDs[i] = getTupleInt(slot, userkey);

		i++;
		if (i >= numUsers) break;
	}

	recathon_queryEnd(queryDesc, recathoncontext);

	// Next, the list of items. We get all of the items, regardless
	// of what set of users we have.
	numItems = count_rows(itemtable);
	itemIDs = (int*) palloc(numItems*sizeof(int));

	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT DISTINCT %s FROM %s ORDER BY %s;",
		itemkey, itemtable, itemkey);
	queryDesc = recathon_queryStart(querystring, &recathoncontext);
	planstate = queryDesc->planstate;

	i = 0;
	for (;;) {
		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		itemIDs[i] = getTupleInt(slot, itemkey);

		i++;
		if (i >= numItems) break;
	}

	recathon_queryEnd(queryDesc, recathoncontext);
	pfree(querystring);

	// Now we return the data.
	(*ret_userIDs) = userIDs;
	(*ret_itemIDs) = itemIDs;
	(*ret_numUsers) = numUsers;
	(*ret_numItems) = numItems;
}

/* ----------------------------------------------------------------
 *		SVDaverages
 *
 *		This function generates some rating averages which
 *		are used as starting points for our SVD. This needs
 *		to be done once per cell. Borrowed from Simon Funk.
 * ----------------------------------------------------------------
 */
void
SVDaverages(char *usertable, char *userkey, char *itemtable, char *itemkey, char *ratingtable,
		char *ratingval, int *userIDs, int *itemIDs, int numUsers, int numItems,
		int numatts, char **attnames, char **attvalues,
		float **ret_itemAvgs, float **ret_userOffsets) {
	int i;
	int *userCounts, *itemCounts;
	float *userAvgs, *itemAvgs;
	float *itemSums;
	float *itemSqs; // Squares of sums. Used to calculate variances.
	float *itemVars; // Variances.
	float globalAvg;
	float globalSum = 0.0;
	float globalAvgSum = 0.0;
	float globalSq = 0.0;
	float globalVar;
	// Information for other queries.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// Initialize arrays.
	itemCounts = (int*) palloc(numItems*sizeof(int));
	itemAvgs = (float*) palloc(numItems*sizeof(float));
	itemSums = (float*) palloc(numItems*sizeof(float));
	itemSqs = (float*) palloc(numItems*sizeof(float));
	itemVars = (float*) palloc(numItems*sizeof(float));
	for (i = 0; i < numItems; i++) {
		itemCounts[i] = 0;
		itemSums[i] = 0.0;
		itemSqs[i] = 0.0;
	}

	// We need to issue a query to get rating information.
	querystring = (char*) palloc(256*sizeof(char));
	sprintf(querystring,"SELECT %s,%s FROM %s;",itemkey,ratingval,ratingtable);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		int itemindex;
		int itemnum = 0;
		float rating = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		itemnum = getTupleInt(slot,itemkey);
		rating = getTupleFloat(slot,ratingval);
		itemindex = binarySearch(itemIDs, itemnum, 0, numItems);

		if (itemindex >= 0 && itemindex < numItems) {
			itemCounts[itemindex] += 1;
			itemSums[itemindex] += rating;
			itemSqs[itemindex] += (rating*rating);
		}
	}

	recathon_queryEnd(queryDesc,recathoncontext);

	// We have enough data to calculate individual item variances.
	for (i = 0; i < numItems; i++) {
		float sum, sumsqr;
		int n;

		n = itemCounts[i];
		sum = itemSums[i];
		sumsqr = itemSqs[i];

		if (n <= 0)
			itemVars[i] = 0;
		else
			itemVars[i] = (sumsqr - ((sum*sum)/n))/n;

		// We can also start calculating the global variance in this loop.
		// Some notation abuse.
		globalSum += sum;
		if (n > 0) {
			sum = sum/n;
			globalAvgSum += sum;
			globalSq += (sum*sum);
		}
	}

	// Now we derive the global variance.
	globalVar = (globalSq - ((globalAvgSum*globalAvgSum)/numItems))/numItems;
	globalAvg = globalSum/count_rows(ratingtable);

	// Finally, we can obtain the baseline averages for each item.
	for (i = 0; i < numItems; i++) {
		float k;

		if (globalVar == 0)
			k = 0;
		else
			k = itemVars[i] / globalVar;

		if ((k + itemCounts[i]) > 0)
			itemAvgs[i] = ((globalAvg*k) + itemSums[i]) / (k + itemCounts[i]);
		else
			itemAvgs[i] = 0;
	}

	// With the averages calculated, we can now calculate the average offset
	// for each user. This involves querying the user table again.
	userCounts = (int*) palloc(numUsers*sizeof(int));
	for (i = 0; i < numUsers; i++)
		userCounts[i] = 0;
	userAvgs = (float*) palloc(numUsers*sizeof(float));
	for (i = 0; i < numUsers; i++)
		userAvgs[i] = 0.0;

	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r",userkey,itemkey,ratingval,ratingtable);
	if (numatts > 0) {
		char addition[512];
		sprintf(addition,", %s u WHERE u.%s = r.%s",usertable,userkey,userkey);
		strncat(querystring,addition,strlen(addition));
	}
	// Add in attribute information.
	for (i = 0; i < numatts; i++) {
		char addition[512];
		sprintf(addition," AND %s = '%s'",attnames[i],attvalues[i]);
		strncat(querystring,addition,strlen(addition));
	}

	// Add the final "ORDER BY".
	strncat(querystring," ORDER BY ",10);
	strncat(querystring,userkey,strlen(userkey));
	strncat(querystring,";",1);

	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		int userindex, itemindex;
		int usernum, itemnum;
		float rating;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		usernum = getTupleInt(slot,userkey);
		itemnum = getTupleInt(slot,itemkey);
		rating = getTupleFloat(slot,ratingval);
		userindex = binarySearch(userIDs, usernum, 0, numUsers);
		itemindex = binarySearch(itemIDs, itemnum, 0, numItems);

		// We need to find the average offset of a user's rating from
		// the average rating.
		if (userindex >= 0 && userindex < numUsers) {
			userCounts[userindex] += 1;
			userAvgs[userindex] += rating - itemAvgs[itemindex];
		}
	}

	recathon_queryEnd(queryDesc,recathoncontext);

	// Now we just divide by the counts.
	for (i = 0; i < numUsers; i++) {
		if (userCounts[i] > 0)
			userAvgs[i] /= userCounts[i];
		else
			userAvgs[i] = 0;
	}

	// Free up memory.
	pfree(itemCounts);
	pfree(itemSums);
	pfree(itemSqs);
	pfree(itemVars);
	pfree(userCounts);
	pfree(querystring);

	// With that information calculated, we can finally return.
	(*ret_itemAvgs) = itemAvgs;
	(*ret_userOffsets) = userAvgs;
}

/* ----------------------------------------------------------------
 *		predictRating
 *
 *		This function gives a rating prediction based on
 *		our SVD models. Used for training only.
 * ----------------------------------------------------------------
 */
float
predictRating(int featurenum, int numFeatures, int userid, int itemid,
		float **userFeatures, float **itemFeatures, float residual) {
	int i;
	float rating;

	rating = residual;
	for (i = featurenum; i < numFeatures; i++)
		rating += userFeatures[i][userid] * itemFeatures[i][itemid];

	return rating;
}

/* ----------------------------------------------------------------
 *		SVDsimilarity
 *
 *		This function trains features for SVD models.
 *		Returns the number of ratings used.
 * ----------------------------------------------------------------
 */
int
SVDsimilarity(char *usertable, char *userkey, char *itemtable, char *itemkey, char *ratingtable, char *ratingval,
		char *usermodelname, char *itemmodelname, char **attnames, char **attvalues, int numatts,
		bool update) {
	float **userFeatures, **itemFeatures;
	int *userIDs, *itemIDs;
	float *itemAvgs, *userOffsets;
	int numUsers, numItems;
	int i, j, k, numRatings;
	int numFeatures = 50;
	svd_node *allRatings;
	FILE *fp;
	char *tempfilename, *insertstring;
	// Information for other queries.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// If this is us updating a cell as opposed to building
	// a recommender, we need to drop the existing entries.
	if (update) {
		char *dropstring;

		dropstring = (char*) palloc(256*sizeof(char));
		sprintf(dropstring,"DELETE FROM %s;",usermodelname);
		recathon_queryExecute(dropstring);
		sprintf(dropstring,"DELETE FROM %s;",itemmodelname);
		recathon_queryExecute(dropstring);
		pfree(dropstring);
	}

	// First, we get our lists of users and items.
	SVDlists(usertable,userkey,itemtable,itemkey,numatts,attnames,attvalues,
		&userIDs, &itemIDs, &numUsers, &numItems);

	// Then we get information for baseline averages.
	SVDaverages(usertable,userkey,itemtable,itemkey,ratingtable,ratingval,
		userIDs,itemIDs,numUsers,numItems,numatts,attnames,attvalues,
		&itemAvgs,&userOffsets);

	// Initialize our feature arrays.
	userFeatures = (float**) palloc(numFeatures*sizeof(float*));
	for (i = 0; i < numFeatures; i++) {
		userFeatures[i] = (float*) palloc(numUsers*sizeof(float));
		for (j = 0; j < numUsers; j++)
			userFeatures[i][j] = 0.1;
	}
	itemFeatures = (float**) palloc(numFeatures*sizeof(float*));
	for (i = 0; i < numFeatures; i++) {
		itemFeatures[i] = (float*) palloc(numItems*sizeof(float));
		for (j = 0; j < numItems; j++)
			itemFeatures[i][j] = 0.1;
	}

	// First we need to count the number of ratings we'll be
	// considering.
	querystring = (char*) palloc(1024*sizeof(char));

	if (numatts > 0) {
		sprintf(querystring,"SELECT count(r.%s) FROM %s r, %s u WHERE u.%s=r.%s",
			ratingval,ratingtable,usertable,userkey,userkey);
		// Add in attribute information.
		for (i = 0; i < numatts; i++) {
			char addition[512];
			sprintf(addition," AND u.%s = '%s'",attnames[i],attvalues[i]);
			strncat(querystring,addition,strlen(addition));
		}

		strncat(querystring,";",1);

		queryDesc = recathon_queryStart(querystring,&recathoncontext);
		planstate = queryDesc->planstate;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot))
			numRatings = 0;
		else
			numRatings = getTupleInt(slot,"count");
		recathon_queryEnd(queryDesc,recathoncontext);
	} else
		numRatings = count_rows(ratingtable);

	// Initialize the ratings array.
	allRatings = (svd_node*) palloc(numRatings*sizeof(svd_node));

	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r",
		userkey,itemkey,ratingval,ratingtable);
	// An optional join.
	if (numatts > 0) {
		char addition[512];
		sprintf(addition,", %s u WHERE u.%s=r.%s",usertable,userkey,userkey);
		strncat(querystring,addition,strlen(addition));
	}
	// Add in attribute information.
	for (i = 0; i < numatts; i++) {
		char addition[512];
		sprintf(addition," AND u.%s = '%s'",attnames[i],attvalues[i]);
		strncat(querystring,addition,strlen(addition));
	}

	// Add the final "ORDER BY".
	strncat(querystring," ORDER BY r.",12);
	strncat(querystring,userkey,strlen(userkey));
	strncat(querystring,";",1);
	// Let's acquire all of our ratings and store them. Sorting initially by
	// user ID avoids unnecessary binary searches.
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	i = 0;
	for (;;) {
		svd_node new_svd;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		new_svd = createSVDnode(slot, userkey, itemkey, ratingval, userIDs, itemIDs, numUsers, numItems);

		allRatings[i] = new_svd;

		i++;
		if (i >= numRatings) break;
	}

	recathon_queryEnd(queryDesc,recathoncontext);

	// We now have all of the ratings, so we can start training our features.
	for (j = 0; j < 100; j++) {
		for (i = 0; i < numFeatures; i++) {
			float learn = 0.001;
			float penalty = 0.002;
			float *userVal = userFeatures[i];
			float *itemVal = itemFeatures[i];

			for (k = 0; k < numRatings; k++) {
				int userid;
				int itemid;
				float rating, err, residual, temp;
				svd_node current_svd;

				current_svd = allRatings[k];
				userid = current_svd->userid;
				itemid = current_svd->itemid;
				rating = current_svd->rating;
				// Need to reset residuals for each new
				// iteration of the trainer.
				if (i == 0)
					current_svd->residual = 0;
				residual = current_svd->residual;

				if (i == 0 && j == 0) {
					err = rating - (itemAvgs[itemid] + userOffsets[userid]);
				} else {
					err = rating - predictRating(i, numFeatures, userid, itemid,
							userFeatures, itemFeatures, residual);
				}
				temp = userVal[userid];
				userVal[userid] += learn * ((err * itemVal[itemid]) - (penalty * userVal[userid]));
				itemVal[itemid] += learn * ((err * temp) - (penalty * itemVal[itemid]));

				// Store residuals.
				if (i == 0)
					current_svd->residual = userVal[userid] * itemVal[itemid];
				else
					current_svd->residual += userVal[userid] * itemVal[itemid];
			}

			CHECK_FOR_INTERRUPTS();
		}
	}

	// With the training finished, we need to write out the data to file,
	// so we can put it back. First, the user model.
	tempfilename = (char*) palloc(256*sizeof(char));
	sprintf(tempfilename,"recathon_temp_%s.dat",usermodelname);
	if ((fp = fopen(tempfilename,"w")) == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("failed to open temporary file")));
	insertstring = (char*) palloc(128*sizeof(char));

	for (i = 0; i < numFeatures; i++) {
		for (j = 0; j < numUsers; j++) {
			sprintf(insertstring,"%d;%d;%f\n",userIDs[j],i,userFeatures[i][j]);
			fwrite(insertstring,1,strlen(insertstring),fp);
		}
	}
	fclose(fp);

	// If we are updating an existing SVD model,
	// we will want to drop the existing primary key
	// constraint before doing the copy, to save time.
	if (update) {
		insertstring = (char*) palloc(1024*sizeof(char));
		sprintf(insertstring,"ALTER TABLE %s DROP CONSTRAINT %s_pkey;",
					usermodelname,usermodelname);
		recathon_utilityExecute(insertstring);
		pfree(insertstring);
	}

	// We can bulk load the data with COPY FROM. It's faster
	// than individual inserts by a good margin.
	sprintf(querystring,"COPY %s FROM '%s' DELIMITERS ';';",
		usermodelname,tempfilename);
	recathon_utilityExecute(querystring);

	// Adding a primary key after the COPY FROM is about 25% faster
	// than adding it before.
	sprintf(querystring,"ALTER TABLE %s ADD PRIMARY KEY (users, feature);",usermodelname);
	recathon_utilityExecute(querystring);

	// Delete the temporary file.
	if (unlink(tempfilename) < 0)
		ereport(WARNING,
			(errcode(ERRCODE_WARNING),
			 errmsg("failed to delete temporary file")));

	// Now do it again for the item model.
	tempfilename = (char*) palloc(256*sizeof(char));
	sprintf(tempfilename,"recathon_temp_%s.dat",itemmodelname);
	if ((fp = fopen(tempfilename,"w")) == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("failed to open temporary file")));

	for (i = 0; i < numFeatures; i++) {
		for (j = 0; j < numItems; j++) {
			char insertstring[128];
			sprintf(insertstring,"%d;%d;%f\n",itemIDs[j],i,itemFeatures[i][j]);
			fwrite(insertstring,1,strlen(insertstring),fp);
		}
	}
	fclose(fp);

	// If we are updating an existing SVD model,
	// we will want to drop the existing primary key
	// constraint before doing the copy, to save time.
	if (update) {
		insertstring = (char*) palloc(1024*sizeof(char));
		sprintf(insertstring,"ALTER TABLE %s DROP CONSTRAINT %s_pkey;",
					itemmodelname,itemmodelname);
		recathon_utilityExecute(insertstring);
		pfree(insertstring);
	}

	// We can bulk load the data with COPY FROM. It's faster
	// than individual inserts by a good margin.
	sprintf(querystring,"COPY %s FROM '%s' DELIMITERS ';';",
		itemmodelname,tempfilename);
	recathon_utilityExecute(querystring);

	// Adding a primary key after the COPY FROM is about 25% faster
	// than adding it before.
	sprintf(querystring,"ALTER TABLE %s ADD PRIMARY KEY (items, feature);",itemmodelname);
	recathon_utilityExecute(querystring);

	// Delete the temporary file.
	if (unlink(tempfilename) < 0)
		ereport(WARNING,
			(errcode(ERRCODE_WARNING),
			 errmsg("failed to delete temporary file")));

	// Free up memory.
	pfree(querystring);
	pfree(userIDs);
	pfree(itemIDs);
	pfree(itemAvgs);
	pfree(userOffsets);
	pfree(allRatings);

	for (i = 0; i < numFeatures; i++)
		pfree(userFeatures[i]);
	pfree(userFeatures);
	for (i = 0; i < numFeatures; i++)
		pfree(itemFeatures[i]);
	pfree(itemFeatures);

	// Return the number of ratings we used.
	return numRatings;
}

/* ----------------------------------------------------------------
 *		hashCreate
 *
 *		Creates a new hash table.
 * ----------------------------------------------------------------
 */
GenHash*
hashCreate(int totalItems)
{
	int hash;
	GenHash *newHashTable;

	if (totalItems <= 10) hash = totalItems;
	else hash = totalItems / 3;

	newHashTable = (GenHash*) palloc(sizeof(GenHash));
	newHashTable->hash = hash;
	newHashTable->table = (GenRating**) palloc0(hash*sizeof(GenRating));

	return newHashTable;
}

/* ----------------------------------------------------------------
 *		hashAdd
 *
 *		Adds an item to the hash table. We're creating the
 *		hash table so that all of the items are in
 *		increasing order, but we're inserting all the items
 *		that way anyway, so we don't need to enfoce it.
 * ----------------------------------------------------------------
 */
void
hashAdd(GenHash *table, GenRating *item)
{
	int hashval;
	GenRating *tempRating;

	hashval = item->ID % table->hash;
	tempRating = table->table[hashval];

	if (!tempRating) {
		table->table[hashval] = item;
		return;
	}

	while (tempRating->next)
		tempRating = tempRating->next;

	tempRating->next = item;

	return;
}

/* ----------------------------------------------------------------
 *		hashFind
 *
 *		Locate an item in the hash table. Returns NULL if
 *		not found.
 * ----------------------------------------------------------------
 */
GenRating*
hashFind(GenHash *table, int itemID)
{
	int hashval;
	GenRating *tempRating;

	hashval = itemID % table->hash;
	tempRating = table->table[hashval];

	while (tempRating) {
		if (tempRating->ID == itemID)
			return tempRating;
		if (tempRating->ID > itemID)
			return NULL;
		tempRating = tempRating->next;
	}

	return NULL;
}

/* ----------------------------------------------------------------
 *		itemCFpredict
 *
 *		Generates a predicted RecScore for a given user and
 *		item, for a recommender that uses item-based
 *		collaborative filtering.
 * ----------------------------------------------------------------
 */
float
itemCFpredict(RecScanState *recnode, char *itemmodel, int itemid)
{
	float recScore;
	GenRating *currentItem;
	// Query objects.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// First, we grab the GenRating for this item ID.
	currentItem = hashFind(recnode->pendingTable, itemid);
	// In case there's some error.
	if (!currentItem)
		return -1;

	querystring = (char*) palloc(1024*sizeof(char));

	// We're going to look through the similarity matrix for the
	// numbers that correspond to this item, and find which of those
	// also correspond to items this user rated. We will use that
	// information to obtain the estimated rating.
	sprintf(querystring,"select * from %s where item1 = %d;",
		itemmodel,currentItem->ID);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		int itemID;
		float similarity;
		GenRating *ratedItem;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		itemID = getTupleInt(slot,"item2");
		similarity = getTupleFloat(slot,"similarity");

		// Find the array slot this item ID corresponds to.
		// If -1 is returned, then the item ID corresponds to
		// another item we haven't rated, so we don't care.
		ratedItem = hashFind(recnode->ratedTable,itemID);
		if (ratedItem) {
			currentItem->score += similarity*ratedItem->score;
			if (similarity < 0)
				similarity *= -1;
			currentItem->totalSim += similarity;
		}
	}

	// Cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	if (currentItem->totalSim == 0) return 0;

	recScore = currentItem->score / currentItem->totalSim;
	return recScore;
}

/* ----------------------------------------------------------------
 *		userCFpredict
 *
 *		Generates a predicted RecScore for a given user and
 *		item, for a recommender that uses user-based
 *		collaborative filtering.
 * ----------------------------------------------------------------
 */
float
userCFpredict(RecScanState *recnode, char *ratingval, int itemid)
{
	float rating, totalSim, average;
	AttributeInfo *attributes;
	// Query objects;
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *qslot;
	MemoryContext recathoncontext;

	attributes = (AttributeInfo*) recnode->attributes;

	rating = 0.0;
	totalSim = 0.0;
	average = recnode->average;

	/* We need to query the ratings table, so that we can
	 * find all ratings for this item and match them up
	 * with what we have in the similarity matrix. We note
	 * that it's necessarily true that the user has not
	 * rated these items. */
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"select * from %s where %s = %d;",
		attributes->ratingtable,attributes->itemkey,itemid);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		int currentUserID;
		float currentRating, similarity;
		GenRating *currentUser;

		qslot = ExecProcNode(planstate);
		if (TupIsNull(qslot)) break;

		currentUserID = getTupleInt(qslot,attributes->userkey);
		currentRating = getTupleFloat(qslot,ratingval);

		currentUser = hashFind(recnode->simTable,currentUserID);
		if (!currentUser) continue;
		similarity = currentUser->totalSim;

		rating += (currentRating - average) * similarity;
		// Poor man's absolute value of the similarity.
		if (similarity < 0)
			similarity *= -1;
		totalSim += similarity;
	}
	recathon_queryEnd(queryDesc,recathoncontext);

	if (totalSim == 0.0) return 0.0;

	rating /= totalSim;
	rating += average;

	return rating;
}

/* ----------------------------------------------------------------
 *		SVDpredict
 *
 *		Generates a predicted RecScore for a given user and
 *		item, for a recommender that uses SVD.
 * ----------------------------------------------------------------
 */
float
SVDpredict(RecScanState *recnode, char *itemmodel, int itemid)
{
	float *userFeatures;
	float recscore = 0.0;
	// Query objects;
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *qslot;
	MemoryContext recathoncontext;

	userFeatures = recnode->userFeatures;

	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"select * from %s where items = %d;",
		itemmodel,itemid);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	// Here we don't use the simpler methods, because they're slightly
	// less efficient. Since we'll be doing this several thousand times,
	// we'll take what we can get.
	for (;;) {
		int i, natts;
		int feature = -1;
		float featValue = 0;

		qslot = ExecProcNode(planstate);
		if (TupIsNull(qslot)) break;
		slot_getallattrs(qslot);
		natts = qslot->tts_tupleDescriptor->natts;

		for (i = 0; i < natts; i++) {
			if (!qslot->tts_isnull[i]) {
				char *col_name;
				Datum slot_result;
				// What we do depends on the column name.
				col_name = qslot->tts_tupleDescriptor->attrs[i]->attname.data;
				slot_result = qslot->tts_values[i];

				if (strcmp(col_name,"feature") == 0)
					feature = DatumGetInt32(slot_result);
				else if (strcmp(col_name,"value") == 0)
					featValue = DatumGetFloat4(slot_result);
			}
		}

		// If there's an error and we didn't find the column.
		if (feature < 0) continue;

		// Add it into the rating and continue.
		recscore += featValue * userFeatures[feature];
	}

	// Cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	return recscore;
}

/* ----------------------------------------------------------------
 *		applyRecScore
 *
 *		This function will calculate a predicted RecScore
 *		and apply it to a given tuple.
 * ----------------------------------------------------------------
 */
void
applyRecScore(RecScanState *recnode, TupleTableSlot *slot, int itemid)
{
	int i, natts;
	float recscore;
	AttributeInfo *attributes;

	attributes = (AttributeInfo*) recnode->attributes;
	natts = slot->tts_tupleDescriptor->natts;

	switch ((recMethod)attributes->method) {
		case itemCosCF:
		case itemPearCF:
			recscore = itemCFpredict(recnode,attributes->recModelName,itemid);
			break;
		case userCosCF:
		case userPearCF:
			recscore = userCFpredict(recnode,attributes->ratingval,itemid);
			break;
		case SVD:
			recscore = SVDpredict(recnode,attributes->recModelName2,itemid);
			break;
		default:
			recscore = -1;
			break;
	}

	/* Plug in the data, marking those columns full. */
	for (i = 0; i < natts; i++) {
		char* col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
		if (strcmp(col_name,"recscore") == 0) {
			slot->tts_values[i] = Float4GetDatum(recscore);
			slot->tts_isnull[i] = false;
			slot->tts_nvalid++;
		}
	}
}

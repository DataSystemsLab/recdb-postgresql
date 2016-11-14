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

/* ----------------------------------------------------------------
 *		createSimNode
 *
 *		Creates a sim_node, used for recommenders.
 * ----------------------------------------------------------------
 */
sim_node
createSimNode(int userid, float event) {
	sim_node newnode;

	newnode = (sim_node) palloc(sizeof(struct sim_node_t));
	newnode->id = userid;
	newnode->event = event;
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
 *		retrieveRecommender
 *
 *		Given an event table and a recommendation method,
 *		we look to see if any recommenders are already
 *		built. If so, we return the RecIndex name.
 * ----------------------------------------------------------------
 */
char*
retrieveRecommender(char *eventtable, char *method) {
	RangeVar *cataloguerv;
	char *querystring, *recindexname;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// If this fails, there's no RecModelsCatalogue, so
	// there are no recommenders.
	cataloguerv = makeRangeVar(NULL,"recmodelscatalogue",0);
	if (!relationExists(cataloguerv)) {
		pfree(cataloguerv);
		return NULL;
	}
	pfree(cataloguerv);

	// If the catalogue does exist, we'll query it looking
	// for recommenders based on the given information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT recommenderindexname FROM RecModelsCatalogue WHERE eventtable = '%s' AND method = '%s';",
		eventtable, method);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	// If there are no results, the recommender does not exist.
	if (TupIsNull(slot)) {
		recathon_queryEnd(queryDesc,recathoncontext);
		pfree(querystring);
		return NULL;
	}

	recindexname = getTupleString(slot,"recommenderindexname");

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	return recindexname;
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
getRecInfo(char *recindexname, char **ret_eventtable,
		char **ret_userkey, char **ret_itemkey,
		char **ret_eventval, char **ret_method, int *ret_numatts) {
	char *eventtable, *userkey, *itemkey, *eventval, *method;
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
	if (ret_eventtable) {
		eventtable = getTupleString(slot,"eventtable");
		(*ret_eventtable) = eventtable;
	}
	if (ret_userkey) {
		userkey = getTupleString(slot,"userkey");
		(*ret_userkey) = userkey;
	}
	if (ret_itemkey) {
		itemkey = getTupleString(slot,"itemkey");
		(*ret_itemkey) = itemkey;
	}
	if (ret_eventval) {
		eventval = getTupleString(slot,"eventval");
		(*ret_eventval) = eventval;
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
 *		validateCreateRStmt
 *
 *		We need to implement a series of sanity checks, to
 *		make sure CREATE RECOMMENDER statement data is
 *		actually usable.
 * ----------------------------------------------------------------
 */
recMethod
validateCreateRStmt(CreateRStmt *recStmt) {
	recMethod method;

	// Our first test is to make sure the ratings table exists.
	if (!relationExists(recStmt->eventtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
			 errmsg("relation \"%s\" does not exist",
				recStmt->eventtable->relname)));

	// Our second test is to see whether or not a recommender has already
	// been created with the given events table and method, or name.
	if (relationExists(recStmt->recname))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
			 errmsg("a recommender with name \"%s\" already exists",
				recStmt->recname->relname)));

	if (retrieveRecommender(recStmt->eventtable->relname,recStmt->method) != NULL)
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
			 errmsg("recommender on table \"%s\" using method \"%s\" already exists",
				recStmt->eventtable->relname,recStmt->method)));

	// We next need to test that the provided columns
	// exist in the events table.
	// Test: user key is in event table.
	if (!columnExistsInRelation(recStmt->userkey,recStmt->eventtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->userkey,recStmt->eventtable->relname)));
	// Test: item key is in event table.
	if (!columnExistsInRelation(recStmt->itemkey,recStmt->eventtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->itemkey,recStmt->eventtable->relname)));
	// Test: event value is in event table.
	if (!columnExistsInRelation(recStmt->eventval,recStmt->eventtable))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("column \"%s\" does not exist in relation \"%s\"",
				recStmt->eventval,recStmt->eventtable->relname)));

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

	// And return.
	return method;
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

	testrv = makeRangeVar(NULL,"recdbproperties",0);
	if (!relationExists(testrv)) {
		pfree(testrv);
		return -1;
	}
	pfree(testrv);

	querystring = (char*) palloc(128*sizeof(char));
	sprintf(querystring,"SELECT update_threshold FROM recdbproperties;");
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
 *		updateCellCounter
 *
 *		Happens whenever an INSERT occurs. If the insert
 *		is for an events table that we've built a
 *		recommender on, we need to update the counters of
 *		the appropriate cells.
 * ----------------------------------------------------------------
 */
void
updateCellCounter(char *eventtable, TupleTableSlot *insertslot) {
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
	sprintf(querystring,"SELECT * FROM RecModelsCatalogue WHERE eventtable = '%s';",
		eventtable);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		// In case of SVD, recmodelname is the user model, and the other is the
		// item model. Otherwise, recmodelname2 is nothing.
		char *recindexname, *recmodelname, *recmodelname2;
		char *userkey, *itemkey, *eventval, *strmethod;
		int updatecounter = -1;
		int eventtotal = -1;
		recMethod method;
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
		userkey = getTupleString(slot,"userkey");
		itemkey = getTupleString(slot,"itemkey");
		eventval = getTupleString(slot,"eventval");
		strmethod = getTupleString(slot,"method");

		// Get the recMethod.
		method = getRecMethod(strmethod);
		pfree(strmethod);

		// Failure case, continue to next tuple.
		if (method < 0) {
			pfree(recindexname);
			pfree(userkey);
			pfree(itemkey);
			pfree(eventval);
			continue;
		}

		// We now have all the information necessary to update this
		// recommender's cell counter. First we need to acquire it,
		// and we might as well get the model name while we're at it.
		countquerystring = (char*) palloc(1024*sizeof(char));
		if (method == SVD)
			sprintf(countquerystring,"SELECT recusermodelname, recitemmodelname, updatecounter, eventtotal FROM %s;",
				recindexname);
		else
			sprintf(countquerystring,"SELECT recmodelname, updatecounter, eventtotal FROM %s;",
				recindexname);

		countqueryDesc = recathon_queryStart(countquerystring,&countcontext);
		countplanstate = countqueryDesc->planstate;

		// Go through what should be the only tuple and obtain the data.
		countslot = ExecProcNode(countplanstate);
		if (TupIsNull(countslot)) {
			// More failure conditions. We can't just error out
			// because the INSERT still needs to happen.
			recathon_queryEnd(countqueryDesc,countcontext);
			pfree(countquerystring);
			pfree(recindexname);
			pfree(userkey);
			pfree(itemkey);
			pfree(eventval);
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
		eventtotal = getTupleInt(countslot,"eventtotal");

		recathon_queryEnd(countqueryDesc,countcontext);
		pfree(countquerystring);

		// Even more failure conditions.
		if (updatecounter < 0) {
			pfree(recmodelname);
			if (recmodelname2)
				pfree(recmodelname2);
			pfree(recindexname);
			pfree(userkey);
			pfree(itemkey);
			continue;
		}

		// With that done, we check the original counter. If the
		// number of new events is greater than threshold * the
		// number of events currently used in the model, we need
		// to trigger an update. Otherwise, just increment.
		updatecounter++;

		if (updatecounter >= (int) (update_threshold * eventtotal)) {
			int numEvents = 0;

			// What we do depends on the recommendation method.
			switch (method) {
				case itemCosCF:
					{
					// Before we update the similarity model, we need to obtain
					// a few item-related things.
					int numItems;
					int *IDs;
					float *lengths;

					lengths = vector_lengths(itemkey, eventtable, eventval,
						&numItems, &IDs);

					// Now update the similarity model.
					numEvents = updateItemCosModel(eventtable, userkey,
						itemkey, eventval, recmodelname,
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

					pearson_info(itemkey, eventtable, eventval, &numItems,
							&IDs, &avgs, &pearsons);

					// Now update the similarity model.
					numEvents = updateItemPearModel(eventtable, userkey,
						itemkey, eventval, recmodelname,
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

					lengths = vector_lengths(userkey, eventtable, eventval,
						&numUsers, &IDs);

					// Now update the similarity model.
					numEvents = updateUserCosModel(eventtable, userkey,
						itemkey, eventval, recmodelname,
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

					pearson_info(userkey, eventtable, eventval, &numUsers,
							&IDs, &avgs, &pearsons);

					// Now update the similarity model.
					numEvents = updateUserPearModel(eventtable, userkey,
						itemkey, eventval, recmodelname,
						IDs, avgs, pearsons, numUsers, true);
					}
					break;
				case SVD:
					// No additional functions, just update the model.
					numEvents = SVDtrain(userkey, itemkey,
						eventtable, eventval,
						recmodelname, recmodelname2, true);
					break;
				default:
					break;
			}

			// Finally, we update the cell to indicate how many events were used
			// to build it. We'll also reset the updatecounter.
			countquerystring = (char*) palloc(1024*sizeof(char));
			sprintf(countquerystring,"UPDATE %s SET updatecounter = 0, eventtotal = %d;",
							recindexname,numEvents);

			// Execute normally, we don't need to see results.
			recathon_queryExecute(countquerystring);
			pfree(countquerystring);
		} else {
			// Just increment.
			countquerystring = (char*) palloc(1024*sizeof(char));
			sprintf(countquerystring,"UPDATE %s SET updatecounter = updatecounter+1;",
							recindexname);
			// Execute normally, we don't need to see results.
			recathon_queryExecute(countquerystring);
			pfree(countquerystring);
		}

		// Final cleanup.
		pfree(recmodelname);
		if (recmodelname2)
			pfree(recmodelname2);
		pfree(recindexname);
		pfree(userkey);
		pfree(itemkey);
		pfree(eventval);
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
vector_lengths(char *key, char *eventtable, char *eventval, int *totalNum, int **IDlist) {
	int *IDs;
	float *lengths;
	int i, j, numItems, priorID;
	// Objects for querying.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// We start by getting the number of distinct items in the event table.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT COUNT(DISTINCT %s) FROM %s;",
		key,eventtable);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	slot = ExecProcNode(planstate);
	numItems = getTupleInt(slot,"count");
	recathon_queryEnd(queryDesc,recathoncontext);

	// Now that we have the number of items, we can create an array or two.
	IDs = (int*) palloc(numItems*sizeof(int));
	lengths = (float*) palloc(numItems*sizeof(float));
	for (j = 0; j < numItems; j++)
		lengths[j] = 0.0;

	// Now we need to populate the two arrays. We'll get all the events from
	// the events table.
	priorID = -1;
	sprintf(querystring,"SELECT * FROM %s ORDER BY %s;",eventtable,key);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	i = -1;

	// This query grabs all item IDs, so we can store them. Later we'll calculate
	// vector lengths.
	for (;;) {
		int currentID = 0;
		float currentEvent = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		currentID = getTupleInt(slot,key);

		// Are we dealing with a new item ID? If so, switch to the next slot.
		if (currentID != priorID) {
			i++;
			priorID = currentID;
			IDs[i] = currentID;
		}

		currentEvent = getTupleFloat(slot,eventval);
		lengths[i] += currentEvent*currentEvent;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// Now that we've totally queried the events table, we need to
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

	// Check every event for the first item, and see how
	// many of those users also rated the second item.
	temp1 = item1; temp2 = item2;
	while (temp1 && temp2) {
		if (temp1->id == temp2->id) {
			similarity += temp1->event * temp2->event;
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

	// Short-circuit check. If one of the items has no events,
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
 *		similarity. Returns the number of events used.
 * ----------------------------------------------------------------
 */
int
updateItemCosModel(char *eventtable, char *userkey, char *itemkey,
		char *eventval, char *modelname, int *itemIDs, float *itemLengths,
		int numItems, bool update) {
	int i, j, priorID;
	int numEvents = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *itemEvents;
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
	itemEvents = (sim_node*) palloc(numItems*sizeof(sim_node));
	for (i = 0; i < numItems; i++)
		itemEvents[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the events table
	// in order to get the key information. We'll also keep track of the number
	// of events used, since we need to store that information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,itemkey);

	// Begin extracting data.
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		// Are we dealing with a new item ID? If so, switch to the next slot.
		if (simitem != priorID) {
			priorID = simitem;
			i++;
		}

		// We now have the user, item, and event for this tuple.
		// We insert the results as a sim_node into the
		// itemEvents table; we'll do calculations later.
		newnode = createSimNode(simuser, simevent);
		itemEvents[i] = simInsert(itemEvents[i], newnode);
		numEvents++;
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

		item_i = itemEvents[i];
		if (!item_i) continue;
		length_i = itemLengths[i];

		for (j = i+1; j < numItems; j++) {
			float length_j;
			sim_node item_j;
			int item1, item2;
			float similarity;

			item_j = itemEvents[j];
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
		freeSimList(itemEvents[i]);
		itemEvents[i] = NULL;
	}

	// Return the number of events we used.
	return numEvents;
}

/* ----------------------------------------------------------------
 *		pearson_info
 *
 *		Looks at all the items in our users/items table and
 *		calculates their average events, as well as
 *		another data item useful for Pearson correlation,
 *		which I'm just calling a Pearson because I'm not
 *		sure it has a name. It can be pre-calculated, so we
 *		will. Also returns a list of user/item IDs.
 * ----------------------------------------------------------------
 */
void
pearson_info(char *key, char *eventtable, char *eventval, int *totalNum,
		int **IDlist, float **avgList, float **pearsonList) {
	int *IDs, *counts;
	float *avgs, *pearsons;
	int i, j, numItems, priorID;
	// Objects for querying.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// We start by getting the number of items in the event table.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT COUNT(DISTINCT %s) FROM %s;",
		key,eventtable);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	slot = ExecProcNode(planstate);
	numItems = getTupleInt(slot,"count");
	recathon_queryEnd(queryDesc,recathoncontext);

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

	// Now we need to populate the four arrays. We'll get all the events from
	// the events table.
	priorID = -1;
	sprintf(querystring,"SELECT * FROM %s ORDER BY %s;",eventtable,key);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	i = -1;

	// This query grabs all item IDs, so we can store them. It also fills in
	// some other information we'll need.
	for (;;) {
		int currentID = 0;
		float currentEvent = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		currentID = getTupleInt(slot,key);

		// Are we dealing with a new item ID? If so, switch to the next slot.
		if (currentID != priorID) {
			i++;
			priorID = currentID;
			IDs[i] = currentID;
		}

		currentEvent = getTupleFloat(slot,eventval);
		counts[i] += 1;
		avgs[i] += currentEvent;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);

	// Now that we've totally queried the events table, we need to
	// obtain the actual averages for each item.
	for (i = 0; i < numItems; i++) {
		if (counts[i] > 0)
			avgs[i] /= ((float)counts[i]);
	}
	pfree(counts);

	// We can reuse the same query to obtain the events again, and
	// calculate Pearsons.
	priorID = -1;
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	i = -1;

	// We scan through the entire event table once, sorting the events
	// based on which item they apply to.
	for (;;) {
		int currentID = 0;
		float currentEvent = 0.0;
		float difference = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		currentID = getTupleInt(slot,key);
		// Are we dealing with a new item ID? If so, switch to the next slot.
		if (currentID != priorID) {
			priorID = currentID;
			i++;
		}
		currentEvent = getTupleFloat(slot,eventval);

		// We have the item number and event value from this tuple.
		// Now we need to update Pearsons.
		difference = currentEvent - avgs[i];
		pearsons[i] += difference*difference;
	}

	// Query cleanup.
	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// Now that we've totally queried the events table, we need to
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

	// Check every event for the first item, and see how
	// many of those users also rated the second item.
	temp1 = item1; temp2 = item2;
	while (temp1 && temp2) {
		if (temp1->id == temp2->id) {
			similarity += (temp1->event - avg1) * (temp2->event - avg2);
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

	// Short-circuit check. If one of the items has no events,
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
 *		similarity. Returns the number of events used.
 * ----------------------------------------------------------------
 */
int
updateItemPearModel(char *eventtable, char *userkey, char *itemkey,
		char *eventval, char *modelname, int *itemIDs, float *itemAvgs,
		float *itemPearsons, int numItems, bool update) {
	int i, j, priorID;
	int numEvents = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *itemEvents;
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
	itemEvents = (sim_node*) palloc(numItems*sizeof(sim_node));
	for (i = 0; i < numItems; i++)
		itemEvents[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the events table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,itemkey);

	// Begin extracting data.
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		// Are we dealing with a new item ID? If so, switch to the next slot.
		if (simitem != priorID) {
			priorID = simitem;
			i++;
		}

		// We now have the user, item, and event for this tuple.
		// We insert the results as a sim_node into the
		// itemEvents table; we'll do calculations later.
		newnode = createSimNode(simuser, simevent);
		itemEvents[i] = simInsert(itemEvents[i], newnode);
		numEvents++;
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

		item_i = itemEvents[i];
		if (!item_i) continue;
		avg_i = itemAvgs[i];
		pearson_i = itemPearsons[i];

		for (j = i+1; j < numItems; j++) {
			float avg_j, pearson_j;
			sim_node item_j;
			int item1, item2;
			float similarity;

			item_j = itemEvents[j];
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
		freeSimList(itemEvents[i]);
		itemEvents[i] = NULL;
	}

	// Return the number of events we used.
	return numEvents;
}

/* ----------------------------------------------------------------
 *		updateUserCosModel
 *
 *		Given a single cell of a recommender, this
 *		function rebuilds the recModel for that cell, using
 *		user-based collaborative filtering with cosine
 *		similarity. Returns the number of events used.
 * ----------------------------------------------------------------
 */
int
updateUserCosModel(char *eventtable, char *userkey, char *itemkey,
		char *eventval, char *modelname, int *userIDs, float *userLengths,
		int numUsers, bool update) {
	int i, j, priorID;
	int numEvents = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *userEvents;
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
	userEvents = (sim_node*) palloc(numUsers*sizeof(sim_node));
	for (i = 0; i < numUsers; i++)
		userEvents[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all user pairs. We need to query the events table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,userkey);

	// Begin extracting data.
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		// Are we dealing with a new user ID? If so, switch to the next slot.
		if (simuser != priorID) {
			priorID = simuser;
			i++;
		}

		// We now have the user, item, and event for this tuple.
		// We insert the results as a sim_node into the
		// userEvents table; we'll do calculations later.
		newnode = createSimNode(simitem, simevent);
		userEvents[i] = simInsert(userEvents[i], newnode);
		numEvents++;
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

		user_i = userEvents[i];
		if (!user_i) continue;
		length_i = userLengths[i];

		for (j = i+1; j < numUsers; j++) {
			float length_j;
			sim_node user_j;
			int user1, user2;
			float similarity;

			user_j = userEvents[j];
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
		freeSimList(userEvents[i]);
		userEvents[i] = NULL;
	}

	// Return the number of events we used.
	return numEvents;
}

/* ----------------------------------------------------------------
 *		updateUserPearModel
 *
 *		Given a single cell of a recommender, this
 *		function rebuilds the recModel for that cell, using
 *		user-based collaborative filtering with Pearson
 *		similarity. Returns the number of events used.
 * ----------------------------------------------------------------
 */
int
updateUserPearModel(char *eventtable, char *userkey, char *itemkey,
		char *eventval, char *modelname, int *userIDs, float *userAvgs,
		float *userPearsons, int numUsers, bool update) {
	int i, j, priorID;
	int numEvents = 0;
	char *querystring, *insertstring, *temprecfile;
	sim_node *userEvents;
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
	userEvents = (sim_node*) palloc(numUsers*sizeof(sim_node));
	for (i = 0; i < numUsers; i++)
		userEvents[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the events table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,userkey);

	// Begin extracting data.
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		// Are we dealing with a new user ID? If so, switch to the next slot.
		if (simuser != priorID) {
			priorID = simuser;
			i++;
		}

		// We now have the user, item, and event for this tuple.
		// We insert the results as a sim_node into the
		// userEvents table; we'll do calculations later.
		newnode = createSimNode(simitem, simevent);
		userEvents[i] = simInsert(userEvents[i], newnode);
		numEvents++;
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

		user_i = userEvents[i];
		if (!user_i) continue;
		avg_i = userAvgs[i];
		pearson_i = userPearsons[i];

		for (j = i+1; j < numUsers; j++) {
			float avg_j, pearson_j;
			sim_node user_j;
			int user1, user2;
			float similarity;

			user_j = userEvents[j];
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
		freeSimList(userEvents[i]);
		userEvents[i] = NULL;
	}

	// Return the number of events we used.
	return numEvents;
}

/* ----------------------------------------------------------------
 *		createSVDnode
 *
 *		This function creates a new SVD node out of a
 *		TupleTableSlot.
 * ----------------------------------------------------------------
 */
svd_node createSVDnode(TupleTableSlot *slot, char *userkey, char *itemkey, char *eventval,
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
	new_svd->event = -1;
	new_svd->residual = 0.0;

	userid = getTupleInt(slot,userkey);
	itemid = getTupleInt(slot,itemkey);
	new_svd->event = getTupleFloat(slot,eventval);

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
SVDlists(char *userkey, char *itemkey, char *eventtable,
		int **ret_userIDs, int **ret_itemIDs,
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
	sprintf(querystring,"SELECT COUNT(DISTINCT %s) FROM %s;",
		userkey,eventtable);

	queryDesc = recathon_queryStart(querystring, &recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	if (TupIsNull(slot))
		numUsers = 0;
	else
		numUsers = getTupleInt(slot,"count");
	recathon_queryEnd(queryDesc, recathoncontext);
	userIDs = (int*) palloc(numUsers*sizeof(int));

	sprintf(querystring,"SELECT DISTINCT %s FROM %s ORDER BY %s;",
		userkey,eventtable,userkey);

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

	// Next, the list of items.
	sprintf(querystring,"SELECT COUNT(DISTINCT %s) FROM %s;",
		itemkey,eventtable);

	queryDesc = recathon_queryStart(querystring, &recathoncontext);
	planstate = queryDesc->planstate;

	slot = ExecProcNode(planstate);
	if (TupIsNull(slot))
		numItems = 0;
	else
		numItems = getTupleInt(slot,"count");
	recathon_queryEnd(queryDesc, recathoncontext);
	itemIDs = (int*) palloc(numItems*sizeof(int));

	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT DISTINCT %s FROM %s ORDER BY %s;",
		itemkey, eventtable, itemkey);
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
 *		This function generates some event averages which
 *		are used as starting points for our SVD. This needs
 *		to be done once per cell. Borrowed from Simon Funk.
 * ----------------------------------------------------------------
 */
void
SVDaverages(char *userkey, char *itemkey, char *eventtable, char *eventval,
		int *userIDs, int *itemIDs, int numUsers, int numItems,
		float **ret_itemAvgs, float **ret_userOffsets) {
	int i, priorID;
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

	// We need to issue a query to get event information.
	querystring = (char*) palloc(256*sizeof(char));
	sprintf(querystring,"SELECT %s,%s FROM %s ORDER BY %s;",
			itemkey,eventval,eventtable,itemkey);

	priorID = -1;
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	i = -1;

	for (;;) {
		int itemnum = 0;
		float event = 0.0;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		itemnum = getTupleInt(slot,itemkey);
		event = getTupleFloat(slot,eventval);

		// Are we dealing with a new item ID? If so, switch to the next slot.
		if (itemnum != priorID) {
			priorID = itemnum;
			i++;
		}

		itemCounts[i] += 1;
		itemSums[i] += event;
		itemSqs[i] += (event*event);
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
	globalAvg = globalSum/count_rows(eventtable);

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

	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r;",
		userkey,itemkey,eventval,eventtable);

	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		int userindex, itemindex;
		int usernum, itemnum;
		float event;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		usernum = getTupleInt(slot,userkey);
		itemnum = getTupleInt(slot,itemkey);
		event = getTupleFloat(slot,eventval);
		userindex = binarySearch(userIDs, usernum, 0, numUsers);
		itemindex = binarySearch(itemIDs, itemnum, 0, numItems);

		// We need to find the average offset of a user's event from
		// the average event.
		if (userindex >= 0 && userindex < numUsers) {
			userCounts[userindex] += 1;
			userAvgs[userindex] += event - itemAvgs[itemindex];
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
 *		SVDtrain
 *
 *		This function trains features for SVD models.
 *		Returns the number of events used.
 * ----------------------------------------------------------------
 */
int
SVDtrain(char *userkey, char *itemkey, char *eventtable, char *eventval,
		char *usermodelname, char *itemmodelname, bool update) {
	float **userFeatures, **itemFeatures;
	int *userIDs, *itemIDs;
	float *itemAvgs, *userOffsets;
	int numUsers, numItems;
	int i, j, k, numEvents;
	int numFeatures = 50;
	svd_node *allEvents;
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
	SVDlists(userkey,itemkey,eventtable,
		&userIDs, &itemIDs, &numUsers, &numItems);

	// Then we get information for baseline averages.
	SVDaverages(userkey,itemkey,eventtable,eventval,
		userIDs,itemIDs,numUsers,numItems,
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

	// First we need to count the number of events we'll be
	// considering.
	querystring = (char*) palloc(1024*sizeof(char));
	numEvents = count_rows(eventtable);

	// Initialize the events array.
	allEvents = (svd_node*) palloc(numEvents*sizeof(svd_node));

	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,userkey);

	// Let's acquire all of our events and store them. Sorting initially by
	// user ID avoids unnecessary binary searches.
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	i = 0;
	for (;;) {
		svd_node new_svd;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		new_svd = createSVDnode(slot, userkey, itemkey, eventval, userIDs, itemIDs, numUsers, numItems);

		allEvents[i] = new_svd;

		i++;
		if (i >= numEvents) break;
	}

	recathon_queryEnd(queryDesc,recathoncontext);

	// We now have all of the events, so we can start training our features.
	for (j = 0; j < 100; j++) {
		for (i = 0; i < numFeatures; i++) {
			float learn = 0.001;
			float penalty = 0.002;
			float *userVal = userFeatures[i];
			float *itemVal = itemFeatures[i];

			for (k = 0; k < numEvents; k++) {
				int userid;
				int itemid;
				float event, err, residual, temp;
				svd_node current_svd;

				current_svd = allEvents[k];
				userid = current_svd->userid;
				itemid = current_svd->itemid;
				event = current_svd->event;
				// Need to reset residuals for each new
				// iteration of the trainer.
				if (i == 0)
					current_svd->residual = 0;
				residual = current_svd->residual;

				if (i == 0 && j == 0) {
					err = event - (itemAvgs[itemid] + userOffsets[userid]);
				} else {
					err = event - predictRating(i, numFeatures, userid, itemid,
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
	pfree(allEvents);

	for (i = 0; i < numFeatures; i++)
		pfree(userFeatures[i]);
	pfree(userFeatures);
	for (i = 0; i < numFeatures; i++)
		pfree(itemFeatures[i]);
	pfree(itemFeatures);

	// Return the number of events we used.
	return numEvents;
}

/* ----------------------------------------------------------------
 *		generateItemCosModel
 *
 *		Create an item-based cosine model on the fly.
 * ----------------------------------------------------------------
 */
void
generateItemCosModel(RecScanState *recnode) {
	int i, j, priorID;
	AttributeInfo *attributes;
	float **itemmodel;
	char *eventtable, *userkey, *itemkey, *eventval;
	int numItems;
	int *itemIDs;
	float *itemLengths;
	sim_node *itemEvents;
	// Information for other queries.
	char *querystring;
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;

	attributes = (AttributeInfo*) recnode->attributes;
	eventtable = attributes->eventtable;
	userkey = attributes->userkey;
	itemkey = attributes->itemkey;
	eventval = attributes->eventval;

	/* We start by getting vector lengths. */
	itemLengths = vector_lengths(itemkey,eventtable,eventval,&numItems,&itemIDs);

	/* We have the number of items, so we can initialize our model. */
	itemmodel = (float**) palloc(numItems*sizeof(float*));
	for (i = 0; i < numItems; i++)
		itemmodel[i] = (float*) palloc0(numItems*sizeof(float));

	/* Then we can calculate similarity values for our model. We start by
	 * storing all the ratings. */
	itemEvents = (sim_node*) palloc(numItems*sizeof(sim_node));
	for (i = 0; i < numItems; i++)
		itemEvents[i] = NULL;

	/* With the model created, we need to populate it, which means calculating
	 * similarity between all item pairs. We need to query the events table
	 * in order to get the key information. We'll also keep track of the number
	 * of events used, since we need to store that information. */
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,itemkey);

	/* Begin extracting data. */
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		/* Shut the compiler up. */
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		/* Are we dealing with a new item ID? If so, switch to the next slot. */
		if (simitem != priorID) {
			priorID = simitem;
			i++;
		}

		/* We now have the user, item, and event for this tuple.
		 * We insert the results as a sim_node into the
		 * itemEvents table; we'll do calculations later. */
		newnode = createSimNode(simuser, simevent);
		itemEvents[i] = simInsert(itemEvents[i], newnode);
	}

	/* Query cleanup. */
	recathon_queryEnd(simqueryDesc, simcontext);

	/* Now we do the similarity calculations. Note that we
	 * don't include duplicate entries, to save time and space.
	 * The first item ALWAYS has a lower value than the second. */
	for (i = 0; i < numItems; i++) {
		float length_i;
		sim_node item_i;

		item_i = itemEvents[i];
		if (!item_i) continue;
		length_i = itemLengths[i];

		for (j = i+1; j < numItems; j++) {
			float length_j;
			sim_node item_j;
			float similarity;

			item_j = itemEvents[j];
			if (!item_j) continue;
			length_j = itemLengths[j];

			similarity = cosineSimilarity(item_i, item_j, length_i, length_j);
			if (similarity <= 0) continue;

			/* Now we output. Like with the pre-computed model, we'll
			 * only worry about half the model. This allows us to fill
			 * in the matrix left-to-right, top-to-bottom. */
			itemmodel[i][j] = similarity;
		}

		CHECK_FOR_INTERRUPTS();
	}

	/* Free up the lists of sim_nodes now, since we're done. */
	for (i = 0; i < numItems; i++) {
		freeSimList(itemEvents[i]);
		itemEvents[i] = NULL;
	}

	/* Fill in the appropriate information. */
	recnode->fullTotalItems = numItems;
	recnode->fullItemList = itemIDs;
	recnode->itemCFmodel = itemmodel;
}

/* ----------------------------------------------------------------
 *		generateItemPearModel
 *
 *		Create an item-based Pearson model on the fly.
 * ----------------------------------------------------------------
 */
void
generateItemPearModel(RecScanState *recnode) {
	int i, j, priorID;
	char *querystring;
	char *eventtable, *userkey, *itemkey, *eventval;
	sim_node *itemEvents;
	int numItems;
	int *itemIDs;
	float *itemAvgs;
	float *itemPearsons;
	AttributeInfo *attributes;
	float **itemmodel;
	// Information for other queries.
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;

	attributes = (AttributeInfo*) recnode->attributes;
	eventtable = attributes->eventtable;
	userkey = attributes->userkey;
	itemkey = attributes->itemkey;
	eventval = attributes->eventval;

	// First we need to get relevant Pearson information.
	pearson_info(itemkey, eventtable, eventval, &numItems, &itemIDs, &itemAvgs, &itemPearsons);

	/* We have the number of items, so we can initialize our model. */
	itemmodel = (float**) palloc(numItems*sizeof(float*));
	for (i = 0; i < numItems; i++)
		itemmodel[i] = (float*) palloc0(numItems*sizeof(float));

	// With the precomputation done, we need to derive the actual item
	// similarities. We can do this in a way that's linear in the number
	// of I/Os and also the amount of storage. The complexity is relegated
	// to in-memory calculations, which is the most affordable. We need to
	// use this data structure here.
	itemEvents = (sim_node*) palloc(numItems*sizeof(sim_node));
	for (i = 0; i < numItems; i++)
		itemEvents[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the events table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,itemkey);

	// Begin extracting data.
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		// Are we dealing with a new item ID? If so, switch to the next slot.
		if (simitem != priorID) {
			priorID = simitem;
			i++;
		}

		// We now have the user, item, and event for this tuple.
		// We insert the results as a sim_node into the
		// itemEvents table; we'll do calculations later.
		newnode = createSimNode(simuser, simevent);
		itemEvents[i] = simInsert(itemEvents[i], newnode);
	}

	// Query cleanup.
	recathon_queryEnd(simqueryDesc, simcontext);
	pfree(querystring);

	// Now we do the similarity calculations. Note that we
	// don't include duplicate entries, to save time and space.
	// The first item ALWAYS has a lower value than the second.
	for (i = 0; i < numItems; i++) {
		float avg_i, pearson_i;
		sim_node item_i;

		item_i = itemEvents[i];
		if (!item_i) continue;
		avg_i = itemAvgs[i];
		pearson_i = itemPearsons[i];

		for (j = i+1; j < numItems; j++) {
			float avg_j, pearson_j;
			sim_node item_j;
			float similarity;

			item_j = itemEvents[j];
			if (!item_j) continue;
			avg_j = itemAvgs[j];
			pearson_j = itemPearsons[j];

			similarity = pearsonSimilarity(item_i, item_j, avg_i, avg_j, pearson_i, pearson_j);
			if (similarity == 0.0) continue;

			/* Now we output. Like with the pre-computed model, we'll
			 * only worry about half the model. This allows us to fill
			 * in the matrix left-to-right, top-to-bottom. */
			itemmodel[i][j] = similarity;
		}

		CHECK_FOR_INTERRUPTS();
	}

	// Free up the lists of sim_nodes and we're done.
	for (i = 0; i < numItems; i++) {
		freeSimList(itemEvents[i]);
		itemEvents[i] = NULL;
	}

	// Return the relevant information.
	recnode->fullTotalItems = numItems;
	recnode->fullItemList = itemIDs;
	recnode->itemCFmodel = itemmodel;
}

/* ----------------------------------------------------------------
 *		generateUserCosModel
 *
 *		Create a user-based cosine model on the fly.
 * ----------------------------------------------------------------
 */
void
generateUserCosModel(RecScanState *recnode) {
	int i, j, priorID;
	int numEvents = 0;
	char *querystring;
	sim_node *userEvents;
	char *eventtable, *userkey, *itemkey, *eventval;
	AttributeInfo *attributes;
	float **usermodel;
	int numUsers;
	int *userIDs;
	float *userLengths;
	// Information for other queries.
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;

	attributes = (AttributeInfo*) recnode->attributes;
	eventtable = attributes->eventtable;
	userkey = attributes->userkey;
	itemkey = attributes->itemkey;
	eventval = attributes->eventval;

	// First we need vector lengths.
	userLengths = vector_lengths(userkey, eventtable, eventval, &numUsers, &userIDs);

	/* We have the number of users, so we can initialize our model. */
	usermodel = (float**) palloc(numUsers*sizeof(float*));
	for (i = 0; i < numUsers; i++)
		usermodel[i] = (float*) palloc0(numUsers*sizeof(float));

	// With the precomputation done, we need to derive the actual user
	// similarities. We can do this in a way that's linear in the number
	// of I/Os and also the amount of storage. The complexity is relegated
	// to in-memory calculations, which is the most affordable. We need to
	// use this data structure here.
	userEvents = (sim_node*) palloc(numUsers*sizeof(sim_node));
	for (i = 0; i < numUsers; i++)
		userEvents[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all user pairs. We need to query the events table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,userkey);

	// Begin extracting data.
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		// Are we dealing with a new user ID? If so, switch to the next slot.
		if (simuser != priorID) {
			priorID = simuser;
			i++;
		}

		// We now have the user, item, and event for this tuple.
		// We insert the results as a sim_node into the
		// userEvents table; we'll do calculations later.
		newnode = createSimNode(simitem, simevent);
		userEvents[i] = simInsert(userEvents[i], newnode);
		numEvents++;
	}

	// Query cleanup.
	recathon_queryEnd(simqueryDesc, simcontext);
	pfree(querystring);

	// Now we do the similarity calculations. Note that we
	// don't include duplicate entries, to save time and space.
	// The first user ALWAYS has a lower value than the second.
	for (i = 0; i < numUsers; i++) {
		float length_i;
		sim_node user_i;

		user_i = userEvents[i];
		if (!user_i) continue;
		length_i = userLengths[i];

		for (j = i+1; j < numUsers; j++) {
			float length_j;
			sim_node user_j;
			float similarity;

			user_j = userEvents[j];
			if (!user_j) continue;
			length_j = userLengths[j];

			similarity = cosineSimilarity(user_i, user_j, length_i, length_j);
			if (similarity <= 0) continue;

			/* Now we output. Like with the pre-computed model, we'll
			 * only worry about half the model. This allows us to fill
			 * in the matrix left-to-right, top-to-bottom. */
			usermodel[i][j] = similarity;
		}

		CHECK_FOR_INTERRUPTS();
	}

	// Free up the lists of sim_nodes and we're done.
	for (i = 0; i < numUsers; i++) {
		freeSimList(userEvents[i]);
		userEvents[i] = NULL;
	}

	// Return the relevant information.
	recnode->totalUsers = numUsers;
	recnode->userList = userIDs;
	recnode->userCFmodel = usermodel;
}

/* ----------------------------------------------------------------
 *		generateUserPearModel
 *
 *		Create a user-based Pearson model on the fly.
 * ----------------------------------------------------------------
 */
void
generateUserPearModel(RecScanState *recnode) {
	int i, j, priorID;
	int numEvents = 0;
	char *querystring;
	sim_node *userEvents;
	char *eventtable, *userkey, *itemkey, *eventval;
	AttributeInfo *attributes;
	float **usermodel;
	int numUsers;
	int *userIDs;
	float *userAvgs;
	float *userPearsons;
	// Information for other queries.
	QueryDesc *simqueryDesc;
	PlanState *simplanstate;
	TupleTableSlot *simslot;
	MemoryContext simcontext;

	attributes = (AttributeInfo*) recnode->attributes;
	eventtable = attributes->eventtable;
	userkey = attributes->userkey;
	itemkey = attributes->itemkey;
	eventval = attributes->eventval;

	// First, we need Pearson info.
	pearson_info(userkey, eventtable, eventval, &numUsers, &userIDs, &userAvgs, &userPearsons);

	/* We have the number of users, so we can initialize our model. */
	usermodel = (float**) palloc(numUsers*sizeof(float*));
	for (i = 0; i < numUsers; i++)
		usermodel[i] = (float*) palloc0(numUsers*sizeof(float));

	// With the precomputation done, we need to derive the actual item
	// similarities. We can do this in a way that's linear in the number
	// of I/Os and also the amount of storage. The complexity is relegated
	// to in-memory calculations, which is the most affordable. We need to
	// use this data structure here.
	userEvents = (sim_node*) palloc(numUsers*sizeof(sim_node));
	for (i = 0; i < numUsers; i++)
		userEvents[i] = NULL;

	// With the model created, we need to populate it, which means calculating
	// similarity between all item pairs. We need to query the events table
	// in order to get the key information.
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,userkey);

	// Begin extracting data.
	priorID = -1;
	simqueryDesc = recathon_queryStart(querystring, &simcontext);
	simplanstate = simqueryDesc->planstate;
	i = -1;

	for (;;) {
		int simuser, simitem;
		float simevent;
		sim_node newnode;

		// Shut the compiler up.
		simuser = 0; simitem = 0; simevent = 0.0;

		simslot = ExecProcNode(simplanstate);
		if (TupIsNull(simslot)) break;

		simuser = getTupleInt(simslot,userkey);
		simitem = getTupleInt(simslot,itemkey);
		simevent = getTupleFloat(simslot,eventval);

		// Are we dealing with a new user ID? If so, switch to the next slot.
		if (simuser != priorID) {
			priorID = simuser;
			i++;
		}

		// We now have the user, item, and event for this tuple.
		// We insert the results as a sim_node into the
		// userEvents table; we'll do calculations later.
		newnode = createSimNode(simitem, simevent);
		userEvents[i] = simInsert(userEvents[i], newnode);
		numEvents++;
	}

	// Query cleanup.
	recathon_queryEnd(simqueryDesc, simcontext);
	pfree(querystring);

	// Now we do the similarity calculations. Note that we
	// don't include duplicate entries, to save time and space.
	// The first item ALWAYS has a lower value than the second.
	for (i = 0; i < numUsers; i++) {
		float avg_i, pearson_i;
		sim_node user_i;

		user_i = userEvents[i];
		if (!user_i) continue;
		avg_i = userAvgs[i];
		pearson_i = userPearsons[i];

		for (j = i+1; j < numUsers; j++) {
			float avg_j, pearson_j;
			sim_node user_j;
			float similarity;

			user_j = userEvents[j];
			if (!user_j) continue;
			avg_j = userAvgs[j];
			pearson_j = userPearsons[j];

			similarity = pearsonSimilarity(user_i, user_j, avg_i, avg_j, pearson_i, pearson_j);
			if (similarity == 0.0) continue;

			/* Now we output. Like with the pre-computed model, we'll
			 * only worry about half the model. This allows us to fill
			 * in the matrix left-to-right, top-to-bottom. */
			usermodel[i][j] = similarity;
		}

		CHECK_FOR_INTERRUPTS();
	}

	// Free up the lists of sim_nodes and we're done.
	for (i = 0; i < numUsers; i++) {
		freeSimList(userEvents[i]);
		userEvents[i] = NULL;
	}

	// Return the relevant information.
	recnode->totalUsers = numUsers;
	recnode->userList = userIDs;
	recnode->userCFmodel = usermodel;
}

/* ----------------------------------------------------------------
 *		generateSVDmodel
 *
 *		Create a SVD model on the fly.
 * ----------------------------------------------------------------
 */
void
generateSVDmodel(RecScanState *recnode) {
	float **userFeatures, **itemFeatures;
	int *userIDs, *itemIDs;
	float *itemAvgs, *userOffsets;
	int numUsers, numItems;
	int i, j, k, numEvents;
	int numFeatures = 50;
	svd_node *allEvents;
	AttributeInfo *attributes;
	char *eventtable, *userkey, *itemkey, *eventval;
	// Information for other queries.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	attributes = (AttributeInfo*) recnode->attributes;
	eventtable = attributes->eventtable;
	userkey = attributes->userkey;
	itemkey = attributes->itemkey;
	eventval = attributes->eventval;

	// First, we get our lists of users and items.
	SVDlists(userkey,itemkey,eventtable,
		&userIDs, &itemIDs, &numUsers, &numItems);

	// Then we get information for baseline averages.
	SVDaverages(userkey,itemkey,eventtable,eventval,
		userIDs,itemIDs,numUsers,numItems,
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

	// First we need to count the number of events we'll be
	// considering.
	querystring = (char*) palloc(1024*sizeof(char));
	numEvents = count_rows(eventtable);

	// Initialize the events array.
	allEvents = (svd_node*) palloc(numEvents*sizeof(svd_node));

	sprintf(querystring,"SELECT r.%s,r.%s,r.%s FROM %s r ORDER BY r.%s;",
		userkey,itemkey,eventval,eventtable,userkey);

	// Let's acquire all of our events and store them. Sorting initially by
	// user ID avoids unnecessary binary searches.
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	i = 0;
	for (;;) {
		svd_node new_svd;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		new_svd = createSVDnode(slot, userkey, itemkey, eventval, userIDs, itemIDs, numUsers, numItems);

		allEvents[i] = new_svd;

		i++;
		if (i >= numEvents) break;
	}

	recathon_queryEnd(queryDesc,recathoncontext);

	// We now have all of the events, so we can start training our features.
	for (j = 0; j < 100; j++) {
		for (i = 0; i < numFeatures; i++) {
			float learn = 0.001;
			float penalty = 0.002;
			float *userVal = userFeatures[i];
			float *itemVal = itemFeatures[i];

			for (k = 0; k < numEvents; k++) {
				int userid;
				int itemid;
				float event, err, residual, temp;
				svd_node current_svd;

				current_svd = allEvents[k];
				userid = current_svd->userid;
				itemid = current_svd->itemid;
				event = current_svd->event;
				// Need to reset residuals for each new
				// iteration of the trainer.
				if (i == 0)
					current_svd->residual = 0;
				residual = current_svd->residual;

				if (i == 0 && j == 0) {
					err = event - (itemAvgs[itemid] + userOffsets[userid]);
				} else {
					err = event - predictRating(i, numFeatures, userid, itemid,
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

	// Free up memory.
	pfree(querystring);
	pfree(itemAvgs);
	pfree(userOffsets);
	pfree(allEvents);

	// Return the relevant information.
	recnode->numFeatures = numFeatures;
	recnode->totalUsers = numUsers;
	recnode->fullTotalItems = numItems;
	recnode->userList = userIDs;
	recnode->fullItemList = itemIDs;
	recnode->SVDusermodel = userFeatures;
	recnode->SVDitemmodel = itemFeatures;
}

/* ----------------------------------------------------------------
 *		itemCFgenerate
 *
 *		Generates a predicted RecScore for a given user and
 *		item, for a recommender that uses item-based
 *		collaborative filtering built on-the-fly.
 * ----------------------------------------------------------------
 */
float
itemCFgenerate(RecScanState *recnode, int itemid, int itemindex)
{
	int i;
	float recScore;
	GenRating *currentItem;

	// First, we grab the GenRating for this item ID.
	currentItem = hashFind(recnode->pendingTable, itemid);
	// In case there's some error.
	if (!currentItem)
		return -1;

	// We're going to look through the similarity matrix for the
	// numbers that correspond to this item, and find which of those
	// also correspond to items this user rated. We will use that
	// information to obtain the estimated rating.

	for (i = itemindex+1; i < recnode->fullTotalItems; i++) {
		int itemID;
		float similarity;
		GenRating *ratedItem;

		itemID = recnode->fullItemList[i];
		similarity = recnode->itemCFmodel[itemindex][i];

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

	if (currentItem->totalSim == 0) return 0;

	recScore = currentItem->score / currentItem->totalSim;
	return recScore;
}

/* ----------------------------------------------------------------
 *		userCFgenerate
 *
 *		Generates a predicted RecScore for a given user and
 *		item, for a recommender that uses user-based
 *		collaborative filtering built on-the-fly.
 * ----------------------------------------------------------------
 */
float
userCFgenerate(RecScanState *recnode, int itemid, int itemindex)
{
	float event, totalSim, average;
	AttributeInfo *attributes;
	// Query objects;
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *qslot;
	MemoryContext recathoncontext;

	attributes = (AttributeInfo*) recnode->attributes;

	event = 0.0;
	totalSim = 0.0;
	average = recnode->average;

	/* We need to query the events table, so that we can
	 * find all events for this item and match them up
	 * with what we have in the similarity matrix. We note
	 * that it's necessarily true that the user has not
	 * rated these items. */
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"select * from %s where %s = %d;",
		attributes->eventtable,attributes->itemkey,itemid);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		int currentUserID;
		float currentRating, similarity;
		GenRating *currentUser;

		qslot = ExecProcNode(planstate);
		if (TupIsNull(qslot)) break;

		currentUserID = getTupleInt(qslot,attributes->userkey);
		currentRating = getTupleFloat(qslot,attributes->eventval);

		currentUser = hashFind(recnode->simTable,currentUserID);
		if (!currentUser) continue;
		similarity = currentUser->totalSim;

		event += (currentRating - average) * similarity;
		// Poor man's absolute value of the similarity.
		if (similarity < 0)
			similarity *= -1;
		totalSim += similarity;
	}
	recathon_queryEnd(queryDesc,recathoncontext);

	if (totalSim == 0.0) return 0.0;

	event /= totalSim;
	event += average;

	return event;
}

/* ----------------------------------------------------------------
 *		SVDgenerate
 *
 *		Generates a predicted RecScore for a given user and
 *		item, for a recommender that uses SVD built
 *		on-the-fly.
 * ----------------------------------------------------------------
 */
float
SVDgenerate(RecScanState *recnode, int itemid, int itemindex)
{
	int i;
	float **userFeatures, **itemFeatures;
	float recscore = 0.0;

	userFeatures = recnode->SVDusermodel;
	itemFeatures = recnode->SVDitemmodel;

	// At this point, our work is easy.
	for (i = 0; i < recnode->numFeatures; i++)
		recscore += userFeatures[i][recnode->userindex] * itemFeatures[i][itemindex];

	return recscore;
}

/* ----------------------------------------------------------------
 *		applyItemSimGenerate
 *
 *		The equivalent of applyItemSim for on-the-fly
 *		recommendation.
 * ----------------------------------------------------------------
 */
void
applyItemSimGenerate(RecScanState *recnode)
{
	int i, j;
	GenHash *ratedTable;

	ratedTable = recnode->ratedTable;

	// For every item we've rated, we need to obtain its similarity
	// scores and apply them to the appropriate items. This is
	// necessary because we're only storing half of the similarity
	// matrix.
	for (i = 0; i < ratedTable->hash; i++) {
		GenRating *currentItem;

		for (currentItem = ratedTable->table[i]; currentItem;
				currentItem = currentItem->next) {
			int itemindex = currentItem->index;

			for (j = itemindex+1; j < recnode->fullTotalItems; j++) {
				int itemID;
				float similarity;
				GenRating *pendingItem;

				itemID = recnode->fullItemList[j];
				similarity = recnode->itemCFmodel[itemindex][j];

				// If the similarity is 0, there's no point.
				if (similarity == 0.0)
					continue;

				// Find the array slot this item ID corresponds to.
				// If -1 is returned, then the item ID corresponds to
				// another item we've rated, so we don't care.
				pendingItem = hashFind(recnode->pendingTable,itemID);
				if (pendingItem) {
					pendingItem->score += similarity*currentItem->score;
					if (similarity < 0)
						similarity *= -1;
					pendingItem->totalSim += similarity;
				}
			}
		}
	}
}

/* ----------------------------------------------------------------
 *		prepUserForRating
 *
 *		Given a user ID, we need to create some data
 *		structures that will allow us to efficiently
 *		predict ratings for this user.
 * ----------------------------------------------------------------
 */
bool
prepUserForRating(RecScanState *recstate, int userID) {
	int i, userindex;
	// Query objects.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *hslot;
	MemoryContext recathoncontext;

	AttributeInfo *attributes = (AttributeInfo*) recstate->attributes;
	attributes->userID = userID;

	/* First off, we need to delete any existing structures. */
	if (recstate->ratedTable) {
		freeHash(recstate->ratedTable);
		recstate->ratedTable = NULL;
	}
	if (recstate->pendingTable) {
		freeHash(recstate->pendingTable);
		recstate->pendingTable = NULL;
	}
	if (recstate->simTable) {
		freeHash(recstate->simTable);
		recstate->simTable = NULL;
	}
	if (recstate->userFeatures) {
		pfree(recstate->userFeatures);
		recstate->userFeatures = NULL;
	}

	/* INSERT FORMER LIST CODE HERE */
	querystring = (char*) palloc(1024*sizeof(char));

	switch ((recMethod) attributes->method) {
		/* If this is an item-based CF recommender, we can pre-obtain
		 * the ratings of this user, and add in their contributions to
		 * the scores of all the other items. */
		case itemCosCF:
		case itemPearCF:
			/* The rated list is all of the items this user has
			 * rated already. We store the ratings now and we'll
			 * use them during calculation. */
			sprintf(querystring,"select count(*) from %s where %s = %d;",
				attributes->eventtable,attributes->userkey,userID);
			queryDesc = recathon_queryStart(querystring,&recathoncontext);
			planstate = queryDesc->planstate;
			hslot = ExecProcNode(planstate);
			recstate->totalRatings = getTupleInt(hslot,"count");
			recathon_queryEnd(queryDesc,recathoncontext);

			/* It's possible that someone has rated no items. */
			if (recstate->totalRatings <= 0) {
				elog(WARNING, "user %d has rated no items, no predictions can be made",
					userID);
				return false;
			}

			recstate->ratedTable = hashCreate(recstate->totalRatings);

			/* Now to acquire the actual ratings. */
			sprintf(querystring,"select * from %s where %s = %d order by %s;",
				attributes->eventtable,attributes->userkey,
				userID,attributes->itemkey);
			queryDesc = recathon_queryStart(querystring,&recathoncontext);
			planstate = queryDesc->planstate;

			i = 0;
			for (;;) {
				int currentItem;
				float currentRating;
				GenRating *newItem;

				hslot = ExecProcNode(planstate);
				if (TupIsNull(hslot)) break;

				currentItem = getTupleInt(hslot,attributes->itemkey);
				currentRating = getTupleFloat(hslot,attributes->eventval);

				newItem = (GenRating*) palloc(sizeof(GenRating));
				newItem->ID = currentItem;
				newItem->index = binarySearch(recstate->fullItemList,currentItem,0,recstate->fullTotalItems);
				newItem->score = currentRating;
				newItem->next = NULL;
				hashAdd(recstate->ratedTable, newItem);

				i++;
				if (i >= recstate->totalRatings) break;
			}
			recathon_queryEnd(queryDesc,recathoncontext);

			/* Quick error protection. Again, I don't know how this could
			 * possibly happen, but better safe than sorry. */
			recstate->totalRatings = i;
			if (recstate->totalRatings <= 0) {
				elog(WARNING, "user %d has rated no items, no predictions can be made",
					userID);
				return false;
			}

			/* The pending list is all of the items we have yet to
			 * calculate ratings for. We need to maintain partial
			 * scores and similarity sums for each one. In this version
			 * of the code, note that we rate all items. */
			recstate->pendingTable = hashCreate(recstate->fullTotalItems);
			for (i = 0; i < recstate->fullTotalItems; i++) {
				GenRating *newItem;

				newItem = (GenRating*) palloc(sizeof(GenRating));
				newItem->ID = recstate->fullItemList[i];
				/* The pending list doesn't need indexes. */
				newItem->index = -1;
				newItem->score = 0.0;
				newItem->totalSim = 0.0;
				newItem->next = NULL;
				hashAdd(recstate->pendingTable, newItem);
			}

			/* With another function, we apply the ratings and similarities
			 * from the rated items to the unrated ones. It's good to get
			 * this done early, as this will allow the operator to be
			 * non-blocking, which is important. */
			if (attributes->opType == OP_GENERATE || attributes->opType == OP_GENERATEJOIN)
				applyItemSimGenerate(recstate);
			else
				applyItemSim(recstate, attributes->recModelName);
			break;
		case userCosCF:
		case userPearCF:
			userindex = binarySearch(recstate->userList, userID, 0, recstate->totalUsers);

			/* The first thing we'll do is obtain the average rating. */
			sprintf(querystring,"select avg(%s) as average from %s where %s = %d;",
				attributes->eventval,attributes->eventtable,
				attributes->userkey,userID);
			queryDesc = recathon_queryStart(querystring,&recathoncontext);
			planstate = queryDesc->planstate;

			hslot = ExecProcNode(planstate);
			recstate->average = getTupleFloat(hslot,"average");
			recathon_queryEnd(queryDesc,recathoncontext);

			/* Next, we need to store this user's similarity model
			 * in a hash table for easier access. We base the table on
			 * the number of items we have to rate - a close enough
			 * approximation that we won't have much trouble. */
			recstate->simTable = hashCreate(recstate->fullTotalItems);

			/* We need to find the entire similarity table for this
			 * user, which will be in two parts. */
			if (attributes->opType == OP_GENERATE || attributes->opType == OP_GENERATEJOIN) {
				for (i = 0; i < userindex; i++) {
					int currentUser;
					float currentSim;
					GenRating *newUser;

					currentUser = recstate->userList[i];
					currentSim = recstate->userCFmodel[i][userindex];

					newUser = (GenRating*) palloc(sizeof(GenRating));
					newUser->ID = currentUser;
					newUser->index = i;
					newUser->totalSim = currentSim;
					newUser->next = NULL;
					hashAdd(recstate->simTable, newUser);
				}

				for (i = userindex+1; i < recstate->totalUsers; i++) {
					int currentUser;
					float currentSim;
					GenRating *newUser;

					currentUser = recstate->userList[i];
					currentSim = recstate->userCFmodel[userindex][i];

					newUser = (GenRating*) palloc(sizeof(GenRating));
					newUser->ID = currentUser;
					newUser->index = i;
					newUser->totalSim = currentSim;
					newUser->next = NULL;
					hashAdd(recstate->simTable, newUser);
				}
			} else {
				sprintf(querystring,"select * from %s where user1 < %d and user2 = %d;",
					attributes->recModelName,attributes->userID,
					attributes->userID);
				queryDesc = recathon_queryStart(querystring,&recathoncontext);
				planstate = queryDesc->planstate;

				for (;;) {
					int currentUser;
					float currentSim;
					GenRating *newUser;

					hslot = ExecProcNode(planstate);
					if (TupIsNull(hslot)) break;

					currentUser = getTupleInt(hslot,"user1");
					currentSim = getTupleFloat(hslot,"similarity");

					newUser = (GenRating*) palloc(sizeof(GenRating));
					newUser->ID = currentUser;
					/* Pre-generated recommendation doesn't need
					 * indexes. */
					newUser->index = -1;
					newUser->totalSim = currentSim;
					newUser->next = NULL;
					hashAdd(recstate->simTable, newUser);
				}
				recathon_queryEnd(queryDesc,recathoncontext);

				/* Here's the second. */
				sprintf(querystring,"select * from %s where user1 = %d;",
					attributes->recModelName,attributes->userID);
				queryDesc = recathon_queryStart(querystring,&recathoncontext);
				planstate = queryDesc->planstate;

				for (;;) {
					int currentUser;
					float currentSim;
					GenRating *newUser;

					hslot = ExecProcNode(planstate);
					if (TupIsNull(hslot)) break;

					currentUser = getTupleInt(hslot,"user2");
					currentSim = getTupleFloat(hslot,"similarity");

					newUser = (GenRating*) palloc(sizeof(GenRating));
					newUser->ID = currentUser;
					/* Pre-generated recommendation doesn't need
					 * indexes. */
					newUser->index = -1;
					newUser->totalSim = currentSim;
					newUser->next = NULL;
					hashAdd(recstate->simTable, newUser);
				}
				recathon_queryEnd(queryDesc,recathoncontext);
			}

			break;
		/* If this is a SVD recommender, we can pre-obtain the user features,
		 * which stay fixed, and cut the I/O time in half. Of course, if this
		 * is generated on-the-fly, this is done already. */
		case SVD:
			if (attributes->opType != OP_GENERATE && attributes->opType != OP_GENERATEJOIN) {
				recstate->userFeatures = (float*) palloc(50*sizeof(float));
				for (i = 0; i < 50; i++)
					recstate->userFeatures[i] = 0;
				sprintf(querystring,"select * from %s where users = %d;",
					attributes->recModelName,userID);
				queryDesc = recathon_queryStart(querystring,&recathoncontext);
				planstate = queryDesc->planstate;

				for (;;) {
					int feature;
					float featValue;

					hslot = ExecProcNode(planstate);
					if (TupIsNull(hslot)) break;

					feature = getTupleInt(hslot,"feature");
					featValue = getTupleFloat(hslot,"value");

					recstate->userFeatures[feature] = featValue;
				}

				recathon_queryEnd(queryDesc,recathoncontext);
			}
			break;
		default:
			elog(ERROR, "invalid recommendation method in prepUserForRating()");
	}

	/* If we've gotten to this point, this is a valid user, so return true. */
	pfree(querystring);
	return true;
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
 *		freeHash
 *
 *		Free a hash table.
 * ----------------------------------------------------------------
 */
void
freeHash(GenHash *table) {
	int i;

	if (!table)
		return;

	for (i = 0; i < table->hash; i++) {
		GenRating *tempRating;

		tempRating = table->table[i];
		while (tempRating) {
			GenRating *tempRating2 = tempRating->next;
			pfree(tempRating);
			tempRating = tempRating2;
		}
	}

	pfree(table->table);
	pfree(table);
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
userCFpredict(RecScanState *recnode, char *eventval, int itemid)
{
	float event, totalSim, average;
	AttributeInfo *attributes;
	// Query objects;
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *qslot;
	MemoryContext recathoncontext;

	attributes = (AttributeInfo*) recnode->attributes;

	event = 0.0;
	totalSim = 0.0;
	average = recnode->average;

	/* We need to query the events table, so that we can
	 * find all events for this item and match them up
	 * with what we have in the similarity matrix. We note
	 * that it's necessarily true that the user has not
	 * rated these items. */
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"select * from %s where %s = %d;",
		attributes->eventtable,attributes->itemkey,itemid);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	for (;;) {
		int currentUserID;
		float currentRating, similarity;
		GenRating *currentUser;

		qslot = ExecProcNode(planstate);
		if (TupIsNull(qslot)) break;

		currentUserID = getTupleInt(qslot,attributes->userkey);
		currentRating = getTupleFloat(qslot,eventval);

		currentUser = hashFind(recnode->simTable,currentUserID);
		if (!currentUser) continue;
		similarity = currentUser->totalSim;

		event += (currentRating - average) * similarity;
		// Poor man's absolute value of the similarity.
		if (similarity < 0)
			similarity *= -1;
		totalSim += similarity;
	}
	recathon_queryEnd(queryDesc,recathoncontext);

	if (totalSim == 0.0) return 0.0;

	event /= totalSim;
	event += average;

	return event;
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

		// Add it into the event and continue.
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
applyRecScore(RecScanState *recnode, TupleTableSlot *slot, int itemid, int itemindex)
{
	float recscore;
	AttributeInfo *attributes;

	attributes = (AttributeInfo*) recnode->attributes;

	switch ((recMethod)attributes->method) {
		case itemCosCF:
		case itemPearCF:
			if (attributes->opType == OP_GENERATE || attributes->opType == OP_GENERATEJOIN)
				recscore = itemCFgenerate(recnode,itemid,itemindex);
			else
				recscore = itemCFpredict(recnode,attributes->recModelName,itemid);
			break;
		case userCosCF:
		case userPearCF:
			if (attributes->opType == OP_GENERATE || attributes->opType == OP_GENERATEJOIN)
				recscore = userCFgenerate(recnode,itemid,itemindex);
			else
				recscore = userCFpredict(recnode,attributes->eventval,itemid);
			break;
		case SVD:
			if (attributes->opType == OP_GENERATE || attributes->opType == OP_GENERATEJOIN)
				recscore = SVDgenerate(recnode,itemid,itemindex);
			else
				recscore = SVDpredict(recnode,attributes->recModelName2,itemid);
			break;
		default:
			recscore = -1;
			break;
	}

	slot->tts_values[recnode->eventatt] = Float4GetDatum(recscore);
	slot->tts_isnull[recnode->eventatt] = false;
}

/* ----------------------------------------------------------------
 *		applyItemSim
 *
 *		This function is one of the first steps in
 *		item-based CF prediction generation. We take the
 *		ratings this user has generated and we apply those
 *		similarity values to our tentative ratings.
 * ----------------------------------------------------------------
 */
void
applyItemSim(RecScanState *recnode, char *itemmodel)
{
	int i;
	GenHash *ratedTable;
	// Query objects.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	ratedTable = recnode->ratedTable;

<<<<<<< HEAD
	querystring = (char*) palloc(1024*sizeof(char));

	// For every item we've rated, we need to obtain its similarity
	// scores and apply them to the appropriate items. This is
	// necessary because we're only storing half of the similarity
	// matrix.
	for (i = 0; i < ratedTable->hash; i++) {
		GenRating *currentItem;

		for (currentItem = ratedTable->table[i]; currentItem;
				currentItem = currentItem->next) {
			sprintf(querystring,"select * from %s where item1 = %d;",
				itemmodel,currentItem->ID);
			queryDesc = recathon_queryStart(querystring,&recathoncontext);
			planstate = queryDesc->planstate;

			for (;;) {
				int itemID;
				float similarity;
				GenRating *pendingItem;

				slot = ExecProcNode(planstate);
				if (TupIsNull(slot)) break;

				itemID = getTupleInt(slot,"item2");
				similarity = getTupleFloat(slot,"similarity");

				// Find the array slot this item ID corresponds to.
				// If -1 is returned, then the item ID corresponds to
				// another item we've rated, so we don't care.
				pendingItem = hashFind(recnode->pendingTable,itemID);
				if (pendingItem) {
					pendingItem->score += similarity*currentItem->score;
					if (similarity < 0)
						similarity *= -1;
					pendingItem->totalSim += similarity;
				}
			}

			recathon_queryEnd(queryDesc,recathoncontext);
		}
	}

	pfree(querystring);
}

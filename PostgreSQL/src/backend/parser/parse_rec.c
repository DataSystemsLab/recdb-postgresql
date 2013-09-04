/*-------------------------------------------------------------------------
 *
 * parse_rec.c
 *	  functions to parse RECOMMEND queries
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_rec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "parser/parse_rec.h"
#include "utils/builtins.h"
#include "utils/recathon.h"

static bool allValid(List *fromClause);
static void validateClauses(SelectStmt *stmt);
static bool recCmp(char *recname, char *recindexname);
static RecommendInfo* getRecommenderList(List *fromClause, RecommendInfo *recInfo);
static RecommendInfo* addRecInfo(RecommendInfo *headRecInfo, RecommendInfo *tailInfo, RangeVar *fromVar,
	TupleTableSlot *slot);
static AttributeInfo *getRecAttributes(RangeVar *recommender, int numatts, int method);
static void validateTargetList(SelectStmt *stmt, RecommendInfo *recInfo);
static int findUserID(Node* recExpr, RangeVar* recommender, char* userkey);
static int checkWhereClause(ColumnRef* attribute, RangeVar* recommender, char* userkey);
static void modifyAExpr(Node *currentExpr, char* recname, char* viewname);
static void modifyTargetList(List *target_list, char *recname, char *viewname);
static void modifyColumnRef(ColumnRef *attribute, char *recname, char *viewname);
static void modifyWhere(SelectStmt *stmt, RecommendInfo *headInfo);
static void modifyFrom(SelectStmt *stmt, RecommendInfo *recInfo);
static void filterfirst(Node *whereExpr, RecommendInfo *recInfo);
static bool filterfirstrecurse(Node *whereExpr, RecommendInfo *recInfo);

/*
 * transformRecommendClause -
 *	  Transform the expression, double-checking all given parameters to
 *	  make sure they are valid, and rewriting the query if necessary.
 *	  Used for RECOMMEND clause.
 *
 * constructName does not affect the semantics, but is used in error messages.
 */
SelectStmt *
transformRecommendClause(ParseState *pstate, SelectStmt *stmt,
					     const char *constructName)
{
	RecommendInfo *recInfo, *tailInfo;
	int userID;
	recInfo = NULL;

	// In order to avoid an infinite loop, we need to do an extra validation
	// step. If the user is issuing a correct RECOMMEND query, then one of
	// the entries in the FROM clause is not an actual relation. Thus, we
	// will look through the FROM clause and only proceed with RECOMMEND
	// checks if one of the relations is invalid.
	if (allValid(stmt->fromClause))
		return stmt;

	// Step one: ensure that at least one of the FROM elements is among our
	// list of existing recommenders. For each recommender we look to,
	// get some critical information from the RecModelsCatalogue. We also
	// check to see if the item table is in our FROM clause, as if it is,
	// it will change our recommend operator from FILTER to JOIN. If they
	// are not querying a recommender, this is a standard SELECT statement,
	// so just leave.
	recInfo = getRecommenderList(stmt->fromClause, recInfo);

	if (!recInfo)
		return stmt;

	// Find the end of the recommender list. (Will be used in a future version
	// perhaps?)
	tailInfo = recInfo;
	while (tailInfo->next)
		tailInfo = tailInfo->next;

	// If it turns out we are querying a recommender, then we need
	// to do some preprocessing and sanity checks. If any of the sanity
	// checks fail, we'll throw an error.
	validateClauses(stmt);

	// Now that we know the overall query has the right structure,
	// it's time to look at the specific information included.

	// In its current form, we can't have multiple recommenders in the
	// same nesting level. That's in the works for a future version of
	// Recathon, but for now it's one recommender per query. Multiple
	// recommender scores can be combined in other ways.
	if (recInfo->next)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("only one recommender can be used per basic query at this time")));

	// Step two: ensure that, somewhere in the target_list,
	// we have a reference to the item ID from the rating table.
	// The star_found boolean will indicate if all key target
	// elements are included by default (i.e. if there is a *).
	// We need to find the item ID for at least one recommender.
	// The other item IDs will be added into the WHERE clause
	// and then added implicitly to the target list as junk.
	// We also need to ensure that "recscore" is in the target
	// list as well.
	validateTargetList(stmt, recInfo);

	// Step three: make sure that a user ID is specified somewhere in the
	// WHERE clause.
	userID = findUserID(stmt->whereClause,recInfo->recommender,recInfo->attributes->userkey);
	if (userID < 0)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("recommend statement requires user ID in WHERE clause")));
	for (tailInfo = recInfo; tailInfo; tailInfo = tailInfo->next)
		tailInfo->attributes->userID = userID;

	// Step four: look through the WHERE clause to see if there are any RecScore
	// restrictions. If there are, we can't do pre-filtering.
	filterfirst(stmt->whereClause, recInfo);

	// Step five: ensure that all of the referenced recommenders are linked
	// together by their item IDs. We do this by adding to the WHERE clause.
	modifyWhere(stmt, recInfo);

	// Step six: now that we've verified the correctness of our query, we need to
	// rewrite the FROM clause to look through the correct table. We do this by
	// conducting a select query over the recommender index table.
	modifyFrom(stmt, recInfo);

	// With that done, we put in the changed recInfo and return.
	stmt->recommendClause = (Node*) recInfo;
	return stmt;
}

/*
 * allValid -
 *	  Check to ensure that all of the relations in the FROM clause
 *	  are valid.
 */
static bool
allValid(List *fromClause) {
	ListCell *from_cell;

	// Iterate through the FROM list to see if any of
	// the FROM targets is not valid.
	foreach(from_cell,fromClause) {
		Node *from_node = lfirst(from_cell);
		if (nodeTag(from_node) == T_RangeVar) {
			RangeVar *fromVar;

			fromVar = (RangeVar*) from_node;
			if (!relationExists(fromVar))
				return false;
		}
	}

	return true;
}

/*
 * validateClauses -
 *	  A series of sanity checks to make sure we don't have any
 *	  unnecessary clauses that could mess with our query.
 */
static void
validateClauses(SelectStmt *stmt) {
	// A FROM clause is required.
	if (!stmt->fromClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed without FROM clause")));
	// Numerous other clauses have to not be there. We're very picky.
	if (stmt->distinctClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed with DISTINCT clause")));
	if (stmt->intoClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed with INTO clause")));
	if (stmt->groupClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed with GROUP BY clause")));
	if (stmt->havingClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed with HAVING clause")));
	if (stmt->windowClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed with WINDOW clause")));
	if (stmt->lockingClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed with FOR clause")));
	if (stmt->withClause)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("RECOMMEND clause is not allowed with WITH clause")));

	return;
}

/* recCmp -
 *	  A quick function to compare a recommender name
 *	  to a recommender index name. Just making the
 *	  code cleaner.
 */
static bool
recCmp(char *recname, char *recindexname) {
	bool result;
	char *newindexname;

	newindexname = (char*) palloc((strlen(recname)+6)*sizeof(char));
	sprintf(newindexname,"%sIndex",recname);
	result = strcmp(newindexname,recindexname) == 0;

	pfree(newindexname);
	return result;
}

/*
 * getRecommenderList -
 *	  A function to look through the FROM list of our
 *	  query, to make sure that at least one of them
 *	  is a recommender. It also returns a list of
 *	  recommenders, with lots of relevant info.
 */
static RecommendInfo*
getRecommenderList(List *fromClause, RecommendInfo *recInfo) {
	RecommendInfo *tailInfo = recInfo;
	ListCell *from_cell;
	RangeVar *recmodelrv;
	bool recModelExists;
	// Information for query.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *slot;
	MemoryContext recathoncontext;

	// If there's no RecModelsCatalogue, just short-circuit.
	recmodelrv = makeRangeVar(NULL,"recmodelscatalogue",0);
	recModelExists = relationExists(recmodelrv);
	pfree(recmodelrv);
	if (!recModelExists)
		return NULL;

	// Because it's more expensive to query than it is to
	// scan our FROM list, the FROM list will be the inner
	// loop.
	querystring = (char*) palloc(128*sizeof(char*));
	sprintf(querystring,"SELECT * FROM RecModelsCatalogue;");
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	// Look through each of the returned tuples and compare it
	// to the from list.
	for (;;) {
		char *recindexname;

		slot = ExecProcNode(planstate);
		if (TupIsNull(slot)) break;

		recindexname = getTupleString(slot,"recommenderindexname");
		if (recindexname) {
			// Iterate through the FROM list to see if any of
			// the FROM targets is a recommender.
			foreach(from_cell,fromClause) {
				Node *from_node = lfirst(from_cell);
				if (nodeTag(from_node) == T_RangeVar) {
					RangeVar *fromVar;

					fromVar = (RangeVar*) from_node;
					if (recCmp(fromVar->relname,recindexname)) {
						tailInfo = addRecInfo(recInfo,tailInfo,fromVar,slot);
						// Is this the first recommender
						// we've found?
						if (!recInfo)
							recInfo = tailInfo;
					}
				}
			}
		}
	}

	recathon_queryEnd(queryDesc,recathoncontext);
	pfree(querystring);

	// If there are no recommenders in the list, it's a normal query.
	if (!recInfo)
		return NULL;

	// Now we check the FROM clause again to see if we are including the item
	// table, which means we want to do a JOINRECOMMEND. We assume the person
	// who made the query is sensible and will not add the item table to the
	// FROM clause unless it is appropriately in the WHERE clause.
	for (tailInfo = recInfo; tailInfo; tailInfo = tailInfo->next) {
		char* itemtable = recInfo->attributes->itemtable;

		foreach(from_cell,fromClause) {
			Node *from_node = lfirst(from_cell);
			if (nodeTag(from_node) == T_RangeVar) {
				RangeVar *fromVar;

				fromVar = (RangeVar*) from_node;
				if (strcmp(fromVar->relname,itemtable) == 0) {
					RecommendInfo *partnerInfo;

					tailInfo->opType = OP_JOIN;					
					tailInfo->attributes->opType = OP_JOIN;
					// We also need to mark the items table
					// as being in a RecJoin, because we need
					// to enforce a SeqScan on it.
					partnerInfo = makeNode(RecommendInfo);
					partnerInfo->opType = OP_JOINPARTNER;
					fromVar->recommender = (Node*) partnerInfo;
				}
			}
		}
	}

	return recInfo;
}

/*
 * addRecInfo -
 *	  This function does the legwork in creating a RecommendInfo*
 *	  to match a recommender in the FROM list.
 */
static RecommendInfo*
addRecInfo(RecommendInfo *headRecInfo, RecommendInfo *tailInfo, RangeVar *fromVar, TupleTableSlot *slot) {
	int numatts, method;
	char *strmethod;
	AttributeInfo *newAttInfo;

	// We'll make a new RecommendInfo object to insert some
	// information into.
	RecommendInfo *tempInfo = makeNode(RecommendInfo);
	tempInfo->recommender = fromVar;
	tempInfo->attributes = NULL;
	tempInfo->next = NULL;

	// If tailInfo is NULL, then we're dealing with the very
	// first recommender encountered, so it will go at the
	// head of the list. Otherwise, append it to the tail.
	if (!tailInfo)
		tailInfo = tempInfo;
	else {
		tailInfo->next = tempInfo;
		tailInfo = tempInfo;
	}

	// Grab the number of attributes from our tuple.
	numatts = getTupleInt(slot,"contextattributes");

	// Obtain the method used for this recommender.
	strmethod = getTupleString(slot,"method");
	method = (int) getRecMethod(strmethod);
	pfree(strmethod);

	// With the number of attributes in hand, we can use our other function
	// to get an AttributeInfo struct.
	newAttInfo = getRecAttributes(fromVar, numatts, method);

	// With that structure created, we go over the tuple a second time
	// to get the remaining information.
	newAttInfo->usertable = getTupleString(slot,"usertable");
	newAttInfo->itemtable = getTupleString(slot,"itemtable");
	newAttInfo->ratingtable = getTupleString(slot,"ratingtable");
	newAttInfo->userkey = getTupleString(slot,"userkey");
	newAttInfo->itemkey = getTupleString(slot,"itemkey");
	newAttInfo->ratingval = getTupleString(slot,"ratingval");

	// This is as much as we can fill in the data here, so let's
	// attach and return. To clarify, we're returning the current
	// tail of the RecommendInfo list.
	tailInfo->attributes = newAttInfo;
	return tailInfo;
}

/*
 * getRecAttributes -
 *	  We already know what attributes need to be considered in our query, but
 *	  in order to successfully perform the query, we need to know and store
 *	  the values. This function obtains the values and creates one of the
 *	  basic structures that will hold critical information.
 */
static AttributeInfo *
getRecAttributes(RangeVar *recommender, int numatts, int method) {
	int i;
	char **recAtts, **attValues;
	AttributeInfo *attInfo;
	char *recindexname;

	// Fortunately, we already have a function that will get the
	// attribute names for us.
	if (numatts > 0) {
		recindexname = (char*) palloc((strlen(recommender->relname)+6)*sizeof(char));
		sprintf(recindexname,"%sIndex",recommender->relname);
		recAtts = getAttNames(recindexname, numatts, method);
		pfree(recindexname);

		// We're going to obtain the values for ourselves, though.
		attValues = (char**) palloc(numatts*sizeof(char*));
		for (i = 0; i < numatts; i++)
			attValues[i] = (char*) palloc(256*sizeof(char));
	} else {
		recAtts = NULL;
		attValues = NULL;
	}

	// And we store it all in this structure.
	attInfo = makeNode(AttributeInfo);

	attInfo->userID = -1;
	attInfo->recName = (char*) palloc(128*sizeof(char));
	sprintf(attInfo->recName,"%s",recommender->relname);
	attInfo->usertable = NULL;
	attInfo->itemtable = NULL;
	attInfo->ratingtable = NULL;
	attInfo->userkey = NULL;
	attInfo->itemkey = NULL;
	attInfo->ratingval = NULL;
	attInfo->method = method;
	attInfo->recModelName = NULL;
	attInfo->recModelName2 = NULL;
	attInfo->recViewName = NULL;
	attInfo->numAtts = numatts;
	attInfo->attNames = recAtts;
	attInfo->attValues = attValues;
	attInfo->target_val = NULL;
	attInfo->IDfound = false;
	attInfo->cellType = CELL_BETA;
	attInfo->opType = OP_FILTER;

	return attInfo;
}

/*
 * validateTargetList -
 *	  One of the requirements of a RECOMMEND query is that the item ID
 *	  be part of the target list in the SELECT clause. Here, we ensure
 *	  this, and also make adjustments in case of multiple-recommender
 *	  queries.
 */
static void
validateTargetList(SelectStmt *stmt, RecommendInfo *recInfo) {
	bool star_found, IDfound, recscoreFound;
	ListCell *select_cell;
	RecommendInfo *tailInfo;

	star_found = false; IDfound = false; recscoreFound = false;

	// Now we look through our target_list. We need to ensure that every recommender
	// has its item ID listed. Since we're creating our own scan operator, we don't
	// need any other requirements.
	foreach(select_cell,stmt->targetList) {
		ResTarget* select_target;
		ColumnRef* target_val;
		ListCell *col_cell;

		select_target = (ResTarget*) lfirst(select_cell);
		target_val = (ColumnRef*) select_target->val;
		switch (target_val->fields->length) {
		case 1:
			// If there's only one element in the target, it had better
			// be the item ID from our rating table, RecScore, or a *.
			col_cell = target_val->fields->head;
			if (nodeTag(lfirst(col_cell)) == T_String) {
				bool IDhere = false;
				Value* col_string = (Value*) lfirst(col_cell);
				for (tailInfo = recInfo; tailInfo; tailInfo = tailInfo->next) {
					if (strcmp(col_string->val.str,tailInfo->attributes->itemkey) == 0) {
						// Only applies if we haven't found this item ID already.
						if (!tailInfo->attributes->IDfound) {
							IDfound = true;
							// If only one recommender so far has used this item ID,
							// we'll assign it this target_val, and we'll modify it to
							// specify this recommender. Future recommenders also using
							// this item ID will create new junk targets.
							if (!IDhere) {
								IDhere = true;
								tailInfo->attributes->target_val = target_val;
							}
						}
					} else if (strcmp(col_string->val.str,"recscore") == 0)
						recscoreFound = true;
				}
			} else if (nodeTag(lfirst(col_cell)) == T_A_Star)
				star_found = true;
			break;
		case 2:
		{
			// If our target has two elements, then the first needs to
			// refer to our recommender, and the second needs to be
			// the item ID from our rating table, RecScore, or *. If we
			// identify a recommender match, we store it in this variable.
			// At the end of the for loop, a NULL variable means no match.
			RecommendInfo *go_on = NULL;
			col_cell = target_val->fields->head;
			if (nodeTag(lfirst(col_cell)) == T_String) {
				Value* col_string = (Value*) lfirst(col_cell);
				for (tailInfo = recInfo; tailInfo; tailInfo = tailInfo->next) {
					// Fix: our recommender might not have an alias.
					if (tailInfo->recommender->alias == NULL) {
						if (strcmp(col_string->val.str,tailInfo->recommender->relname) == 0) {
							go_on = tailInfo;
							break;
						}
					} else {
						if ((strcmp(col_string->val.str,tailInfo->recommender->relname) == 0) ||
							(strcmp(col_string->val.str,tailInfo->recommender->alias->aliasname) == 0)) {
							go_on = tailInfo;
							break;
						}
					}
				}
			}
			// If the first element does indeed match our recommender, go on.
			if (go_on != NULL) {
				col_cell = col_cell->next;
				if (nodeTag(lfirst(col_cell)) == T_String) {
					Value* col_string = (Value*) lfirst(col_cell);
					// If the itemID attribute is matched.
					if (strcmp(col_string->val.str,go_on->attributes->itemkey) == 0) {
						IDfound = true;
						go_on->attributes->IDfound = true;
					} else if (strcmp(col_string->val.str,"recscore") == 0)
						recscoreFound = true;
				}
				// If they just call "recommender.*".
				else if (nodeTag(lfirst(col_cell)) == T_A_Star) {
					star_found = true;
				}
			}
			break;
		}
		default:
			break;
		}

		if (star_found) break;
	}

	// There are two conditions that will allow us to continue. The first
	// is that the target list is just *. The second is that the itemID was
	// matched for some recommender, as well as RecScore. If we're given the
	// item ID for any recommender, we're good to go. Even if it doesn't
	// reference the correct recommender, we can add in a reference to make
	// it work.
	if (!star_found) {
		if (!recscoreFound)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("RecScore is not in the SELECT clause")));
		else if (!IDfound)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("item ID is not in the SELECT clause")));
		else {
			// If we have some modifying to do.
			for (tailInfo = recInfo; tailInfo; tailInfo = tailInfo->next) {
				if (!tailInfo->attributes->IDfound) {
					// First, we need to know if we'll be using the recommender
					// alias or its actual name.
					char *recname;

					recname = (char*) palloc(128*sizeof(char));
					// Use alias or regular name?
					if (tailInfo->recommender->alias)
						sprintf(recname,"%s",tailInfo->recommender->alias->aliasname);
					else
						sprintf(recname,"%s",tailInfo->recommender->relname);

					// We're only going to modify existing item ID references in
					// the target list. Let's not add new ones, it's not needed.
					if (tailInfo->attributes->target_val) {
						// If the attributes item has a non-null target_val,
						// we're going to modify it so it contains a
						// recommender reference.
						tailInfo->attributes->target_val->fields =
							lcons(makeString(recname),tailInfo->attributes->target_val->fields);
					}
				}
			}
		}
	}
}

/*
 * findUserID -
 *	  A function to scan through the WHERE clause looking for
 *	  a user ID.
 */
static int
findUserID(Node* recExpr, RangeVar* recommender, char *userkey) {
	A_Expr *recAExpr;

	if (!recExpr)
		return -1;
	recAExpr = (A_Expr*) recExpr;

	// If our expression is an AND, recurse.
	if (recAExpr->kind == AEXPR_AND) {
		int userID;

		userID = findUserID(recAExpr->lexpr,recommender,userkey);
		if (userID >= 0) return userID;
		userID = findUserID(recAExpr->rexpr,recommender,userkey);
		if (userID >= 0) return userID;
	}

	// If our expression is an =, then do the actual check.
	if (recAExpr->kind == AEXPR_OP) {
		Value *opVal;
		char *opType;

		// It is possible to have this odd error under some circumstances.
		if (recAExpr->name->length == 0)
			return -1;

		opVal = (Value*) lfirst(recAExpr->name->head);
		opType = opVal->val.str;

		if (strcmp(opType,"=") == 0) {
			Node *cRefExpr = NULL;
			Node *constExpr = NULL;
			// We need to check the left and right arguments.

			// One of the two arguments should be some sort of
			// constant. The other is our column reference.
			if (nodeTag(recAExpr->rexpr) == T_A_Const) {
				cRefExpr = recAExpr->lexpr;
				constExpr = recAExpr->rexpr;
			}
			else if (nodeTag(recAExpr->lexpr) == T_A_Const) {
				cRefExpr = recAExpr->rexpr;
				constExpr = recAExpr->lexpr;
			}
			else
				return -1;

			// Check if the left is the user ID.
			if (nodeTag(cRefExpr) == T_ColumnRef) {
				int retvalue = checkWhereClause((ColumnRef*)cRefExpr,recommender,userkey);

				if (retvalue == -1)
					return -1;
				if (retvalue == 1) {
					// If we found the userID, make sure it corresponds to
					// an integer, then return it.
					A_Const *temp_num = (A_Const*) constExpr;
					Value val_val = temp_num->val;

					if (val_val.type != T_Integer)
						return -1;
					return (int) val_val.val.ival;
				}
			} else
				return -1;
		}
	}
	// All other kinds fail, at least for now.
	return -1;
}
/*
 * checkWhereClause -
 *	  A function to see if a given char* is within our WHERE
 *	  clause. Returns 1 if the user ID was found, 2 if the
 *	  RecScore was found, and -1 if neither was found.
 */
static int
checkWhereClause(ColumnRef* attribute, RangeVar* recommender, char* userkey) {
	List* attFields;
	ListCell* att_cell;

	attFields = attribute->fields;

	// Like before, what we do depends on the length of this list.
	switch (attFields->length) {
		case 1:
			// If there's only one element in the target, it needs
			// to be the user ID or RecScore.
			att_cell = attFields->head;

			if (nodeTag(lfirst(att_cell)) == T_String) {
				Value* att_string = (Value*) lfirst(att_cell);

				// If the userID attribute is matched.
				if (strcmp(att_string->val.str,userkey) == 0)
					return 1;
				// If recscore is matched.
				if (strcmp(att_string->val.str,"recscore") == 0)
					return 2;
			}
			break;
		case 2:
		{
			// If our target has two elements, then the first needs to
			// refer to our recommender, and the second needs to be
			// the user ID or RecScore.
			bool go_on = false;
			att_cell = attFields->head;
			if (nodeTag(lfirst(att_cell)) == T_String) {
				Value* att_string = (Value*) lfirst(att_cell);
				if (recommender->alias == NULL) {
					if (strcmp(att_string->val.str,recommender->relname) == 0)
						go_on = true;
				} else {
					if ((strcmp(att_string->val.str,recommender->relname) == 0) ||
						(strcmp(att_string->val.str,recommender->alias->aliasname) == 0))
						go_on = true;
				}
			}
			// If the first element does indeed match our recommender, go on.
			if (go_on == true) {
				att_cell = att_cell->next;
				if (nodeTag(lfirst(att_cell)) == T_String) {
					Value* att_string = (Value*) lfirst(att_cell);
					// If the userID attribute is matched.
					if (strcmp(att_string->val.str,userkey) == 0)
						return 1;
					// If recscore is matched.
					if (strcmp(att_string->val.str,"recscore") == 0)
						return 2;
				}
			}
			break;
		}
		default:
			break;
		}
	return -1;
}

/*
 * modifyWhere -
 *	  Enhance a WHERE clause to add equality between all of the item IDs
 *	  for the various recommenders.
 */
static void
modifyWhere(SelectStmt *stmt, RecommendInfo *headInfo) {
	RecommendInfo *walker1, *walker2;
	A_Expr *newAExpr;

	if (!headInfo) return;

	for (walker1 = headInfo; walker1->next; walker1 = walker1->next) {
		ColumnRef *ref1, *ref2;
		char *rec1, *rec2;
		A_Expr *eqOp;

		walker2 = walker1->next;

		ref1 = makeNode(ColumnRef);
		ref1->location = -1;
		// This is safe to do, as the itemIDname will not change after this point.
		ref1->fields = list_make1((Node*) makeString(walker1->attributes->itemkey));

		// It's not safe to simply plug in the recname, though, as it will change
		// soon. So we're going to do this instead.
		rec1 = (char*) palloc(128*sizeof(char));
		// Use alias or regular name?
		if (walker1->recommender->alias)
			sprintf(rec1,"%s",walker1->recommender->alias->aliasname);
		else
			sprintf(rec1,"%s",walker1->recommender->relname);
		ref1->fields = lcons(makeString(rec1),ref1->fields);

		ref2 = makeNode(ColumnRef);
		ref2->location = -1;
		ref2->fields = list_make1((Node*) makeString(walker2->attributes->itemkey));

		rec2 = (char*) palloc(128*sizeof(char));
		// Use alias or regular name?
		if (walker2->recommender->alias)
			sprintf(rec2,"%s",walker2->recommender->alias->aliasname);
		else
			sprintf(rec2,"%s",walker2->recommender->relname);
		ref2->fields = lcons(makeString(rec2),ref2->fields);

		// With the two ColumnRef nodes created, we're going to equate them.
		eqOp = makeSimpleA_Expr(AEXPR_OP, "=", (Node*) ref1, (Node*) ref2, -1);

		// Now we need to see what's been done with the WHERE clause already.
		// If there's nothing, we plug this in. If there's already something,
		// we need to create an AND A_Expr to connect them.
		if (!stmt->whereClause)
			stmt->whereClause = (Node*) eqOp;
		else {
			newAExpr = makeA_Expr(AEXPR_AND, NIL, stmt->whereClause, (Node*) eqOp, -1);
			stmt->whereClause = (Node*) newAExpr;
		}
	}
}

/*
 * modifyAExpr -
 *	  Scan through an AExpr, which would come from the WHERE or RECOMMEND clauses,
 *	  and replace one table name for another. Used for full maintenance.
 */
static void
modifyAExpr(Node *currentExpr, char* recname, char* viewname) {
	A_Expr *currentAExpr;

	if (!currentExpr)
		return;

	currentAExpr = (A_Expr*) currentExpr;
	// If our expression is an AND, recurse.
	if (currentAExpr->kind == AEXPR_AND) {
		modifyAExpr(currentAExpr->lexpr,recname,viewname);
		modifyAExpr(currentAExpr->rexpr,recname,viewname);
	}
	// If our expression is an =, then do the actual check.
	if (currentAExpr->kind == AEXPR_OP) {
		// We need to check the left argument to see if it matches our recommender.

		// Left should be a recommender attribute.
		if (nodeTag(currentAExpr->lexpr) == T_ColumnRef)
			modifyColumnRef((ColumnRef*)currentAExpr->lexpr, recname, viewname);
	}
}

/* Scan through the target list and replace one table name for another. */
static void
modifyTargetList(List *target_list, char *recname, char *viewname) {
	ListCell *select_cell;

	foreach(select_cell,target_list) {
		ResTarget* select_target = (ResTarget*) lfirst(select_cell);
		ColumnRef* target_val = (ColumnRef*) select_target->val;
		modifyColumnRef(target_val, recname, viewname);
	}
}

/* Check a ColumnRef to see if the table name should be replaced. */
static void
modifyColumnRef(ColumnRef *attribute, char *recname, char *viewname) {
	List* attFields = attribute->fields;
	ListCell* att_cell;
	// What we do depends on the length of this list.
	switch (attFields->length) {
		case 2:
		{
			// If our target has two elements, then the first needs to
			// refer to our recommender. If it's an alias, we won't need
			// to change anything; if it's a direct reference to the name,
			// we need to modify it.
			att_cell = attFields->head;
			if (nodeTag(lfirst(att_cell)) == T_String) {
				Value* att_string = (Value*) lfirst(att_cell);
				if (strcmp(att_string->val.str,recname) == 0)
					att_string->val.str = viewname;
			}
		}
		default:
			break;
	}
}

/*
 * modifyFrom -
 *	  Scan through the FROM list, replacing each instance of a recommender with
 *	  the appropriate table that we need to scan. Also make alterations to other
 *	  parts of the query as necessary.
 */
static void
modifyFrom(SelectStmt *stmt, RecommendInfo *recInfo) {
	RecommendInfo *tailInfo;

	for (tailInfo = recInfo; tailInfo; tailInfo = tailInfo->next) {
		int i;
		char *query_string, *relname;
		char *recmodelname, *recmodelname2, *recviewname;
		recMethod method;
		// Query information.
		QueryDesc *queryDesc;
		PlanState *planstate;
		TupleTableSlot *slot;
		MemoryContext recathoncontext;

		method = (recMethod) recInfo->attributes->method;

		// The first query is to obtain the recmodel used for a given
		// recommender. The query changes depending on our method,
		// due to the possibility of SVD.
		query_string = (char*) palloc(1024*sizeof(char));
		if (method == SVD)
			sprintf(query_string,"select r.recusermodelname,r.recitemmodelname,r.recviewname from %sindex r",
				recInfo->recommender->relname);
		else
			sprintf(query_string,"select r.recmodelname,r.recviewname from %sindex r",
				recInfo->recommender->relname);
		if (recInfo->attributes->numAtts > 0) strncat(query_string," where ",7);
		// Add info to the WHERE clause, based on the attribute, if there are any.
		for (i = 0; i < recInfo->attributes->numAtts; i++) {
			char add_string[128];
			sprintf(add_string,"r.%s='%s'",recInfo->attributes->attNames[i],
				recInfo->attributes->attValues[i]);
			strncat(query_string,add_string,strlen(add_string));
			if (i+1 < recInfo->attributes->numAtts)
				strncat(query_string," and ",5);
		}
		strncat(query_string,";",1);

		// Now we prep the query.
		queryDesc = recathon_queryStart(query_string, &recathoncontext);
		planstate = queryDesc->planstate;

		// Now that we have a correct planstate, we can actually get information
		// from the table.
		slot = ExecProcNode(planstate);
		if (TupIsNull(slot))
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("could not find corresponding RecModel")));

		// Now we grab the appropriate information from the query tuple.
		if (method == SVD) {
			recmodelname = getTupleString(slot,"recusermodelname");
			recmodelname2 = getTupleString(slot,"recitemmodelname");
		} else {
			recmodelname = getTupleString(slot,"recmodelname");
			recmodelname2 = NULL;
		}
		recviewname = getTupleString(slot,"recviewname");

		// Now to tidy up.
		recathon_queryEnd(queryDesc, recathoncontext);
		pfree(query_string);

		// If we get to this point and there's no recmodelname, our query turned
		// up no results, and something weird has gone wrong. I can't imagine a
		// scenario where this is even possible, since the attributes we're trying
		// to obtain are required to not be null, but it can't hurt.
		if (recmodelname == NULL || recviewname == NULL)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("could not find corresponding RecModel")));

		// Convert to lowercase.
		for (i = 0; i < strlen(recmodelname); i++)
			recmodelname[i] = tolower(recmodelname[i]);
		for (i = 0; i < strlen(recviewname); i++)
			recviewname[i] = tolower(recviewname[i]);
		if (recmodelname2) {
			for (i = 0; i < strlen(recmodelname2); i++)
				recmodelname2[i] = tolower(recmodelname2[i]);
		}

		// Store the info, so we can use it later for the query.
		recInfo->attributes->recModelName = recmodelname;
		recInfo->attributes->recModelName2 = recmodelname2;
		recInfo->attributes->recViewName = recviewname;

		// When we do find the match, we need to replace our recommender from the FROM clause
		// with the recviewname we found. We also need to modify everything in the WHERE and
		// RECOMMEND clauses if necessary, as well as the target list. At one point, we would
		// try to add a condition to the WHERE clause to restrict the user ID, but since we're
		// utilizing a custom scan operator, it's easier to do it manually at that stage.
		relname = tailInfo->recommender->relname;
		modifyAExpr(stmt->whereClause,relname,recviewname);
		modifyTargetList(stmt->targetList,relname,recviewname);
		tailInfo->recommender->relname = recviewname;

		// We need to store a pointer to this RecommendInfo struct in the RangeVar
		// itself, because we'll be passing it on to future structures and eventually
		// to the plan tree. It is circular, but it's a necessary evil.
		tailInfo->recommender->recommender = (Node*) tailInfo;
	}
}

/*
 * filterfirst -
 *	  Scan through the WHERE clause, looking for any instances of RecScore.
 *	  If there are any, that means that someone is utilizing some sort of
 *	  RecScore qualification, and so we have to calculate the actual RecScore
 *	  before we filter based on the WHERE qualifications.
 */
static void
filterfirst(Node *whereExpr, RecommendInfo *recInfo) {
	RecommendInfo *tailInfo;

	for (tailInfo = recInfo; tailInfo; tailInfo = tailInfo->next) {
		if (filterfirstrecurse(whereExpr, tailInfo)) {
			tailInfo->attributes->opType = OP_NOFILTER;
			tailInfo->opType = OP_NOFILTER;
		}
	}
}

/*
 * filterfirstrecurse -
 *	  The actual guts of filterfirst. We do this once per recommender.
 *	  Pretty similar to findUserID, but we do different things based
 *	  on what we find. We do utilize checkRecAttributes, though, to save
 *	  ourselves some code.
 */
static bool
filterfirstrecurse(Node *whereExpr, RecommendInfo *recInfo) {
	if (!whereExpr)
		return false;

	// If we've found a ColumnRef, then we check it to see if it
	// matches RecScore for this (or all) recommenders.
	if (nodeTag(whereExpr) == T_ColumnRef) {
		int retvalue = checkWhereClause((ColumnRef*)whereExpr, recInfo->recommender, recInfo->attributes->userkey);

		// 2 corresponds to "RecScore".
		if (retvalue == 2)
			return true;
		else
			return false;
	}

	// If we've found an A_Expr, recurse on the left and right. We only
	// need to find the RecScore in one place, so we "or" them together.
	if (nodeTag(whereExpr) == T_A_Expr) {
		A_Expr *whereAExpr = (A_Expr*) whereExpr;

		return filterfirstrecurse(whereAExpr->lexpr,recInfo) ||
			filterfirstrecurse(whereAExpr->rexpr,recInfo);
	}

	// All other types fail, for now.
	return false;
}

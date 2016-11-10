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
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_rec.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/recathon.h"

static void validateClauses(SelectStmt *stmt);
static RecommendInfo* getEventsTable(List *fromClause, Node *recommendClause);
static char* getTableRef(ColumnRef *colref, char **colname);
static AttributeInfo* getAttributeInfo(char *eventtable, char *userkey, char *itemkey, char *eventval,
                                       RecommendInfo *recInfo);
static int checkWhereClause(ColumnRef* attribute, RangeVar* recommender, char* userkey, char* eventval);
//static void modifyAExpr(Node *currentExpr, char* recname, char* viewname);
//static void modifyTargetList(List *target_list, char *recname, char *viewname);
//static void modifyColumnRef(ColumnRef *attribute, char *recname, char *viewname);
static void modifyFrom(SelectStmt *stmt, RecommendInfo *recInfo);
static void filterfirst(Node *whereExpr, RecommendInfo *recInfo);
static bool filterfirstrecurse(Node *whereExpr, RecommendInfo *recInfo);
//static void applyRecJoin(Node *whereClause, List *fromClause, RecommendInfo *recInfo);
//static RangeVar* locateJoinTable(Node* recExpr, List *fromClause, RangeVar* eventtable, char* key);
static bool tableMatch(RangeVar* table, char* tablename);
static Node *makeTrueConst();
static Node *userWhereClause(Node* whereClause, char *userkey);
static RangeVar* getRecommendVar(RangeVar * var);

/*
 * transformRecommendClause -
 *	  Transform the expression, double-checking all given parameters to
 *	  make sure they are valid, and rewriting the query if necessary.
 *	  Used for RECOMMEND clause.
 *
 * constructName does not affect the semantics, but is used in error messages.
 */
SelectStmt *
transformRecommendClause(ParseState *pstate, List **targetlist, SelectStmt *stmt,
                         const char *constructName)
{
    RecommendInfo *recInfo;
    Node *userWhere;
    recInfo = NULL;
    
    // We need to do some preprocessing and sanity checks. If any of the sanity
    // checks fail, we'll throw an error.
    validateClauses(stmt);
    
    // Now that we know the overall query has the right structure,
    // it's time to look at the specific information included.
    
    // Step one: by using the provided information in the RECOMMEND clause,
    // we need to determine what the event table is that we intend to use.
    // We also need to make sure that all of the RECOMMEND clause columns
    // correspond to said events table.
    recInfo = getEventsTable(stmt->fromClause, stmt->recommendClause);
    
    if (!recInfo)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("a valid events table has not been provided")));
    
    // Step two: look through the WHERE clause to see if there are any RecScore
    // restrictions. If there are, we need to mark this specially.
    filterfirst(stmt->whereClause, recInfo);
    
    // Step three: see if we are joining the user ID or item ID to another table, in
    // which case we want to perform a RecJoin.
    //	applyRecJoin(stmt->whereClause, stmt->fromClause, recInfo);
    
    // Step four: now that we've verified the correctness of our query, we need to
    // see if a recommender already exists for a given table and method. If so, we'll
    // reference the created RecModel; if not, we'll build it on the fly from the
    // schema of the events table. Either way, we make a note of which of our tables
    // will be the basis of our recommender schema.
    modifyFrom(stmt, recInfo);
    
    // Step five: we need to scan the WHERE clause and find which elements pertain
    // to the user key; this will save us a lot of unnecessary work later on. We'll
    // make a copy of it that we can modify for our purposes.
    userWhere = userWhereClause(((Node *) copyObject(stmt->whereClause)), recInfo->attributes->userkey);
    //	userWhere = userWhereClause(stmt->whereClause, recInfo->attributes->userkey);
    recInfo->attributes->userWhereClause = userWhere;
    
    // There's an additional step, where we add the RECOMMEND clause elements into
    // the target list if they aren't there, but we can't perform this step until
    // the target list and FROM clauses have been processed, so we'll leave that
    // for later.
    
    // With that done, we put in the changed recInfo and return.
    stmt->recommendClause = (Node*) recInfo;
    return stmt;
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

/*
 * getRecommendVar -
 *	  A helper function for getEventsTable, which prevents
 *	  circular linking from happening.
 *
 *
 */
static RangeVar* getRecommendVar(RangeVar * var){
    RangeVar * newvar = makeNode(RangeVar);
    newvar->catalogname = var->catalogname;
    newvar->schemaname = var->schemaname;
    newvar->relname = var->relname;
    newvar->inhOpt = var->inhOpt;
    newvar->relpersistence = var->relpersistence;
    newvar->alias = var->alias;
    newvar->location = var->location;
    newvar->recommender = NULL;
    
    return newvar;
}

/*
 * getEventsTable -
 *	  A function to look through the FROM list of our
 *	  query, to ascertain which of the elements is the
 *	  events table. We'll also run sanity checks on the
 *	  RECOMMEND clause while we're here.
 */
static RecommendInfo*
getEventsTable(List *fromClause, Node *recommendClause) {
    char *usertref, *itemtref, *eventtref;
    char *userkey, *itemkey, *eventval;
    char *eventtable = NULL;
    AttributeInfo *attributes;
    bool elem_match = true;
    
    RecommendInfo *recInfo = (RecommendInfo*) recommendClause;
    ColumnRef *usercr = (ColumnRef*) recInfo->userkey;
    ColumnRef *itemcr = (ColumnRef*) recInfo->itemkey;
    ColumnRef *eventcr = (ColumnRef*) recInfo->eventval;
    
    // First off, we'll perform sanity checks on the elements
    // of the RECOMMEND clause. We need to make sure their
    // table references are valid and matching.
    userkey = getTableRef(usercr, &usertref);
    itemkey = getTableRef(itemcr, &itemtref);
    eventval = getTableRef(eventcr, &eventtref);
    
    // If one element has no table reference, none of them can have
    // table references.
    if ((usertref && !itemtref) || (!usertref && itemtref))
        elem_match = false;
    if ((usertref && !eventtref) || (!usertref && eventtref))
        elem_match = false;
    
    // If table references exist, they all have to be the same.
    if (usertref && elem_match) {
        if ((strcmp(usertref,itemtref) != 0) ||
            (strcmp(usertref,eventtref) != 0))
            elem_match = false;
    }
    
    if (!elem_match)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("elements of RECOMMEND clause must have matching table references")));
    
    // Now that we've confirmed the correctness of the RECOMMEND
    // clause table references, we will use them to find the event table
    // referred to in the FROM clause. If there are table references, that makes
    // the task easy; if not, then we can manually check to see if each column
    // exists in a specific table.
    if (usertref) {
        // Let's look for a FROM table element that matches.
        ListCell *from_cell;
        foreach(from_cell,fromClause) {
            Node *from_node = lfirst(from_cell);
            if (nodeTag(from_node) == T_RangeVar) {
                RangeVar *fromVar;
                
                fromVar = (RangeVar*) from_node;
                if (tableMatch(fromVar,usertref)) {
                    eventtable = fromVar->relname;
                    //Use getRecommendVar to prevent circular linking while using copy functions
                    recInfo->recommender = getRecommendVar(fromVar);
                    fromVar->recommender = (Node*) recInfo;
                    break;
                }
            }
        }
    } else {
        // If we can't easily identify the events table via
        // table references, we will do so by cross-referencing the
        // column names with the name of each table.
        ListCell *from_cell;
        foreach(from_cell,fromClause) {
            Node *from_node = lfirst(from_cell);
            if (nodeTag(from_node) == T_RangeVar) {
                RangeVar *fromVar;
                
                fromVar = (RangeVar*) from_node;
                // If all the key columns are in this table...
                if (columnExistsInRelation(userkey,fromVar) &&
                    columnExistsInRelation(itemkey,fromVar) &&
                    columnExistsInRelation(eventval,fromVar)) {
                    // Did we already find a table matching all of
                    // these columns? If so, that's an error.
                    if (eventtable) {
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("ambiguous references in RECOMMEND clause")));
                    } else {
                        // Make a note of the name, and also
                        // do cross-storage of the table.
                        eventtable = fromVar->relname;
                        //Use getRecommendVar to prevent circular linking while using copy functions
                        recInfo->recommender = getRecommendVar(fromVar);
                        fromVar->recommender = (Node*) recInfo;
                    }
                }
            }
        }
    }
    
    // If we found nothing, return error.
    if (!eventtable)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("valid events table not found")));
    
    // Now that we've confirmed the RECOMMEND clause is well-formed,
    // we can start assembling our structure.
    attributes = getAttributeInfo(eventtable, userkey, itemkey, eventval,
                                  recInfo);
    recInfo->attributes = attributes;
    
    // That's all we need for now, really.
    return recInfo;
}

/*
 * getTableRef -
 *	  A function that takes in a ColumnRef and returns the
 *	  column name, with the table reference as a side effect.
 */
static char*
getTableRef(ColumnRef *colref, char **colname) {
    ListCell* col_cell;
    Value* col_string;
    int i;
    
    if (!colref)
        return NULL;
    
    if (nodeTag(colref) != T_ColumnRef) {
        (*colname) = NULL;
        return NULL;
    }
    
    // We take a look at the fields of the ColumnRef.
    if (colref->fields->length < 1) {
        (*colname) = NULL;
        return NULL;
    }
    
    col_cell = colref->fields->head;
    // Random error condition: is this field not a string?
    // I can't imagine where this would happen.
    if (nodeTag(lfirst(col_cell)) != T_String) {
        (*colname) = NULL;
        return NULL;
    }
    
    if (colref->fields->length < 2) {
        (*colname) = NULL;
        col_string = (Value*) lfirst(col_cell);
        return col_string->val.str;
    }
    
    // Depending on the number of fields, we need to continue
    // on until we reach the final two.
    for (i = 0; i+2 < colref->fields->length; i++)
        col_cell = col_cell->next;
    
    // Extract the first string value (the table reference)
    // and apply it.
    col_string = (Value*) lfirst(col_cell);
    (*colname) = col_string->val.str;
    
    // Now extract the actual column name and return it.
    col_cell = col_cell->next;
    col_string = (Value*) lfirst(col_cell);
    return col_string->val.str;
}

/*
 * getAttributeInfo -
 *	  A function to create an initial AttributeInfo struct. Much of this
 *	  is currently obsolete; it will be removed once the rest of the code
 *	  is cleaned up.
 */
static AttributeInfo*
getAttributeInfo(char *eventtable, char *userkey, char *itemkey, char *eventval,
                 RecommendInfo *recInfo) {
    AttributeInfo* attributes = makeNode(AttributeInfo);
    attributes->userID = -1;
    attributes->recName = NULL;
    attributes->usertable = NULL;
    attributes->itemtable = NULL;
    attributes->eventtable = eventtable;
    attributes->userkey = userkey;
    attributes->itemkey = itemkey;
    attributes->eventval = eventval;
    attributes->method = getRecMethod(recInfo->strmethod);
    attributes->recIndexName = NULL;
    attributes->recModelName = NULL;
    attributes->recModelName2 = NULL;
    attributes->recViewName = NULL;
    attributes->userWhereClause = NULL;
    attributes->IDfound = false;
    attributes->cellType = CELL_ALPHA;
    attributes->opType = recInfo->opType;
    attributes->noFilter = false;
    
    return attributes;
}

/*
 * addRecTargets -
 *	  This function will verify that certain ColumnRefs are part of the
 *	  target list. If they aren't there, they'll be added as resjunk, and
 *	  not returned in the final result.
 */
void
addRecTargets(ParseState *pstate, List **targetlist, Node *recClause) {
    RecommendInfo *recInfo = (RecommendInfo*) recClause;
    
    // Add the elements. Note that 3 corresponds to "RECOMMEND_CLAUSE"
    // in parse_clause.c.
    findTargetlistEntrySQL92(pstate, recInfo->userkey, targetlist, 3);
    findTargetlistEntrySQL92(pstate, recInfo->itemkey, targetlist, 3);
    findTargetlistEntrySQL92(pstate, recInfo->eventval, targetlist, 3);
}

/*
 * checkWhereClause -
 *	  A function to see if a given char* is within our WHERE
 *	  clause. Returns 1 if the user ID was found, 2 if the
 *	  event value was found, and -1 if neither was found.
 */
static int
checkWhereClause(ColumnRef* attribute, RangeVar* recommender, char* userkey, char *eventval) {
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
                // If the event value is matched.
                if (strcmp(att_string->val.str,eventval) == 0)
                    return 2;
            }
            break;
        case 2:
        {
            // If our target has two elements, then the first needs to
            // refer to our recommender, and the second needs to be
            // the user ID or eventval
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
                    // If the event value is matched.
                    if (strcmp(att_string->val.str,eventval) == 0)
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
 * modifyAExpr -
 *	  Scan through an AExpr, which would come from the WHERE or RECOMMEND clauses,
 *	  and replace one table name for another.
 *	  CURRENTLY NOT IN USE.
 */
/*static void
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
 }*/

/*
 * modifyTargetList -
 *	  Scan through the target list and replace one table name for another.
 *	  CURRENTLY NOT IN USE.
 */
/*static void
 modifyTargetList(List *target_list, char *recname, char *viewname) {
	ListCell *select_cell;
 
	foreach(select_cell,target_list) {
 ResTarget* select_target = (ResTarget*) lfirst(select_cell);
 ColumnRef* target_val = (ColumnRef*) select_target->val;
 modifyColumnRef(target_val, recname, viewname);
	}
 }*/

/*
 * modifyColumnRef -
 *	  Check a ColumnRef to see if the table name should be replaced.
 *	  CURRENTLY NOT IN USE.
 */
/*static void
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
 }*/

/*
 * modifyFrom -
 *	  Check and see if a recommender already exists based on a given events
 *	  table and method. If so, scan through the FROM list, replacing each instance
 *	  of the events table with the appropriate table that we need to scan. Also
 *	  make alterations to other parts of the query as necessary.
 */
static void
modifyFrom(SelectStmt *stmt, RecommendInfo *recInfo) {
    int i;
    //	char *eventtable;
    char *query_string, *recindexname;
    char *recmodelname, *recmodelname2, *recviewname;
    recMethod method;
    // Query information.
    QueryDesc *queryDesc;
    PlanState *planstate;
    TupleTableSlot *slot;
    MemoryContext recathoncontext;
    
    method = (recMethod) recInfo->attributes->method;
    // We'll take a look to see if a recommender was already built
    // on this table and method.
    recindexname = retrieveRecommender(recInfo->attributes->eventtable,recInfo->strmethod);
    
    // If no recommender turned up, we'll just return right away.
    // We'll utilize the events table for our event generation. Though
    // we should note if this is a join table.
    if (!recindexname) {
        if (recInfo->opType == OP_JOIN) {
            recInfo->opType = OP_GENERATEJOIN;
            recInfo->attributes->opType = OP_GENERATEJOIN;
        }
        return;
    }
    
    // We're using a different method from the default, which is
    // OP_GENERATE. If we changed it to OP_JOIN, though, we'll
    // leave it.
    if (recInfo->opType == OP_GENERATE) {
        recInfo->opType = OP_FILTER;
        recInfo->attributes->opType = OP_FILTER;
    }
    
    // If a recommender did turn up, then we need to track down the
    // RecView and replace our event table with it. We'll also store
    // the model table(s) for later use.
    query_string = (char*) palloc(1024*sizeof(char));
    if (method == SVD)
        sprintf(query_string,"select r.recusermodelname,r.recitemmodelname,r.recviewname from %s r;",
                recindexname);
    else
        sprintf(query_string,"select r.recmodelname,r.recviewname from %s r;",
                recindexname);
    
    // Now we prep the query.
    queryDesc = recathon_queryStart(query_string, &recathoncontext);
    planstate = queryDesc->planstate;
    
    // Now that we have a correct planstate, we can actually get information
    // from the table.
    slot = ExecProcNode(planstate);
    if (TupIsNull(slot)) {
        ereport(WARNING,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("recommender is built, but model could not be accessed. Building recommendation on the fly")));
        return;
    }
    
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
    if (recmodelname == NULL || recviewname == NULL) {
        ereport(WARNING,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("recommender is built, but data could not be accessed. Building recommendation on the fly")));
        return;
    }
    
    // Convert to lowercase.
    for (i = 0; i < strlen(recindexname); i++)
        recindexname[i] = tolower(recindexname[i]);
    for (i = 0; i < strlen(recmodelname); i++)
        recmodelname[i] = tolower(recmodelname[i]);
    for (i = 0; i < strlen(recviewname); i++)
        recviewname[i] = tolower(recviewname[i]);
    if (recmodelname2) {
        for (i = 0; i < strlen(recmodelname2); i++)
            recmodelname2[i] = tolower(recmodelname2[i]);
    }
    
    // Store the info, so we can use it later for the query.
    recInfo->attributes->recIndexName = recindexname;
    recInfo->attributes->recModelName = recmodelname;
    recInfo->attributes->recModelName2 = recmodelname2;
    recInfo->attributes->recViewName = recviewname;
    //	recInfo->attributes->recViewName = recInfo->attributes->eventtable;
    
    // When we do find the match, we need to replace our event table from the FROM clause
    // with the recviewname we found. We also need to modify everything in the WHERE and
    // RECOMMEND clauses if necessary, as well as the target list. At one point, we would
    // try to add a condition to the WHERE clause to restrict the user ID, but since we're
    // utilizing a custom scan operator, it's easier to do it manually at that stage.
    //	eventtable = recInfo->attributes->eventtable;
    //	modifyAExpr(stmt->whereClause,eventtable,recviewname);
    //	modifyTargetList(stmt->targetList,eventtable,recviewname);
    //	recInfo->recommender->relname = recviewname;
    
    // We need to store a pointer to this RecommendInfo struct in the RangeVar
    // itself, because we'll be passing it on to future structures and eventually
    // to the plan tree. Once we do this, we're done storing it in the recInfo,
    // so we'll remove it to avoid the circular linking. That would be disastrous
    // if copyObject were ever invoked.
    
    //recInfo->recommender->recommender = (Node*) recInfo;
    //recInfo->recommender = NULL;
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
    if (filterfirstrecurse(whereExpr, recInfo))
        recInfo->attributes->noFilter = true;
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
        int retvalue = checkWhereClause((ColumnRef*)whereExpr, recInfo->recommender, recInfo->attributes->userkey, recInfo->attributes->eventval);
        
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

/*
 * applyRecJoin -
 *	  A function to determine if we need to employ a RecJoin.
 *	  CURRENTLY NOT IN USE.
 */
/*static void
 applyRecJoin(Node *whereClause, List *fromClause, RecommendInfo *recInfo) {
	RangeVar *partnerTable;
	AttributeInfo *attributes = recInfo->attributes;
	RecommendInfo* partnerInfo;
 
	// It's only possible if we have multiple tables.
	if (list_length(fromClause) < 2)
 return;
 
	// Start by seeing if there's a match with the item key.
	partnerTable = locateJoinTable(whereClause, fromClause,
 recInfo->recommender, attributes->itemkey);
	// Then go to the user key.
	if (!partnerTable)
 partnerTable = locateJoinTable(whereClause, fromClause,
 recInfo->recommender, attributes->userkey);
 
	// If we found no such table, give up.
	if (!partnerTable)
 return;
 
	// Otherwise, we found an appropriate table. Make a note.
	recInfo->opType = OP_JOIN;
	attributes->opType = OP_JOIN;
	// Then we need to mark the appropriate table, appropriately.
	partnerInfo = makeNode(RecommendInfo);
	partnerInfo->opType = OP_JOINPARTNER;
	partnerTable->recommender = partnerInfo;
 }*/


/*
 * locateJoinTable -
 *	  A function to search through the WHERE clause and see if we are
 *	  joining our recommender with some other table, either by item ID
 *	  or user ID. We give preference to item ID.
 *	  CURRENTLY NOT IN USE.
 */
/*static RangeVar*
 locateJoinTable(Node* recExpr, List *fromClause, RangeVar* eventtable, char* key) {
	A_Expr *recAExpr;
 
	if (!recExpr)
 return NULL;
 
	// Turns out this isn't necessarily an A_Expr.
	if (nodeTag(recExpr) != T_A_Expr)
 return NULL;
 
	recAExpr = (A_Expr*) recExpr;
 
	// If our expression is an =, then do the actual check.
	if (recAExpr->kind == AEXPR_OP) {
 Value *opVal;
 char *opType;
 
 // It is possible to have this odd error under some circumstances.
 if (recAExpr->name->length == 0)
 return NULL;
 
 opVal = (Value*) lfirst(recAExpr->name->head);
 opType = opVal->val.str;
 
 if (strcmp(opType,"=") == 0) {
 // We need to check the left and right arguments.
 char *leftcol, *lefttable;
 char *rightcol, *righttable;
 
 // Left should be a ColumnRef. If it is, extract the info.
 // If we're dealing with the key in question, continue.
 if (nodeTag(recAExpr->lexpr) == T_ColumnRef) {
 ColumnRef *leftcr = (ColumnRef*) recAExpr->lexpr;
 
 leftcol = getTableRef(leftcr,&lefttable);
 if (strcmp(leftcol,key) != 0)
 return NULL;
 } else
 return NULL;
 
 // Right should be a ColumnRef too. If it is, extract the info.
 // If we're dealing with the key in question, continue.
 if (nodeTag(recAExpr->rexpr) == T_ColumnRef) {
 ColumnRef *rightcr = (ColumnRef*) recAExpr->rexpr;
 
 rightcol = getTableRef(rightcr,&righttable);
 if (strcmp(rightcol,key) != 0)
 return NULL;
 } else
 return NULL;
 
 // If either table reference is null, just return NULL. That's
 // not a valid WHERE clause anyway.
 if (!lefttable || !righttable)
 return NULL;
 
 // So we're dealing with two tables equating the key column.
 // If one of the tables matches our ratings table, we will
 // return the other one.
 if (tableMatch(eventtable,lefttable)) {
 ListCell *from_cell;
 foreach(from_cell,fromClause) {
 Node *from_node = lfirst(from_cell);
 if (nodeTag(from_node) == T_RangeVar) {
 RangeVar *fromVar;
 
 fromVar = (RangeVar*) from_node;
 if (tableMatch(fromVar,righttable))
 return fromVar;
 }
 }
 }
 else if (tableMatch(eventtable,righttable)) {
 ListCell *from_cell;
 foreach(from_cell,fromClause) {
 Node *from_node = lfirst(from_cell);
 if (nodeTag(from_node) == T_RangeVar) {
 RangeVar *fromVar;
 
 fromVar = (RangeVar*) from_node;
 if (tableMatch(fromVar,lefttable))
 return fromVar;
 }
 }
 }
 }
	}
	// If we didn't find what we're looking for, recurse.
	else {
 RangeVar *rtnvar;
 
 rtnvar = locateJoinTable(recAExpr->lexpr,fromClause,eventtable,key);
 if (rtnvar) return rtnvar;
 rtnvar = locateJoinTable(recAExpr->rexpr,fromClause,eventtable,key);
 if (rtnvar) return rtnvar;
	}
 
	// All other kinds fail, at least for now.
	return NULL;
 }*/

/*
 * tableMatch -
 *	  A function to determine if a given char* matches the name or
 *	  alias of a recommender.
 */
static bool
tableMatch(RangeVar* table, char* tablename) {
    if (table->alias) {
        if (strcmp(table->alias->aliasname,tablename) == 0) {
            return true;
        }
    } else {
        if (strcmp(table->relname,tablename) == 0) {
            return true;
        }
    }
    
    return false;
}

/*
 * makeTrueConst -
 *	  A function that creates an AConst to represent a TRUE
 *	  value. Used when generating a user WHERE clause.
 */
static Node*
makeTrueConst() {
    A_Const *n;
    TypeCast *tc;
    
    n = makeNode(A_Const);
    n->val.type = T_String;
    n->val.val.str = "t";
    n->location = -1;
    
    tc = makeNode(TypeCast);
    tc->arg = (Node *) n;
    tc->typeName = SystemTypeName("bool");
    tc->location = -1;
    
    return (Node *) tc;
}

/*
 * userWhereOp -
 *	  A helper function for userWhereClause, which recurses deeper
 *	  into AEXPR_OP types if necessary. Returns 1 if the user key
 *	  was found, -1 if some other ColumnRef was found, and 0 if
 *	  neither.
 */
static int
userWhereOp(Node* whereClause, char *userkey) {
    A_Expr *recAExpr;
    int leftresult = 0, rightresult = 0;
    
    if (!whereClause)
        return 0;
    
    recAExpr = (A_Expr*) whereClause;
    
    // If our expression is an OP or IN, then do the actual check.
    if (recAExpr->kind == AEXPR_OP || recAExpr->kind == AEXPR_IN) {
        char *leftcol, *lefttable;
        char *rightcol, *righttable;
        bool leftiscol = false, rightiscol = false, userfound = false;
        bool leftaexpr = false, rightaexpr = false;
        
        // It is possible to have this odd error under some circumstances.
        if (recAExpr->name->length == 0)
            return 0;
        
        // If the left column is our user key column, that's a good sign.
        // Let's just hope we're not joining with some other column.
        if (recAExpr->lexpr && nodeTag(recAExpr->lexpr) == T_ColumnRef) {
            ColumnRef *leftcr = (ColumnRef*) recAExpr->lexpr;
            
            leftiscol = true;
            leftcol = getTableRef(leftcr,&lefttable);
            if (strcmp(leftcol,userkey) == 0)
                userfound = true;
        }
        
        // If the right column is our user key column, return right away.
        if (recAExpr->rexpr && nodeTag(recAExpr->rexpr) == T_ColumnRef) {
            ColumnRef *rightcr = (ColumnRef*) recAExpr->rexpr;
            
            rightiscol = true;
            rightcol = getTableRef(rightcr,&righttable);
            if (strcmp(rightcol,userkey) == 0)
                userfound = true;
        }
        
        // If this OP/IN doesn't involve the user key at all, then we'll
        // replace it with a TRUE boolean constant. Likewise, if we're
        // equating it with some other column, that's not useful to us,
        // so we'll again replace it with a TRUE constant.
        if (leftiscol && rightiscol)
            return -1;
        else if (!leftiscol && !rightiscol)
            return 0;
        
        // If this OP has more A_Exprs under it, we need to recurse and
        // see what's in them.
        if (recAExpr->lexpr && nodeTag(recAExpr->lexpr) == T_A_Expr) {
            leftaexpr = true;
            leftresult = userWhereOp(recAExpr->lexpr,userkey);
        }
        if (recAExpr->rexpr && nodeTag(recAExpr->rexpr) == T_A_Expr) {
            rightaexpr = true;
            rightresult = userWhereOp(recAExpr->rexpr, userkey);
        }
        
        // Any AExprs?
        if (leftaexpr || rightaexpr) {
            // If we found another column anywhere, the whole thing is
            // useless.
            if (leftresult < 0 || rightresult < 0)
                return -1;
            
            // If we didn't find the user column, it's similarly useless.
            if (leftresult == 0 && rightresult == 0)
                return 0;
            
            // Otherwise, if we found the user key, we can use this item.
            if (userfound) {
                return 1;
            }
            else
                return 0;
        }
    }
    // If our expression is an OP or IN, then do the actual check.
    if (recAExpr->kind == AEXPR_OP || recAExpr->kind == AEXPR_IN) {
        char *leftcol, *lefttable;
        char *rightcol, *righttable;
        bool leftiscol = false, rightiscol = false, userfound = false;
        bool leftaexpr = false, rightaexpr = false;
        
        // It is possible to have this odd error under some circumstances.
        if (recAExpr->name->length == 0)
            return 0;
        
        // If this OP has more A_Exprs under it, we need to recurse and
        // see what's in them.
        if (recAExpr->lexpr && nodeTag(recAExpr->lexpr) == T_A_Expr) {
            leftaexpr = true;
            leftresult = userWhereOp(recAExpr->lexpr,userkey);
        }
        if (recAExpr->rexpr && nodeTag(recAExpr->rexpr) == T_A_Expr) {
            rightaexpr = true;
            rightresult = userWhereOp(recAExpr->rexpr, userkey);
        }
        
        // Any AExprs?
        if (leftaexpr || rightaexpr) {
            // If we found another column anywhere, the whole thing is
            // useless.
            if (leftresult < 0 || rightresult < 0)
                return -1;
            
            // If we found the user column, though, make a note.
            if (leftresult == 1 || rightresult == 1)
                userfound = true;
        }
        
        // If at least one isn't an A_Expr, then we check to see if either
        // is a ColumnRef.
        
        // If the left column is our user key column, that's a good sign.
        // Let's just hope we're not joining with some other column.
        if (recAExpr->lexpr && nodeTag(recAExpr->lexpr) == T_ColumnRef) {
            ColumnRef *leftcr = (ColumnRef*) recAExpr->lexpr;
            
            leftiscol = true;
            leftcol = getTableRef(leftcr,&lefttable);
            
            if (strcmp(leftcol,userkey) == 0)
                userfound = true;
            else
                return -1;
        }
        
        // If the right column is our user key column, return right away.
        if (recAExpr->rexpr && nodeTag(recAExpr->rexpr) == T_ColumnRef) {
            ColumnRef *rightcr = (ColumnRef*) recAExpr->rexpr;
            
            rightiscol = true;
            rightcol = getTableRef(rightcr,&righttable);
            
            if (strcmp(rightcol,userkey) == 0)
                userfound = true;
            else
                return -1;
        }
        
        // If both items are columns, that can't possibly be good.
        if (leftiscol && rightiscol)
            return 0;
        
        // Otherwise, if we found the user key, we can use this item.
        if (userfound)
            return 1;
        else
            return 0;
    }
    // Recurse in a similar manner if this is an AND/OR/NOT.
    else if (recAExpr->kind == AEXPR_AND || recAExpr->kind == AEXPR_OR) {
        leftresult = userWhereOp(recAExpr->lexpr,userkey);
        rightresult = userWhereOp(recAExpr->rexpr,userkey);
        
        if (leftresult < 0 || rightresult < 0)
            return -1;
        else if (leftresult == 0 && rightresult == 0)
            return 0;
        else
            return 1;
    } else if (recAExpr->kind == AEXPR_NOT) {
        return userWhereOp(recAExpr->rexpr,userkey);
    }
    
    // Return 0 by default.
    return 0;
}

/*
 * userWhereClause -
 *	  A function to retrieve a modified WHERE clause, where all
 *	  elements pertain only to the user key. Any elements that do
 *	  not pertain to the user key are replaced with TRUE; this
 *	  makes the user filtering inexact in complex cases, but it
 *	  should be good enough for us. Returns NULL if there are no
 *	  elements pertaining to the user key.
 */
static Node*
userWhereClause(Node* whereClause, char *userkey) {
    A_Expr *recAExpr;
    
    if (!whereClause)
        return NULL;
    
    // Turns out this isn't necessarily an A_Expr.
    if (nodeTag(whereClause) != T_A_Expr)
        return NULL;
    
    recAExpr = (A_Expr*) whereClause;
    
    // If our expression is an OP or IN, then do the actual check.
    if (recAExpr->kind == AEXPR_OP || recAExpr->kind == AEXPR_IN) {
        char *leftcol, *lefttable;
        char *rightcol, *righttable;
        bool leftiscol = false, rightiscol = false, userfound = false;
        bool leftaexpr = false, rightaexpr = false;
        int leftresult = 0, rightresult = 0;
        
        // It is possible to have this odd error under some circumstances.
        if (recAExpr->name->length == 0)
            return NULL;
        
        // If this OP has more A_Exprs under it, we need to recurse and
        // see what's in them.
        if (recAExpr->lexpr && nodeTag(recAExpr->lexpr) == T_A_Expr) {
            leftaexpr = true;
            leftresult = userWhereOp(recAExpr->lexpr,userkey);
        }
        if (recAExpr->rexpr && nodeTag(recAExpr->rexpr) == T_A_Expr) {
            rightaexpr = true;
            rightresult = userWhereOp(recAExpr->rexpr, userkey);
        }
        
        // Any AExprs?
        if (leftaexpr || rightaexpr) {
            // If we found another column anywhere, the whole thing is
            // useless.
            if (leftresult < 0 || rightresult < 0)
                return makeTrueConst();
            
            // If we found the user column, though, make a note.
            if (leftresult == 1 || rightresult == 1)
                userfound = true;
        }
        
        // If at least one isn't an A_Expr, then we check to see if either
        // is a ColumnRef.
        
        // If the left column is our user key column, that's a good sign.
        // Let's just hope we're not joining with some other column.
        if (recAExpr->lexpr && nodeTag(recAExpr->lexpr) == T_ColumnRef) {
            ColumnRef *leftcr = (ColumnRef*) recAExpr->lexpr;
            
            leftiscol = true;
            leftcol = getTableRef(leftcr,&lefttable);
            
            if (strcmp(leftcol,userkey) == 0)
                userfound = true;
            else
                return makeTrueConst();
        }
        
        // If the right column is our user key column, return right away.
        if (recAExpr->rexpr && nodeTag(recAExpr->rexpr) == T_ColumnRef) {
            ColumnRef *rightcr = (ColumnRef*) recAExpr->rexpr;
            
            rightiscol = true;
            rightcol = getTableRef(rightcr,&righttable);
            
            if (strcmp(rightcol,userkey) == 0)
                userfound = true;
            else
                return makeTrueConst();
        }
        
        // If both items are columns, that can't possibly be good.
        if (leftiscol && rightiscol)
            return makeTrueConst();
        
        // Otherwise, if we found the user key, we can use this item.
        if (userfound)
            return (Node *) recAExpr;
        else
            return makeTrueConst();
    }
    // Recurse if this is an AND/OR/NOT.
    else if (recAExpr->kind == AEXPR_AND || recAExpr->kind == AEXPR_OR) {
        recAExpr->lexpr = userWhereClause(recAExpr->lexpr,userkey);
        recAExpr->rexpr = userWhereClause(recAExpr->rexpr,userkey);
    } else if (recAExpr->kind == AEXPR_NOT) {
        recAExpr->rexpr = userWhereClause(recAExpr->rexpr,userkey);
    }
    
    
    // Return the expression.
    return (Node *) recAExpr;
}

/*
 * userWhereClause -
 *	  A function to transform a modified WHERE clause.
 */
void
userWhereTransform(ParseState *pstate, Node* recommendClause) {
    RecommendInfo *recInfo;
    Node *userWhere;
    
    if (!recommendClause)
        return;
    
    recInfo = (RecommendInfo*) recommendClause;
    userWhere = recInfo->attributes->userWhereClause;
    if (userWhere) {
        userWhere = transformExpr(pstate, userWhere);
        userWhere = coerce_to_boolean(pstate, userWhere, "USER_WHERE");
    }
    recInfo->attributes->userWhereClause = userWhere;
    //debug
    //printf("type of att userwhere: %u\n", userWhere->type);
}

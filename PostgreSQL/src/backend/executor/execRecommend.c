/*-------------------------------------------------------------------------
 *
 * execRecommend.c
 *	  This code provides support for generalized recommender scans.
 *	  The code acts very similarly to ExecScan, and uses existing
 *	  methods to do the heavy lifting. We've just added support for
 *	  recommenders.
 *
 * Portions Copyright (c) 2012-2013, University of Minnesota
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execRecommend.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/execRecommend.h"
#include "executor/nodeSeqscan.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/recathon.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#define DEBUG 0
#define VERBOSE 0

static TupleTableSlot* ExecIndexRecommend(RecScanState *recnode,
					 ExecScanAccessMtd accessMtd,
					 ExecScanRecheckMtd recheckMtd);
static TupleTableSlot* ExecFilterRecommend(RecScanState *recnode,
					 ExecScanAccessMtd accessMtd,
					 ExecScanRecheckMtd recheckMtd);
static void applyItemSim(RecScanState *recnode, char *itemmodel);

/*
 * ExecRecFetch -- fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.	If we aren't, just execute
 * the access method's next-tuple routine.
 *
 * This method is identical to ExecScanFetch, we just want to avoid
 * possible duplicate function issues.
 */
static inline TupleTableSlot *
ExecRecFetch(ScanState *node,
			  ExecScanAccessMtd accessMtd,
			  ExecScanRecheckMtd recheckMtd)
{
	EState	   *estate = node->ps.state;

	if (estate->es_epqTuple != NULL)
	{
		/*
		 * We are inside an EvalPlanQual recheck.  Return the test tuple if
		 * one is available, after rechecking any access-method-specific
		 * conditions.
		 */
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		Assert(scanrelid > 0);
		if (estate->es_epqTupleSet[scanrelid - 1])
		{
			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Return empty slot if we already returned a tuple */
			if (estate->es_epqScanDone[scanrelid - 1])
				return ExecClearTuple(slot);
			/* Else mark to remember that we shouldn't return more */
			estate->es_epqScanDone[scanrelid - 1] = true;

			/* Return empty slot if we haven't got a test tuple */
			if (estate->es_epqTuple[scanrelid - 1] == NULL)
				return ExecClearTuple(slot);

			/* Store test tuple in the plan node's scan slot */
			ExecStoreTuple(estate->es_epqTuple[scanrelid - 1],
						   slot, InvalidBuffer, false);

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */

			return slot;
		}
	}

	/*
	 * Run the node-type-specific access method function to get the next tuple
	 */
	return (*accessMtd) (node);
}

/* ----------------------------------------------------------------
 *		ExecRecommend
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple in the direction specified
 *		in the global variable ExecDirection.
 *		The access method returns the next tuple and execRecommend() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		This version of the function usually will not obtain tuples from
 *		the table. Instead, we'll fetch only one tuple just so we can use
 *		its TupleDesc to create entirely new tuples, then fill them in with
 *		synthetic data.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecRecommend(RecScanState *recnode,
		 ExecScanAccessMtd accessMtd,	/* function returning a tuple */
		 ExecScanRecheckMtd recheckMtd)
{
	/* We hand the legwork off to one of two functions. */
	if (recnode->useRecView)
		return ExecIndexRecommend(recnode,accessMtd,recheckMtd);
	else
		return ExecFilterRecommend(recnode,accessMtd,recheckMtd);
}

/*
 * ExecIndexRecommend
 *
 * This function obtains data directly from the RecView, which we
 * assume is populated with predictions for this user.
 */
static TupleTableSlot*
ExecIndexRecommend(RecScanState *recnode,
					 ExecScanAccessMtd accessMtd,
					 ExecScanRecheckMtd recheckMtd)
{
	ExprContext *econtext;
	List	   *qual;
	ProjectionInfo *projInfo;
	ExprDoneCond isDone;
	TupleTableSlot *resultSlot;
	ScanState *node;
	AttributeInfo *attributes;

	node = recnode->subscan;
	attributes = (AttributeInfo*) recnode->attributes;

	/*
	 * Fetch data from node
	 */
	qual = node->ps.qual;
	projInfo = node->ps.ps_ProjInfo;
	econtext = node->ps.ps_ExprContext;

	/*
	 * Check to see if we're still projecting out tuples from a previous scan
	 * tuple (because there is a function-returning-set in the projection
	 * expressions).  If so, try to project another one.
	 */
	if (node->ps.ps_TupFromTlist)
	{
		Assert(projInfo);		/* can't get here if not projecting */
		resultSlot = ExecProject(projInfo, &isDone);
		if (isDone == ExprMultipleResult)
			return resultSlot;
		/* Done with that source tuple... */
		node->ps.ps_TupFromTlist = false;
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a scan tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * get a tuple from the access method.	Loop until we obtain a tuple that
	 * passes the qualification.
	 */
	for (;;)
	{
		TupleTableSlot *slot;
		int userID;
		bool recQual = true;

		CHECK_FOR_INTERRUPTS();

		slot = recnode->ss.ps.ps_ResultTupleSlot;

		/* If we're using the RecView, we're going to fetch a new
		 * tuple every time. */
		slot = ExecRecFetch(node, accessMtd, recheckMtd);

		/* If the slot is null now, then we've run out of tuples
		 * to return, so we're done. */
		if (TupIsNull(slot))
		{
			if (projInfo)
				return ExecClearTuple(projInfo->pi_slot);
			else
				return slot;
		}

		/*
		 * Before we check the qualifications, we're going to manually check
		 * to see that the tuple matches the provided user ID, because this
		 * is always necessary and it's easier than messing with the target
		 * list.
		 */

		/* First, we'll make sure we're dealing with the right user. */
		userID = getTupleInt(slot,attributes->userkey);

		/* How we could fail to find the user ID, I don't know. */
		if (userID < 0)
			elog(ERROR, "user ID column not found");
		/* If this tuple doesn't match the user ID, just skip it
		 * and move on. */
		if (userID != attributes->userID)
			recQual = false;

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * check that the current tuple satisfies the qual-clause
		 *
		 * check for non-nil qual here to avoid a function call to ExecQual()
		 * when the qual is nil ... saves only a few cycles, but they add up
		 * ...
		 *
		 * we also make sure that the tuple passes our recommender qual
		 */
		if (recQual && (!qual || ExecQual(qual, econtext, false)))
		{
			/*
			 * Found a satisfactory scan tuple.
			 */
			if (projInfo)
			{
				/*
				 * Form a projection tuple, store it in the result tuple slot
				 * and return it --- unless we find we can project no tuples
				 * from this scan tuple, in which case continue scan.
				 */
				resultSlot = ExecProject(projInfo, &isDone);
				if (isDone != ExprEndResult)
				{
					node->ps.ps_TupFromTlist = (isDone == ExprMultipleResult);

					return resultSlot;
				}
			}
			else
			{
				/*
				 * Here, we aren't projecting, so just return scan tuple.
				 */

				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}

}

/*
 * ExecFilterRecommend
 *
 * This function just borrows a tuple descriptor from the RecView,
 * but we create the data ourselves through various means.
 */
static TupleTableSlot*
ExecFilterRecommend(RecScanState *recnode,
					 ExecScanAccessMtd accessMtd,
					 ExecScanRecheckMtd recheckMtd)
{
	ExprContext *econtext;
	List	   *qual;
	ProjectionInfo *projInfo;
	ExprDoneCond isDone;
	TupleTableSlot *resultSlot;
	ScanState *node;
	AttributeInfo *attributes;

	node = recnode->subscan;
	attributes = (AttributeInfo*) recnode->attributes;

	/*
	 * Fetch data from node
	 */
	qual = node->ps.qual;
	projInfo = node->ps.ps_ProjInfo;
	econtext = node->ps.ps_ExprContext;

	/*
	 * Check to see if we're still projecting out tuples from a previous scan
	 * tuple (because there is a function-returning-set in the projection
	 * expressions).  If so, try to project another one.
	 */
	if (node->ps.ps_TupFromTlist)
	{
		Assert(projInfo);		/* can't get here if not projecting */
		resultSlot = ExecProject(projInfo, &isDone);
		if (isDone == ExprMultipleResult)
			return resultSlot;
		/* Done with that source tuple... */
		node->ps.ps_TupFromTlist = false;
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a scan tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * get a tuple from the access method.	Loop until we obtain a tuple that
	 * passes the qualification.
	 */
	for (;;)
	{
		TupleTableSlot *slot;
		int natts, i, userID, itemID, itemindex;

		CHECK_FOR_INTERRUPTS();

		slot = recnode->ss.ps.ps_ResultTupleSlot;

		/*
		 * If we've exhausted our item list, then we're totally
		 * finished. We set a flag for this. It's possible that
		 * we'll be in the inner loop of a join, through poor
		 * planning, so we'll reset the appropriate data in case
		 * we have to do this again, though our JoinRecommend
		 * should assure this doesn't happen.
		 */
		if (recnode->finished) {
			recnode->finished = false;
			recnode->itemNum = 0;
			return NULL;
		}

		/* We're only going to fetch one tuple and store its tuple
		 * descriptor. We can use this tuple descriptor to make as
		 * many new tuples as we want. */
		if (recnode->base_slot == NULL) {
			slot = ExecRecFetch(node, accessMtd, recheckMtd);
			recnode->base_slot = slot->tts_tupleDescriptor;
		}

		/* Create a new slot to operate on. */
		slot = MakeSingleTupleTableSlot(CreateTupleDescCopy(recnode->base_slot));
		slot->tts_isempty = false;

		/*
		 * We now have a blank tuple slot that we need to fill with data.
		 * We already have the user ID, we can quickly fetch the next
		 * item ID, and we need to calculate the RecScore.
		 */

		userID = -1; itemID = -1;
		natts = slot->tts_tupleDescriptor->natts;

		/* We'll replace the user ID and item ID. */
		userID = attributes->userID;
		itemindex = recnode->itemNum;
		itemID = recnode->itemList[itemindex];

		/* Plug in the data, marking those columns full. */
		for (i = 0; i < natts; i++) {
			char* col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
			if (strcmp(col_name,attributes->userkey) == 0) {
				slot->tts_values[i] = Int32GetDatum(userID);
				slot->tts_isnull[i] = false;
				slot->tts_nvalid++;
			} else if (strcmp(col_name,attributes->itemkey) == 0) {
				slot->tts_values[i] = Int32GetDatum(itemID);
				slot->tts_isnull[i] = false;
				slot->tts_nvalid++;
			}
		}

		/* It's possible our filter criteria involves the RecScore somehow.
		 * If that's the case, we need to calculate it before we do the
		 * qual filtering. Also, if we're doing a JoinRecommend, we should
		 * not calculate the RecScore in this node. */
		if (attributes->opType == OP_NOFILTER)
			applyRecScore(recnode, slot, itemID);

		/* Move onto the next item, for next time. */
		recnode->itemNum++;
		if (recnode->itemNum >= recnode->totalItems)
			recnode->finished = true;

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * check that the current tuple satisfies the qual-clause
		 *
		 * check for non-nil qual here to avoid a function call to ExecQual()
		 * when the qual is nil ... saves only a few cycles, but they add up
		 * ...
		 */
		if (!qual || ExecQual(qual, econtext, false))
		{
			/*
			 * Found a satisfactory scan tuple. This is usually when
			 * we will calculate and apply the RecScore.
			 */
			if (attributes->opType == OP_FILTER)
				applyRecScore(recnode, slot, itemID);

			if (projInfo)
			{
				/*
				 * Form a projection tuple, store it in the result tuple slot
				 * and return it --- unless we find we can project no tuples
				 * from this scan tuple, in which case continue scan.
				 */
				resultSlot = ExecProject(projInfo, &isDone);
				if (isDone != ExprEndResult)
				{
					node->ps.ps_TupFromTlist = (isDone == ExprMultipleResult);

					return resultSlot;
				}
			}
			else
			{
				/*
				 * Here, we aren't projecting, so just return scan tuple.
				 */

				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}

}

/*
 * applyItemSim
 *
 * This function is one of the first steps in item-based CF prediction
 * generation. We take the ratings this user has generated and we
 * apply those similarity values to our tentative ratings.
 */
static void
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

/*
 * ExecReScanRecScan
 *
 * This decides what kind of ReScan method to use. We should only be
 * considering a SeqScan; we built in a failsafe, though anything else
 * should be impossible due to how we handled the planning.
 */
void
ExecReScanRecScan(RecScanState *node)
{
	switch (nodeTag(node->subscan))
	{
		case T_SeqScanState:
			ExecReScanSeqScan((SeqScanState *) node->subscan);
			break;

		default:
			elog(ERROR, "invalid RecScan subscan type: %d", (int) nodeTag(node->subscan));
			break;
	}
}

/*
 * ExecRecMarkPos
 *
 * This decides what kind of MarkPos method to use. We should only be
 * considering a SeqScan; we built in a failsafe, though anything else
 * should be impossible due to how we handled the planning.
 */
void
ExecRecMarkPos(RecScanState *node)
{
	switch (nodeTag(node->subscan))
	{
		case T_SeqScanState:
			ExecSeqMarkPos((SeqScanState *) node->subscan);
			break;

		default:
			/* don't make hard error unless caller asks to restore... */
			elog(DEBUG2, "invalid RecScan subscan type: %d", (int) nodeTag(node->subscan));
			break;
	}
}

/*
 * ExecRecRestrPos
 *
 * This decides what kind of RestrPos method to use. We should only be
 * considering a SeqScan; we built in a failsafe, though anything else
 * should be impossible due to how we handled the planning.
 */
void
ExecRecRestrPos(RecScanState *node)
{
	switch (nodeTag(node->subscan))
	{
		case T_SeqScanState:
			ExecSeqRestrPos((SeqScanState *) node->subscan);
			break;

		default:
			elog(ERROR, "invalid RecScan subscan type: %d", (int) nodeTag(node->subscan));
			break;
	}
}

/*
 * ExecInitRecScan
 *
 * This function will initialize the recommender information, as well as
 * whatever scan type we have.
 */
RecScanState *
ExecInitRecScan(RecScan *node, EState *estate, int eflags)
{
	int i;
	RecScanState *recstate;
	ScanState *scanstate;
	AttributeInfo *attributes;
	NodeTag type;
	// Query objects.
	char *querystring;
	QueryDesc *queryDesc;
	PlanState *planstate;
	TupleTableSlot *hslot;
	MemoryContext recathoncontext;

	recstate = (RecScanState*) makeNode(RecScanState);
	type = node->subscan->plan.type;
	node->subscan->plan = node->scan.plan;
	node->subscan->scanrelid = node->scan.scanrelid;
	node->subscan->plan.type = type;

	/* Before we do the Init, we need to update the subscan plan. We utilize
	 * a SeqScan as our subscan, but we have a failsafe just in case. */
	switch(nodeTag(node->subscan)) {
		case T_SeqScan:
			scanstate = (ScanState*) ExecInitSeqScan((SeqScan *) node->subscan, estate, eflags);
			break;
		default:
			elog(ERROR, "invalid RecScan subscan type: %d", (int) nodeTag(node->subscan));
			scanstate = NULL;		/* keep compiler quiet */
			break;
	}

	/* Plug in our scan state. */
	recstate->subscan = scanstate;
	/* Now for regular scan info. Kind of redundant but not much we can do. */
	recstate->ss.ps = scanstate->ps;
	recstate->ss.ss_currentRelation = scanstate->ss_currentRelation;
	recstate->ss.ss_currentScanDesc = scanstate->ss_currentScanDesc;
	recstate->ss.ss_ScanTupleSlot = scanstate->ss_ScanTupleSlot;
	/* And we substitute the tag. */
	recstate->ss.ps.type = T_RecScanState;

	/* With the scan taken care of, we need to initialize the actual recommender. */

	/* Copy information right from the recommender. */
	recstate->attributes = (Node*) ((RecommendInfo*)node->recommender)->attributes;
	attributes = (AttributeInfo *) recstate->attributes;

	/* Code for a future version of Recathon. */
/*	switch(attributes->cellType) {
		case CELL_ALPHA:
			{
				char *heavystring;

				heavystring = (char*) palloc(1024*sizeof(char));
				sprintf(heavystring,"select userid from recathonheavyusers where userid = %d;",
						attributes->userID);
				queryDesc = recathon_queryStart(heavystring, &recathoncontext);
				planstate = queryDesc->planstate;

				hslot = ExecProcNode(planstate);
				if (TupIsNull(hslot))
					recstate->useRecView = false;
				else
					recstate->useRecView = true;

				recathon_queryEnd(queryDesc, recathoncontext);
				pfree(heavystring);
			}

			break;
		case CELL_GAMMA:
			recstate->useRecView = true;
			break;
		case CELL_BETA:
			recstate->useRecView = false;
			break;
		default:
			elog(ERROR, "unrecognized cell type: %d",
				(int) ((RecommendInfo*)node->recommender)->attributes->cellType);
			break;
	}
*/
	/* We need to obtain a full list of all the items we need to calculate
	 * a score for. First count, then query. */
	querystring = (char*) palloc(1024*sizeof(char));
	sprintf(querystring,"select count(%s) from %s where %s not in (select distinct %s from %s where %s = %d);",
		attributes->itemkey,attributes->itemtable,attributes->itemkey,
		attributes->itemkey,attributes->ratingtable,attributes->userkey,
		attributes->userID);
//printf("%s\n",querystring);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;
	hslot = ExecProcNode(planstate);
	recstate->totalItems = getTupleInt(hslot,"count");
	recathon_queryEnd(queryDesc,recathoncontext);

	/* It's highly unlikely, but possible, that someone has rated literally
	 * every item. */
	if (recstate->totalItems <= 0)
		elog(ERROR, "user %d has rated all items, no predictions to be made",
			attributes->userID);

	recstate->itemList = (int*) palloc(recstate->totalItems*sizeof(int));
	recstate->itemNum = 0;

	/* Now for the actual query. */
	sprintf(querystring,"select * from %s where %s not in (select distinct %s from %s where %s = %d) order by %s;",
		attributes->itemtable,attributes->itemkey,
		attributes->itemkey,attributes->ratingtable,attributes->userkey,
		attributes->userID,attributes->itemkey);
//printf("%s\n",querystring);
	queryDesc = recathon_queryStart(querystring,&recathoncontext);
	planstate = queryDesc->planstate;

	i = 0;
	for (;;) {
		int currentItem;

		hslot = ExecProcNode(planstate);
		if (TupIsNull(hslot)) break;

		currentItem = getTupleInt(hslot,attributes->itemkey);

		recstate->itemList[i] = currentItem;
		i++;
		if (i >= recstate->totalItems) break;
	}
	recathon_queryEnd(queryDesc,recathoncontext);

	/* Quick error protection. */
	recstate->totalItems = i;
	if (recstate->totalItems <= 0)
		elog(ERROR, "user %d has rated all items, no predictions to be made",
			attributes->userID);

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
				attributes->ratingtable,attributes->userkey,attributes->userID);
			queryDesc = recathon_queryStart(querystring,&recathoncontext);
			planstate = queryDesc->planstate;
			hslot = ExecProcNode(planstate);
			recstate->totalRatings = getTupleInt(hslot,"count");
			recathon_queryEnd(queryDesc,recathoncontext);

			/* It's possible that someone has rated no items. */
			if (recstate->totalRatings <= 0)
				elog(ERROR, "user %d has rated no items, no predictions can be made",
					attributes->userID);

			recstate->ratedTable = hashCreate(recstate->totalRatings);

			/* Now to acquire the actual ratings. */
			sprintf(querystring,"select * from %s where %s = %d order by %s;",
				attributes->ratingtable,attributes->userkey,
				attributes->userID,attributes->itemkey);
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
				currentRating = getTupleFloat(hslot,attributes->ratingval);

				newItem = (GenRating*) palloc(sizeof(GenRating));
				newItem->ID = currentItem;
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
			if (recstate->totalRatings <= 0)
				elog(ERROR, "user %d has rated no items, no predictions can be made",
					attributes->userID);

			/* The pending list is all of the items we have yet to
			 * calculate ratings for. We need to maintain partial
			 * scores and similarity sums for each one. */
			recstate->pendingTable = hashCreate(recstate->totalItems);
			for (i = 0; i < recstate->totalItems; i++) {
				GenRating *newItem;

				newItem = (GenRating*) palloc(sizeof(GenRating));
				newItem->ID = recstate->itemList[i];
				newItem->score = 0.0;
				newItem->totalSim = 0.0;
				newItem->next = NULL;
				hashAdd(recstate->pendingTable, newItem);
			}

			/* With another function, we apply the ratings and similarities
			 * from the rated items to the unrated ones. It's good to get
			 * this done early, as this will allow the operator to be
			 * non-blocking, which is important. */
			applyItemSim(recstate, attributes->recModelName);

			break;
		case userCosCF:
		case userPearCF:
			/* The first thing we'll do is obtain the average rating. */
			sprintf(querystring,"select avg(%s) as average from %s where %s = %d;",
				attributes->ratingval,attributes->ratingtable,
				attributes->userkey,attributes->userID);
			queryDesc = recathon_queryStart(querystring,&recathoncontext);
			planstate = queryDesc->planstate;

			hslot = ExecProcNode(planstate);
			recstate->average = getTupleFloat(hslot,"average");
			recathon_queryEnd(queryDesc,recathoncontext);

			/* Next, we need to store this user's similarity model
			 * in a hash table for easier access. We base the table on
			 * the number of items we have to rate - a close enough
			 * approximation that we won't have much trouble. */
			recstate->simTable = hashCreate(recstate->totalItems);

			/* We need to find the entire similarity table for this
			 * user, which will be in two parts. Here's the first. */
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
				newUser->totalSim = currentSim;
				newUser->next = NULL;
				hashAdd(recstate->simTable, newUser);
			}
			recathon_queryEnd(queryDesc,recathoncontext);

			break;
		/* If this is a SVD recommender, we can pre-obtain the user features,
		 * which stay fixed, and cut the I/O time in half. */
		case SVD:
			recstate->userFeatures = (float*) palloc(50*sizeof(float));
			for (i = 0; i < 50; i++)
				recstate->userFeatures[i] = 0;
			sprintf(querystring,"select * from %s where users = %d;",
				attributes->recModelName,attributes->userID);
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
			break;
		default:
			elog(ERROR, "invalid recommendation method in ExecInitRecScan()");
	}

	recstate->finished = false;
	recstate->base_slot = NULL;

	/* Lastly, we also need to increase the query counter for this particular table. */
	sprintf(querystring,"update %sindex set querycounter = querycounter+1",attributes->recName);
	if (attributes->numAtts > 0) strncat(querystring," where ",7);
	// Add info to the WHERE clause, based on the attributes.
	for (i = 0; i < attributes->numAtts; i++) {
		char add_string[128];
		sprintf(add_string,"%s='%s'",attributes->attNames[i],
			attributes->attValues[i]);
		strncat(querystring,add_string,strlen(add_string));
		if (i+1 < attributes->numAtts)
			strncat(querystring," and ",5);
	}
	strncat(querystring,";",1);
	if (VERBOSE) printf("%s\n",querystring);
	recathon_queryExecute(querystring);
	pfree(querystring);

	/* In the current version of Recathon, we will not be using
	 * IndexRecommend at all.  */
	if (attributes->opType == OP_INDEX)
		recstate->useRecView = true;
	else
		recstate->useRecView = false;

	return recstate;
}

/*
 * ExecRecScan
 *
 * In order to fetch our initial tuple, we need to utilize the standard SeqScan fetch
 * method. The problem is, those parameters are static methods located in another file.
 * Our solution is to utilize another function that we place in nodeSeqScan.c, which will
 * then redirect themselves back here to ExecRecommend. It's pretty roundabout, but our
 * hands are a little tied. This messes with existing code the least.
 *
 * As before, we have a failsafe to make sure we're using SeqScan.
 */
TupleTableSlot *
ExecRecScan(RecScanState *node)
{
	switch(nodeTag(node->subscan)) {
		case T_SeqScanState:
			return ExecSeqRecScan(node);
		default:
			elog(ERROR, "invalid RecScan subscan type: %d", (int) nodeTag(node->subscan));
	}
	return NULL;		/* keep compiler quiet */
}

/*
 * ExecEndRecScan
 *
 * Ends the SeqScan and then does some additional things.
 */
void
ExecEndRecScan(RecScanState *node)
{
	/* End the normal scan. */
	switch(nodeTag(node->subscan)) {
		case T_SeqScanState:
			ExecEndSeqScan((SeqScanState *) node->subscan);
			break;

		default:
			elog(ERROR, "invalid RecScan subscan type: %d", (int) nodeTag(node->subscan));
			break;
	}

	/* Now for extra stuff. */
	if (node->itemList)
		pfree(node->itemList);
	if (node->userFeatures)
		pfree(node->userFeatures);
}

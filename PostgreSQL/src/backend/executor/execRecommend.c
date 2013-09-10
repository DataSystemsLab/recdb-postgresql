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
		int natts, i, userID, userindex, itemID, itemindex;

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
			recnode->userNum = 0;
			recnode->itemNum = 0;
			return NULL;
		}

		/* We're only going to fetch one tuple and store its tuple
		 * descriptor. We can use this tuple descriptor to make as
		 * many new tuples as we want. */
		if (recnode->base_slot == NULL) {
			slot = ExecRecFetch(node, accessMtd, recheckMtd);
			recnode->base_slot = CreateTupleDescCopy(slot->tts_tupleDescriptor);
		}

		/* Create a new slot to operate on. */
		slot = MakeSingleTupleTableSlot(recnode->base_slot);
		slot->tts_isempty = false;

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/* Mark all slots as usable. */
		natts = slot->tts_tupleDescriptor->natts;
		for (i = 0; i < natts; i++) {
			/* Mark slot. */
			slot->tts_values[i] = Int32GetDatum(0);
			slot->tts_isnull[i] = false;
			slot->tts_nvalid++;
		}

		/* While we're here, record what tuple attributes
		 * correspond to our key columns. This will save
		 * us unnecessary strcmp functions. */
		if (recnode->useratt < 0) {
			for (i = 0; i < natts; i++) {
				char* col_name = slot->tts_tupleDescriptor->attrs[i]->attname.data;
//printf("%s\n",col_name);
				if (strcmp(col_name,attributes->userkey) == 0)
					recnode->useratt = i;
				else if (strcmp(col_name,attributes->itemkey) == 0)
					recnode->itematt = i;
				else if (strcmp(col_name,attributes->eventval) == 0)
					recnode->eventatt = i;

				/* Mark slot. */
				slot->tts_isnull[i] = false;
				slot->tts_nvalid++;
			}
		}

		/*
		 * We now have a problem: we need to create prediction structures
		 * for a user before we do filtering, so that we can have a proper
		 * item list. But we also need to filter before creating those
		 * structures, so we don't end up taking forever with it. The
		 * solution is to filter twice.
		 */
		userID = -1; itemID = -1;

		/* First, replace the user ID. */
		userindex = recnode->userNum;
		userID = recnode->userList[userindex];

		/*
		 * We now have a blank tuple slot that we need to fill with data.
		 * We have a working user ID, but not a valid item list. We'd like to
		 * use the filter to determine if this is a good user, but we can't
		 * do that without an item, in many cases. The solution is to add in
		 * dummy items, then compare it against the filter. If a given user ID
		 * doesn't make it past the filter with any item ID, then that user is
		 * being filtered out, and we'll move on to the next.
		 */
		if (recnode->newUser) {
			recnode->fullItemNum = 0;
			itemindex = recnode->fullItemNum;
			itemID = recnode->fullItemList[itemindex];

			slot->tts_values[recnode->useratt] = Int32GetDatum(userID);
			slot->tts_values[recnode->itematt] = Int32GetDatum(itemID);
			slot->tts_values[recnode->eventatt] = Int32GetDatum(-1);

			/* We have a preliminary slot - let's test it. */
			while (qual && !ExecQual(qual, econtext, false)) {
				/* We failed the test. Try the next item. */
				recnode->fullItemNum++;
				if (recnode->fullItemNum >= recnode->fullTotalItems) {
					/* If we've reached the last item, move onto the next user.
					 * If we've reached the last user, we're done. */
					InstrCountFiltered1(node, recnode->fullTotalItems);
					recnode->userNum++;
					recnode->newUser = true;
					recnode->fullItemNum = 0;
					if (recnode->userNum >= recnode->totalUsers) {
						recnode->userNum = 0;
						recnode->itemNum = 0;
						return NULL;
					}
					userindex = recnode->userNum;
					userID = recnode->userList[userindex];
				}

				itemindex = recnode->fullItemNum;
				itemID = recnode->fullItemList[itemindex];
				slot->tts_values[recnode->useratt] = Int32GetDatum(userID);
				slot->tts_values[recnode->itematt] = Int32GetDatum(itemID);
			}

			/* If we get here, then we found a user who will be actually
			 * returned in the results. */
		}

		/* Mark the user ID and index. */
		attributes->userID = userID;
		recnode->userindex = recnode->userNum;

		/* With the user ID determined, we need to investigate and see
		 * if this is a new user. If so, attempt to create prediction
		 * data structures, or report that this user is invalid. We have
		 * to do this here, so we can establish the item list. */
		if (recnode->newUser) {
			recnode->validUser = prepUserForRating(recnode,userID);
			recnode->newUser = false;
		}

		/* Now replace the item ID, if the user is valid. Otherwise,
		 * leave the item ID as is, as it doesn't matter what it is. */
		itemindex = recnode->itemNum;
		if (recnode->validUser)
			itemID = recnode->itemList[itemindex];

		/* Plug in the data, marking those columns full. We also need to
		 * mark the rating column with something temporary. */
		slot->tts_values[recnode->useratt] = Int32GetDatum(userID);
		slot->tts_values[recnode->itematt] = Int32GetDatum(itemID);
		slot->tts_values[recnode->eventatt] = Int32GetDatum(-1);

		/* It's possible our filter criteria involves the RecScore somehow.
		 * If that's the case, we need to calculate it before we do the
		 * qual filtering. Also, if we're doing a JoinRecommend, we should
		 * not calculate the RecScore in this node. In the current version
		 * of RecDB, an OP_NOFILTER shouldn't be allowed. */
		if (attributes->opType == OP_NOFILTER)
			applyRecScore(recnode, slot, itemID, itemindex);

		/* Move onto the next item, for next time. If we're doing a RecJoin,
		 * though, we'll move onto the next user instead. */
		recnode->itemNum++;
		if (recnode->itemNum >= recnode->totalItems ||
			attributes->opType == OP_JOIN ||
			attributes->opType == OP_GENERATEJOIN) {
			/* If we've reached the last item, move onto the next user.
			 * If we've reached the last user, we're done. */
			recnode->userNum++;
			recnode->newUser = true;
			recnode->itemNum = 0;
			if (recnode->userNum >= recnode->totalUsers)
				recnode->finished = true;
		}

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
			 * If this is an invalid user, then we'll skip this tuple,
			 * adding one to the filter count.
			 */
			if (!recnode->validUser) {
				InstrCountFiltered1(node, 1);
				ResetExprContext(econtext);
				ExecDropSingleTupleTableSlot(slot);
				continue;
			}

			/*
			 * Found a satisfactory scan tuple. This is usually when
			 * we will calculate and apply the RecScore.
			 */
			if (attributes->opType == OP_FILTER || attributes->opType == OP_GENERATE)
				applyRecScore(recnode, slot, itemID, itemindex);

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
		ExecDropSingleTupleTableSlot(slot);
	}

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

	/* Code for a future version of RecDB. */
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
	/* Our next step is to get the list of all users who participated in the
	 * events table. At the least, we need to consider each one up until the
	 * point where WHERE filters are applied. Any user IDs that survive that
	 * filter will have structures created for recommendation. */
	/* Note: if we're generating recommendations on-the-fly, we may not need
	 * to do this, as this list may be created as a side effect. */

	querystring = (char*) palloc(1024*sizeof(char));
	if ((attributes->opType != OP_GENERATE && attributes->opType != OP_GENERATEJOIN) ||
	    attributes->method == itemCosCF ||
	    attributes->method == itemPearCF) {
		sprintf(querystring,"select count(distinct %s) from %s;",
			attributes->userkey,attributes->eventtable);
		queryDesc = recathon_queryStart(querystring,&recathoncontext);
		planstate = queryDesc->planstate;
		hslot = ExecProcNode(planstate);
		recstate->totalUsers = getTupleInt(hslot,"count");
		recathon_queryEnd(queryDesc,recathoncontext);

		/* In the event that there are no user IDs, our ratings table is empty, so
		 * we can't do anything. */
		if (recstate->totalUsers <= 0)
			elog(ERROR, "no ratings in table %s, cannot predict ratings",
				attributes->eventtable);

		recstate->userList = (int*) palloc(recstate->totalUsers*sizeof(int));
		recstate->userNum = 0;

		/* Now for the actual query. */
		sprintf(querystring,"select distinct %s from %s order by %s;",
			attributes->userkey,attributes->eventtable,attributes->userkey);
		queryDesc = recathon_queryStart(querystring,&recathoncontext);
		planstate = queryDesc->planstate;

		i = 0;
		for (;;) {
			int currentUser;

			hslot = ExecProcNode(planstate);
			if (TupIsNull(hslot)) break;

			currentUser = getTupleInt(hslot,attributes->userkey);

			recstate->userList[i] = currentUser;
			i++;
			if (i >= recstate->totalUsers) break;
		}
		recathon_queryEnd(queryDesc,recathoncontext);

		/* Quick error protection. */
		recstate->totalUsers = i;
		if (recstate->totalUsers <= 0)
			elog(ERROR, "no ratings in table %s, cannot predict ratings",
				attributes->eventtable);

		/* Lastly, initialize the attributes->userID. */
		attributes->userID = recstate->userList[0] - 1;
	}

	/* Next, for annoying and convoluted reasons, we need a full list of all the
	 * items in the rating table. This will help us circumvent some filter issues
	 * while remaining as efficient as we can manage. */
	if ((attributes->opType != OP_GENERATE && attributes->opType != OP_GENERATEJOIN) ||
	    attributes->method == userCosCF ||
	    attributes->method == userPearCF) {
		sprintf(querystring,"select count(distinct %s) from %s;",
			attributes->itemkey,attributes->eventtable);
		queryDesc = recathon_queryStart(querystring,&recathoncontext);
		planstate = queryDesc->planstate;
		hslot = ExecProcNode(planstate);
		recstate->fullTotalItems = getTupleInt(hslot,"count");
		recathon_queryEnd(queryDesc,recathoncontext);

		/* In the event that there are no item IDs, our ratings table is empty, so
		 * we can't do anything. */
		if (recstate->fullTotalItems <= 0)
			elog(ERROR, "no ratings in table %s, cannot predict ratings",
				attributes->eventtable);

		recstate->fullItemList = (int*) palloc(recstate->fullTotalItems*sizeof(int));
		recstate->fullItemNum = 0;

		/* Now for the actual query. */
		sprintf(querystring,"select distinct %s from %s order by %s;",
			attributes->itemkey,attributes->eventtable,attributes->itemkey);
		queryDesc = recathon_queryStart(querystring,&recathoncontext);
		planstate = queryDesc->planstate;

		i = 0;
		for (;;) {
			int currentItem;

			hslot = ExecProcNode(planstate);
			if (TupIsNull(hslot)) break;

			currentItem = getTupleInt(hslot,attributes->itemkey);

			recstate->fullItemList[i] = currentItem;
			i++;
			if (i >= recstate->fullTotalItems) break;
		}
		recathon_queryEnd(queryDesc,recathoncontext);

		/* Quick error protection. */
		recstate->fullTotalItems = i;
		if (recstate->fullTotalItems <= 0)
			elog(ERROR, "no ratings in table %s, cannot predict ratings",
				attributes->eventtable);
	}

	recstate->finished = false;
	recstate->base_slot = NULL;
	recstate->newUser = true;
	recstate->useratt = -1;
	recstate->itematt = -1;
	recstate->eventatt = -1;

	/* Initialize certain structures to NULL. */
	recstate->ratedTable = NULL;
	recstate->pendingTable = NULL;
	recstate->simTable = NULL;
	recstate->itemList = NULL;
	recstate->userFeatures = NULL;

	/* In case we don't have a pre-built recommender, we need to assemble
	 * the appropriate structures now. */
	recstate->itemCFmodel = NULL;
	recstate->userCFmodel = NULL;
	recstate->SVDusermodel = NULL;
	recstate->SVDitemmodel = NULL;

	if (attributes->opType == OP_GENERATE || attributes->opType == OP_GENERATEJOIN) {
		switch (attributes->method) {
			case itemPearCF:
				generateItemPearModel(recstate);
				break;
			case userCosCF:
				generateUserCosModel(recstate);
				break;
			case userPearCF:
				generateUserPearModel(recstate);
				break;
			case SVD:
				generateSVDmodel(recstate);
				break;
			/* The default case is itemCosCF. There shouldn't actually
			 * be a possibility of "default", but just in case. */
			case itemCosCF:
			default:
				generateItemCosModel(recstate);
				break;
		}
	}

	/* Lastly, we also need to increase the query counter for this particular table,
	 * if it exists already. If this was on-the-fly, forget it. */
	if (attributes->recIndexName) {
		sprintf(querystring,"update %s set querycounter = querycounter+1;",attributes->recIndexName);
		recathon_queryExecute(querystring);
	}
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
	if (node->base_slot)
		FreeTupleDesc(node->base_slot);
}

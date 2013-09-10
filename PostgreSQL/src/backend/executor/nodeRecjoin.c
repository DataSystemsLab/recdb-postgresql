/*-------------------------------------------------------------------------
 *
 * nodeRecjoin.c
 *	  routines to optimize joins with Recommend nodes
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2013, University of Minnesota
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeRecjoin.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecRecJoin	 - process a nestloop join of two plans
 *		ExecInitRecJoin	 - initialize the join
 *		ExecEndRecJoin	 - shut down the join
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeRecjoin.h"
#include "utils/memutils.h"
#include "utils/recathon.h"


/* ----------------------------------------------------------------
 *		ExecRecJoin(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecRecJoin(RecJoinState *recjoin)
{
	RecScanState *recnode;
	AttributeInfo *attributes;

	NestLoopState *node;
//	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	List	   *joinqual;
	List	   *otherqual;
	ExprContext *econtext;

	node = recjoin->subjoin;
	recnode = recjoin->recnode;
	attributes = (AttributeInfo*) recnode->attributes;

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

//	nl = (NestLoop *) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Check to see if we're still projecting out tuples from a previous join
	 * tuple (because there is a function-returning-set in the projection
	 * expressions).  If so, try to project another one.
	 */
	if (node->js.ps.ps_TupFromTlist)
	{
		TupleTableSlot *result;
		ExprDoneCond isDone;

		result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
		if (isDone == ExprMultipleResult)
			return result;
		/* Done with that source tuple... */
		node->js.ps.ps_TupFromTlist = false;
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a join tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join. We're going to get exactly one
	 * tuple from the outer plan, because we just want to use its tupleDesc
	 * in order to create new nodes. The inner loop is the key; we'll loop
	 * over the items table, and for each item we see, we'll check it against
	 * the items we need to rate. If we do need to rate that item, we'll
	 * generate a prediction, stuff it in the tuple, and return it all. This
	 * repeats until the inner loop is out of tuples.
	 */
	ENL1_printf("entering main loop");

	for (;;)
	{
		int i, userID, innerItemID, natts;
		GenRating *findRating;
		TupleTableSlot *tempslot;

		/*
		 * If we don't have our tupleDesc, then we do exactly one fetch to
		 * the outer loop. We don't actually need the results, because by
		 * doing this, we've made sure that base_slot is populated.
		 */
		if (!recnode->base_slot) {
			GenRating *tempItem;

			outerTupleSlot = ExecProcNode(outerPlan);
			/* If this happens, something has gone wrong. */
			if (TupIsNull(outerTupleSlot)) {
				ENL1_printf("no outer tuple, ending join");
				return NULL;
			}

			/* Otherwise, we need to construct our first hash
			 * table, since we need info from the previous operator
			 * to do so. */
			recjoin->itemTable = hashCreate(recjoin->recnode->totalItems);
			for (i = 0; i < recjoin->recnode->totalItems; i++) {
				int currentItem = recjoin->recnode->itemList[i];

				tempItem = (GenRating*) palloc(sizeof(GenRating));
				tempItem->ID = currentItem;
				tempItem->index = -1;
				tempItem->next = NULL;

				hashAdd(recjoin->itemTable,tempItem);
			}
		}

		/* We construct a new tuple on the fly. */
		outerTupleSlot = MakeSingleTupleTableSlot(recnode->base_slot);
		outerTupleSlot->tts_isempty = false;

		/* Mark all slots as non-empty and zero. */
		natts = outerTupleSlot->tts_tupleDescriptor->natts;
		for (i = 0; i < natts; i++) {
			/* Mark slot. */
			outerTupleSlot->tts_values[i] = Int32GetDatum(0);
			outerTupleSlot->tts_isnull[i] = false;
			outerTupleSlot->tts_nvalid++;
		}

		/*
		 * try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);
		econtext->ecxt_innertuple = innerTupleSlot;

		/* If there's no inner tuple, then we should attempt to get a new
		 * outer tuple, like a normal join. */
		while (TupIsNull(innerTupleSlot))
		{
			GenRating *tempItem;

			ENL1_printf("no inner tuple, getting new outer tuple");
			/* It's not as simple as just getting a new outer tuple,
			 * though. We need to rebuild our hash structure to reflect
			 * the new user ID, as well. We don't actually use the
			 * returned slot here, it's just used to make the previous
			 * level generate certain information. */
			tempslot = ExecProcNode(outerPlan);

			/* If the returned slot is NULL, though, we're just done. */
			if (TupIsNull(tempslot)) {
				ENL1_printf("out of users, ending join");
				return NULL;
			}

			freeHash(recjoin->itemTable);
			recjoin->itemTable = hashCreate(recjoin->recnode->totalItems);
			for (i = 0; i < recjoin->recnode->totalItems; i++) {
				int currentItem = recjoin->recnode->itemList[i];

				tempItem = (GenRating*) palloc(sizeof(GenRating));
				tempItem->ID = currentItem;
				tempItem->index = -1;
				tempItem->next = NULL;

				hashAdd(recjoin->itemTable,tempItem);
			}

			/* Now try again for that inner tuple. */
			ENL1_printf("getting new inner tuple");

			innerTupleSlot = ExecProcNode(innerPlan);
			econtext->ecxt_innertuple = innerTupleSlot;
		}

		/*
		 * We now have an inner tuple and a shell of an outer tuple. We need
		 * to extract the item ID from the inner tuple and use that to build
		 * a new outer tuple, then we send them for qual checking.
		 */
		innerItemID = getTupleInt(innerTupleSlot,attributes->itemkey);
		userID = attributes->userID;

		/*
		 * Is this item ID one of the ones we need to predict a rating for?
		 */
		findRating = hashFind(recjoin->itemTable,innerItemID);
		if (!findRating) continue;

		/*
		 * We're ok to construct a tuple at this point.
		 */
		outerTupleSlot->tts_values[recnode->useratt] = Int32GetDatum(userID);
		outerTupleSlot->tts_isnull[recnode->useratt] = false;
		outerTupleSlot->tts_values[recnode->itematt] = Int32GetDatum(innerItemID);
		outerTupleSlot->tts_isnull[recnode->itematt] = false;

		econtext->ecxt_outertuple = outerTupleSlot;

		/*
		 * at this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext, false))
		{
			node->nl_MatchedOuter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_ANTI)
			{
				node->nl_NeedNewOuter = true;
				continue;		/* return to top of loop */
			}

			/*
			 * In a semijoin, we'll consider returning the first match, but
			 * after that we're done with this outer tuple.
			 */
			if (node->js.jointype == JOIN_SEMI)
				node->nl_NeedNewOuter = true;

			if (otherqual == NIL || ExecQual(otherqual, econtext, false))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				TupleTableSlot *result;
				ExprDoneCond isDone;

				/*
				 * The tuples match our qualifications. We now apply
				 * the RecScore before joining the tuples and sending
				 * them on their happy way.
 				 */
				int itemindex = binarySearch(recnode->fullItemList, innerItemID, 0, recnode->fullTotalItems);
				applyRecScore(recnode, outerTupleSlot, innerItemID, itemindex);

				ENL1_printf("qualification succeeded, projecting tuple");

				result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

				if (isDone != ExprEndResult)
				{
					node->js.ps.ps_TupFromTlist =
						(isDone == ExprMultipleResult);
					return result;
				}
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

/* ----------------------------------------------------------------
 *		ExecInitRecJoin
 * ----------------------------------------------------------------
 */
RecJoinState *
ExecInitRecJoin(RecJoin *node, EState *estate, int eflags)
{
	int i;
	GenRating *tempItem;
	RecJoinState *rjstate;

	rjstate = makeNode(RecJoinState);

	/* Initialize the NestLoop join, then store its information in our structure. */
	rjstate->subjoin = (NestLoopState*) ExecInitNode((Plan*)node->subjoin, estate, eflags);
	rjstate->js.ps = rjstate->subjoin->js.ps;
	rjstate->js.jointype = rjstate->subjoin->js.jointype;
	rjstate->js.joinqual = rjstate->subjoin->js.joinqual;
	rjstate->js.ps.type = T_RecJoinState;

	rjstate->recnode = (RecScanState*) rjstate->subjoin->js.ps.lefttree;
	rjstate->innerscan = rjstate->subjoin->js.ps.righttree;

	NL1_printf("ExecInitRecJoin: %s\n",
			   "node initialized");

	return rjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndRecJoin
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndRecJoin(RecJoinState *node)
{
	NL1_printf("ExecEndRecJoin: %s\n",
			   "ending node processing");

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndRecJoin: %s\n",
			   "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanRecJoin
 * ----------------------------------------------------------------
 */
void
ExecReScanRecJoin(RecJoinState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	/*
	 * If outerPlan->chgParam is not null then plan will be automatically
	 * re-scanned by first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	/*
	 * innerPlan is re-scanned for each new outer tuple and MUST NOT be
	 * re-scanned from here or you'll get troubles from inner index scans when
	 * outer Vars are used as run-time keys...
	 */

	node->js.ps.ps_TupFromTlist = false;
	node->subjoin->nl_NeedNewOuter = true;
	node->subjoin->nl_MatchedOuter = false;
}

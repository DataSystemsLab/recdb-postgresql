/*-------------------------------------------------------------------------
 *
 * execRecommend.h
 *	  methods used by the execRecommend.c file
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execdesc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECRECOMMEND_H
#define EXECRECOMMEND_H

#include "nodes/execnodes.h"

extern void ExecReScanRecScan(RecScanState *node);
extern void ExecRecMarkPos(RecScanState *node);
extern void ExecRecRestrPos(RecScanState *node);
extern TupleTableSlot *ExecRecommend(RecScanState *recnode, ExecScanAccessMtd accessMtd, ExecScanRecheckMtd recheckMtd);
extern RecScanState *ExecInitRecScan(RecScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecRecScan(RecScanState *node);
extern void ExecEndRecScan(RecScanState *node);

#endif   /* EXECRECOMMEND_H  */

/*-------------------------------------------------------------------------
 *
 * nodeRecjoin.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2013, University of Minnesota
 *
 * src/include/executor/nodeRecjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODERECJOIN_H
#define NODERECJOIN_H

#include "nodes/execnodes.h"

extern RecJoinState *ExecInitRecJoin(RecJoin *node, EState *estate, int eflags);
extern TupleTableSlot *ExecRecJoin(RecJoinState *node);
extern void ExecEndRecJoin(RecJoinState *node);
extern void ExecReScanRecJoin(RecJoinState *node);

#endif   /* NODERECJOIN_H */

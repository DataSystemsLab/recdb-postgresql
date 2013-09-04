/*-------------------------------------------------------------------------
 *
 * parse_rec.h
 *	  functions to parse RECOMMEND queries
 *
 *
 * Portions Copyright (c) 2013, University of Minnesota
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_rec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_REC_H
#define PARSE_REC_H

#include "parser/parse_node.h"

/* We only have one external function; most of them are static. */
extern SelectStmt *transformRecommendClause(ParseState *pstate, SelectStmt *stmt,
					     const char *constructName);

#endif	/* PARSE_REC_H */

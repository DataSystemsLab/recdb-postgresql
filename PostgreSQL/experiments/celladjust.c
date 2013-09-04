/* A program that generates a workload to test the Recathon program. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "libpq-fe.h"

#define DEBUG 0
#define true 1
#define false 0
typedef int bool;

typedef enum {
	CELL_ALPHA,
	CELL_BETA,
	CELL_GAMMA,
	CELL_DELTA
} recathon_cell;

typedef struct AttributeInfo {
	int			numatts;
	int			valid_users[10];
	recathon_cell		celltype;
	char			*recname;
	char			**attnames;
	char			**attvalues;
	struct AttributeInfo 	*next;
} AttributeInfo;

static void printCellList(AttributeInfo **atts, int numcells);

// DEBUG FUNCTION: print a linked list of AttributeInfo cells.
static void printCellList(AttributeInfo **atts, int numcells) {
	int i, j;

	for (i = 0; i < numcells; i++) {
		printf("Recommender %s\n",atts[i]->recname);
		printf("Number of atts: %d\n",atts[i]->numatts);
		for (j = 0; j < atts[i]->numatts; j++)
			printf("Att %s has value %s.\n",atts[i]->attnames[j],atts[i]->attvalues[j]);
	}
}

// This function converts a linked list into an array.
AttributeInfo **flatten(AttributeInfo *walker, int *numcells) {
	int ncells = 0;
	int i = 0;
	AttributeInfo *temp;
	AttributeInfo **atts;

	for (temp = walker; temp; temp = temp->next)
		ncells++;

	atts = (AttributeInfo**) malloc(ncells*sizeof(AttributeInfo*));
	for (temp = walker; temp; temp = temp->next) {
		atts[i] = temp;
		i++;
	}

	(*numcells) = ncells;
	return atts;
}

static void addUsers(AttributeInfo **atts, int numcells, PGconn *psql) {
	int i, j;
	PGresult *query;

	for (i = 0; i < numcells; i++) {
		int rows;
		char *qstring;
		AttributeInfo *attcell;

		attcell = atts[i];
		// Prepare a query.
		qstring = (char*) malloc(1024*sizeof(char));
		sprintf(qstring,"select userid from users ");
		// Modify the query with attributes.
		if (attcell->numatts > 0) {
			strncat(qstring,"where ",6);
			for (j = 0; j < attcell->numatts; j++) {
				char addition[128];
				sprintf(addition,"%s = '%s' ",
					attcell->attnames[j],attcell->attvalues[j]);
				strncat(qstring,addition,strlen(addition));
				if (j+1 < attcell->numatts)
					strncat(qstring,"and ",5);
			}
		}
		strncat(qstring,"order by zipcode limit 10;",26);

		// Execute the query.
		query = PQexec(psql,qstring);

		rows = PQntuples(query);
		for (j = 0; j < 10; j++)
			attcell->valid_users[j] = atoi(PQgetvalue(query,j,0));

		PQclear(query);
		free(qstring);
	}
}

int main(int argc, char *argv[]) {
	/* First, we need to take in input from the items file. */
	if (argc != 6) {
		printf("Usage: celladjust recommender alpha beta gamma delta\n");
		printf("The values for alpha, beta, gamma and delta need to be integers that sum to 100.\n");
		exit(0);
	}
	int i, j, k;
	// These are the parameters that come from the user.
	int nalpha, nbeta, ngamma, ndelta;
	// These represent thresholds.
	int talpha, tbeta, tgamma, tdelta;
	// This is a derived parameters from the database.
	int numcells;
	char *recommender;
	PGconn *psql;

	AttributeInfo *head_atts, *tail_atts;
	AttributeInfo **atts;
	recathon_cell celltype;
	head_atts = NULL; tail_atts = NULL;

	// Storing our parameters.
	recommender = argv[1];
	nalpha = atoi(argv[2]);
	nbeta = atoi(argv[3]);
	ngamma = atoi(argv[4]);
	ndelta = atoi(argv[5]);

	if (nalpha+nbeta+ngamma+ndelta != 100) {
		printf("The values for alpha, beta, gamma and delta need to be integers that sum to 100.\n");
		exit(0);
	}

	// Seeding our RNG.
	srand(time(NULL));

	/* Connect to the database. */
	psql = PQconnectdb("host = 'localhost' port = '5432' dbname = 'recathon'");
	if (PQstatus(psql) != CONNECTION_OK) printf("bad conn\n");

if (DEBUG) printf("%s, %s, %s, %s, %s\n",PQdb(psql), PQuser(psql), PQpass(psql), PQhost(psql), PQport(psql));
	if (psql == NULL) printf("connection failed\n");

	// Next, we need to query the index of the recommender, to get the attribute information and
	// cell types.
	{
		char *querystring, *celltype;
		PGresult *query;
		int rows, cols, extra;
		AttributeInfo *newatt;

		querystring = (char*) malloc(1024*sizeof(char));
		// Since we don't know all of the attributes, we need to request everything.
		sprintf(querystring,"select * from %sindex;",recommender);
		query = PQexec(psql,querystring);

		rows = PQntuples(query);
		cols = PQnfields(query);

		// Given the number of rows and the proportions of each cell, we can figure out the
		// number of each kind of cell to create.
		talpha = rows * nalpha;
		tbeta = rows * nbeta;
		tgamma = rows * ngamma;
		tdelta = rows * ndelta;
		extra = ((talpha % 100) + (tbeta % 100) + (tgamma % 100) + (tdelta % 100)) / 100;
		talpha /= 100;
		tbeta /= 100;
		tgamma /= 100;
		tdelta /= 100;
		// We now need to distribute the extra cells.
		if (extra > 0) { talpha++; extra--; }
		if (extra > 0) { tbeta++; extra--; }
		if (extra > 0) { tgamma++; extra--; }
		if (extra > 0) { tdelta++; extra--; }

		// A new AttributeInfo for each row.
		for (j = 0; j < rows; j++) {
			// Get query information. Cell type is attribute #8. Recommender-specific
			// attributes begin at #13.
			newatt = (AttributeInfo*) malloc(sizeof(AttributeInfo));
			newatt->next = NULL;

			newatt->recname = (char*) malloc(128*sizeof(char));
			sprintf(newatt->recname,"%s",recommender);

			newatt->numatts = cols - 12;
			newatt->attnames = (char**) malloc(newatt->numatts*sizeof(char*));
			for (k = 0; k < newatt->numatts; k++)
				newatt->attnames[k] = (char*) malloc(64*sizeof(char));
			newatt->attvalues = (char**) malloc(newatt->numatts*sizeof(char*));
			for (k = 0; k < newatt->numatts; k++)
				newatt->attvalues[k] = (char*) malloc(64*sizeof(char));

			celltype = PQgetvalue(query,j,7);
			if (strcmp(celltype,"Alpha") == 0)
				newatt->celltype = CELL_ALPHA;
			else if (strcmp(celltype,"Beta") == 0)
				newatt->celltype = CELL_BETA;
			else if (strcmp(celltype,"Gamma") == 0)
				newatt->celltype = CELL_GAMMA;
			else
				newatt->celltype = CELL_DELTA;

			// Get column information.
			for (k = 0; k < cols-12; k++) {
				sprintf(newatt->attnames[k],"%s",PQfname(query,k+12));
				sprintf(newatt->attvalues[k],"%s",PQgetvalue(query,j,k+12));
			}

			// With the item complete, we add it to the linked list.
			if (!head_atts) {
				head_atts = newatt;
				tail_atts = newatt;
			} else {
				tail_atts->next = newatt;
				tail_atts = newatt;
			}
		}
		PQclear(query);
		free(querystring);
	}

	// For easy access, we should flatten our AttributeInfo list.
	atts = flatten(head_atts, &numcells);

	// DEBUG: loop through the lists of alpha/beta/gamma/delta cells and print info.
	if (DEBUG) {
		printf("--- ALL CELLS ---\n");
		printCellList(atts, numcells);
	}

	celltype = CELL_DELTA;
	// Now we do an update for the list of cells.
	for (i = 0; i < numcells; i++) {
		int randnum, randatt, randuser, userid;
		// Default value.
		bool valid = false;
		PGresult *workquery;
		char *qstring;
		AttributeInfo *queryatt = atts[i];

		// It's possible one of our buckets will have nothing in it, so
		// we need to continue choosing until we get something valid.
		while (!valid) {
			switch (celltype) {
				case CELL_ALPHA:
					celltype = CELL_BETA;
					if (tbeta > 0) {
						valid = true;
						tbeta--;
					}
					break;
				case CELL_BETA:
					celltype = CELL_GAMMA;
					if (tgamma > 0) {
						valid = true;
						tgamma--;
					}
					break;
				case CELL_GAMMA:
					celltype = CELL_DELTA;
					if (tdelta > 0) {
						valid = true;
						tdelta--;
					}
					break;
				default:
					celltype = CELL_ALPHA;
					if (talpha > 0) {
						valid = true;
						talpha--;
					}
					break;
			}
		}

		// Now to issue the actual update.
		qstring = (char*) malloc(1024*sizeof(char));
		sprintf(qstring,"update %sindex set celltype = ",queryatt->recname);
		// What type of cell?
		switch (celltype) {
			case CELL_ALPHA:
				strncat(qstring,"'Alpha'",7);
				break;
			case CELL_BETA:
				strncat(qstring,"'Beta'",6);
				break;
			case CELL_GAMMA:
				strncat(qstring,"'Gamma'",7);
				break;
			default:
				strncat(qstring,"'Delta'",7);
				break;
		}
		if (queryatt->numatts > 0) {
			strncat(qstring," where ",7);
			for (j = 0; j < queryatt->numatts; j++) {
				char addition[128];
				sprintf(addition,"%s = '%s' ",
					queryatt->attnames[j],queryatt->attvalues[j]);
				strncat(qstring,addition,strlen(addition));
				if (j+1 < queryatt->numatts)
					strncat(qstring,"and ",5);
			}
		}
		strncat(qstring,";",1);

		workquery = PQexec(psql,qstring);
		PQclear(workquery);
		free(qstring);
	}
	PQfinish(psql);
}

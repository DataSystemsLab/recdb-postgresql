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

char **recommenderList(int *numrecs) {
	// First, counting the number of recommenders.
	int numrec = 0;
	int i;
	FILE *fp;
	char buf[1024];
	char **recs;

	if ((fp = fopen("recommenders.properties","r")) == NULL) {
		fprintf(stderr,"Could not open recommenders.properties.\n");
		exit(1);
	}
	for (;fgets(buf,1024,fp) != NULL;)
		numrec++;
	fclose(fp);

	// Creating an array.
	recs = (char**) malloc(numrec*sizeof(char*));
	for (i = 0; i < numrec; i++)
		recs[i] = (char*) malloc(128*sizeof(char));

	// Then going back through to get all of the recommender information.
	if ((fp = fopen("recommenders.properties","r")) == NULL) {
		fprintf(stderr,"Could not open recommenders.properties.\n");
		exit(1);
	}

	i = 0;
	for (;fgets(buf,1024,fp) != NULL;) {
		char *tokline;
		tokline = strtok(buf,":");
		sprintf(recs[i],"%s",tokline);
		i++;
	}

	fclose(fp);
	(*numrecs) = numrec;
	return recs;
}

int main(int argc, char *argv[]) {
	/* First, we need to take in input from the items file. */
	if (argc != 6) {
		printf("Usage: workload total alpha beta gamma delta\n");
		printf("The values for alpha, beta, gamma and delta need to be integers that sum to 100.\n");
		exit(0);
	}
	int i, j, k, numrecs;
	// These are the parameters that come from the user.
	int total, nalpha, nbeta, ngamma, ndelta;
	// These represent thresholds.
	int talpha, tbeta, tgamma, tdelta;
	// These are derived parameters from the database.
	int alphacells, betacells, gammacells, deltacells;
	char **recs;
	PGconn *psql;

	AttributeInfo *head_alpha, *head_beta, *head_gamma, *head_delta;
	AttributeInfo *tail_alpha, *tail_beta, *tail_gamma, *tail_delta;
	AttributeInfo **alpha, **beta, **gamma, **delta;
	head_alpha = NULL; head_beta = NULL; head_gamma = NULL; head_delta = NULL;
	tail_alpha = NULL; tail_beta = NULL; tail_gamma = NULL; tail_delta = NULL;

	// Storing our parameters.
	total = atoi(argv[1]);
	nalpha = atoi(argv[2]);
	nbeta = atoi(argv[3]);
	ngamma = atoi(argv[4]);
	ndelta = atoi(argv[5]);

	// Establish thresholds for our RNG.
	tdelta = 100 - ndelta;
	tgamma = tdelta - ngamma;
	tbeta = tgamma - nbeta;
	talpha = 0;

	if (nalpha+nbeta+ngamma+ndelta != 100) {
		printf("The values for alpha, beta, gamma and delta need to be integers that sum to 100.\n");
		exit(0);
	}

	// Seeding our RNG.
	srand(time(NULL));

	// We start off by getting a recommender list.
	recs = recommenderList(&numrecs);
	printf("Numrecs: %d\n",numrecs);

	/* Connect to the database. */
	psql = PQconnectdb("host = 'localhost' port = '5432' dbname = 'recathon'");
	if (PQstatus(psql) != CONNECTION_OK) printf("bad conn\n");

	printf("%s, %s, %s, %s, %s\n",PQdb(psql), PQuser(psql), PQpass(psql), PQhost(psql), PQport(psql));
	if (psql == NULL) printf("connection failed\n");

	// Next, we need to query the index of each recommender, to get the attribute information and
	// cell types.
	for (i = 0; i < numrecs; i++) {
		char *querystring, *celltype;
		PGresult *query;
		int rows, cols;
		AttributeInfo *newatt;

		querystring = (char*) malloc(1024*sizeof(char));
		// Since we don't know all of the attributes, we need to request everything.
		sprintf(querystring,"select * from %sindex;",recs[i]);
		query = PQexec(psql,querystring);

		rows = PQntuples(query);
		cols = PQnfields(query);

		// A new AttributeInfo for each row.
		for (j = 0; j < rows; j++) {
			// Get query information. Cell type is attribute #8. Recommender-specific
			// attributes begin at #13.
			newatt = (AttributeInfo*) malloc(sizeof(AttributeInfo));
			newatt->next = NULL;

			newatt->recname = (char*) malloc(128*sizeof(char));
			sprintf(newatt->recname,"%s",recs[i]);

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

			// With the item complete, we put it into the appropriate bucket.
			switch (newatt->celltype) {
				case CELL_ALPHA:
					if (!head_alpha) {
						head_alpha = newatt;
						tail_alpha = newatt;
					} else {
						tail_alpha->next = newatt;
						tail_alpha = newatt;
					}
					break;
				case CELL_BETA:
					if (!head_beta) {
						head_beta = newatt;
						tail_beta = newatt;
					} else {
						tail_beta->next = newatt;
						tail_beta = newatt;
					}
					break;
				case CELL_GAMMA:
					if (!head_gamma) {
						head_gamma = newatt;
						tail_gamma = newatt;
					} else {
						tail_gamma->next = newatt;
						tail_gamma = newatt;
					}
					break;
				default:
					if (!head_delta) {
						head_delta = newatt;
						tail_delta = newatt;
					} else {
						tail_delta->next = newatt;
						tail_delta = newatt;
					}
					break;
			}
		}
		PQclear(query);
		free(querystring);
	}

	// For easy randomization, we should flatten our AttributeInfo lists.
	alpha = flatten(head_alpha, &alphacells);
	beta = flatten(head_beta, &betacells);
	gamma = flatten(head_gamma, &gammacells);
	delta = flatten(head_delta, &deltacells);

	// DEBUG: loop through the lists of alpha/beta/gamma/delta cells and print info.
	if (DEBUG) {
		printf("--- ALPHA CELLS ---\n");
		printCellList(alpha, alphacells);
		printf("--- BETA CELLS ---\n");
		printCellList(beta, betacells);
		printf("--- GAMMA CELLS ---\n");
		printCellList(gamma, gammacells);
		printf("--- DELTA CELLS ---\n");
		printCellList(delta, deltacells);
	}

	// One more thing we need to do is obtain a list of users that will work for
	// each AttributeInfo. We can semi-randomize by sorting based on zip code.
	addUsers(alpha, alphacells, psql);
	addUsers(beta, betacells, psql);
	addUsers(gamma, gammacells, psql);
	addUsers(delta, deltacells, psql);

	// Now to issue the given number of queries, with the frequencies established
	// probabilistically.
	for (i = 0; i < total; i++) {
		int randnum, randatt, randuser, userid;
		recathon_cell celltype;
		bool valid = false;
		PGresult *workquery;
		char *qstring;
		AttributeInfo *queryatt;

		// It's possible one of our buckets will have nothing in it, so
		// we need to continue choosing until we get something valid.
		while (!valid) {
			// A RNG chooses which kind of cell we work with.
			randnum = rand() % 100;
			if (randnum < tbeta) {
				if (alphacells > 0) {
					valid = true;
					celltype = CELL_ALPHA;
				}
			}
			else if (randnum < tgamma) {
				if (betacells > 0) {
					valid = true;
					celltype = CELL_BETA;
				}
			}
			else if (randnum < tdelta) {
				if (gammacells > 0) {
					valid = true;
					celltype = CELL_GAMMA;
				}
			}
			else {
				if (deltacells > 0) {
					valid = true;
					celltype = CELL_DELTA;
				}
			}
		}

		// Depending on our cell type, we'll have a different set of possible
		// queries to issue; we can choose from the alpha, beta, gamma or delta
		// buckets. Which item we get is also random.
		switch (celltype) {
			case CELL_ALPHA:
				randatt = rand() % alphacells;
				queryatt = alpha[randatt];
				break;
			case CELL_BETA:
				randatt = rand() % betacells;
				queryatt = beta[randatt];
				break;
			case CELL_GAMMA:
				randatt = rand() % gammacells;
				queryatt = gamma[randatt];
				break;
			default:
				randatt = rand() % deltacells;
				queryatt = delta[randatt];
				break;
		}

		randuser = rand() % 10;
		userid = queryatt->valid_users[randuser];
		qstring = (char*) malloc(1024*sizeof(char));
		sprintf(qstring,"select itemid from %s recommend(10) userid=%d",queryatt->recname,userid);
		if (queryatt->numatts > 0) {
			strncat(qstring," and ",5);
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

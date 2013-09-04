/* A program that generates a workload to test the Recathon program. 
 * It runs several queries and records the results in a file. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
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
	int			numheavy;
	int			numlight;
	int			heavy_users[10];
	int			light_users[10];
	recathon_cell		celltype;
	char			*recname;
	char			**attnames;
	char			**attvalues;
	struct AttributeInfo 	*next;
} AttributeInfo;

// Our time variable is global, as the queries are spread across
// multiple functions.
double total_time;

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

		// How we proceed depends on if this is an alpha cell or not.
		// If it is, we need separate lists of heavy vs. non-heavy
		// users to reference.
		if (attcell->celltype == CELL_ALPHA) {
			// Prepare the heavy query.
			qstring = (char*) malloc(1024*sizeof(char));
			sprintf(qstring,"select distinct userid,zipcode from users ");
			// Modify the query with attributes.
			if (attcell->numatts > 0) {
				strncat(qstring,"where ",6);
				for (j = 0; j < attcell->numatts; j++) {
					char addition[128];
					sprintf(addition,"%s = '%s' ",
						attcell->attnames[j],attcell->attvalues[j]);
					strncat(qstring,addition,strlen(addition));
					if (j+1 < attcell->numatts)
						strncat(qstring,"and ",4);
				}
			}
			strncat(qstring,"and userid in (select userid from heavy_users) ",47);
			strncat(qstring,"order by zipcode limit 10;",26);

			// Execute the query.
			query = PQexec(psql,qstring);

			rows = PQntuples(query);
			for (j = 0; j < 10 && j < rows; j++)
				attcell->heavy_users[j] = atoi(PQgetvalue(query,j,0));
			attcell->numheavy = j;

			PQclear(query);
			free(qstring);

			// Prepare the light query.
			qstring = (char*) malloc(1024*sizeof(char));
			sprintf(qstring,"select distinct userid,zipcode from users ");
			// Modify the query with attributes.
			if (attcell->numatts > 0) {
				strncat(qstring,"where ",6);
				for (j = 0; j < attcell->numatts; j++) {
					char addition[128];
					sprintf(addition,"%s = '%s' ",
						attcell->attnames[j],attcell->attvalues[j]);
					strncat(qstring,addition,strlen(addition));
					if (j+1 < attcell->numatts)
						strncat(qstring,"and ",4);
				}
			}
			strncat(qstring,"and userid not in (select userid from heavy_users) ",51);
			strncat(qstring,"order by zipcode limit 10;",26);

			// Execute the query.
			query = PQexec(psql,qstring);

			rows = PQntuples(query);
			for (j = 0; j < 10 && j < rows; j++)
				attcell->light_users[j] = atoi(PQgetvalue(query,j,0));
			attcell->numlight = j;

			PQclear(query);
			free(qstring);
		} else {
			// Prepare a query.
			qstring = (char*) malloc(1024*sizeof(char));
			sprintf(qstring,"select distinct userid,zipcode from users ");
			// Modify the query with attributes.
			if (attcell->numatts > 0) {
				strncat(qstring,"where ",6);
				for (j = 0; j < attcell->numatts; j++) {
					char addition[128];
					sprintf(addition,"%s = '%s' ",
						attcell->attnames[j],attcell->attvalues[j]);
					strncat(qstring,addition,strlen(addition));
					if (j+1 < attcell->numatts)
						strncat(qstring,"and ",4);
				}
			}
			strncat(qstring,"order by zipcode limit 10;",26);

			// Execute the query.
			query = PQexec(psql,qstring);

			rows = PQntuples(query);
			for (j = 0; j < 10 && j < rows; j++)
				attcell->light_users[j] = atoi(PQgetvalue(query,j,0));
			attcell->numlight = j;
			attcell->numheavy = 0;

			PQclear(query);
			free(qstring);
		}
	}
}

static int queryAll(AttributeInfo **atts, int numcells, int pheavy, PGconn *psql) {
	int i, ret, timeint;
	struct timeval start_time, end_time;

	ret = 0;

	for (i = 0; i < numcells; i++) {
if (DEBUG) printf("i is %d\n", i);
		int rows, randuser, userid, j;
		char *qstring;
		AttributeInfo *queryatt;
		PGresult *workquery;

		queryatt = atts[i];

		// We choose a user differently if it's an alpha cell.
		if (queryatt->celltype == CELL_ALPHA) {
			bool valid = false;
			int randatt;
if (DEBUG) printf("%d heavy cells, %d light cells\n",queryatt->numheavy,queryatt->numlight);
			// It's possible a cell is totally empty. If so, no point querying.
			if (queryatt->numheavy == 0 && queryatt->numlight == 0) continue;
			else if (queryatt->numlight == 0) {
				randuser = rand() % queryatt->numheavy;
				userid = queryatt->heavy_users[randuser];
			} else if (queryatt->numheavy == 0) {
				randuser = rand() % queryatt->numlight;
				userid = queryatt->light_users[randuser];
			} else {
				randatt = rand() % 100;
				if (randatt < pheavy) {
					randuser = rand() % queryatt->numheavy;
					userid = queryatt->heavy_users[randuser];
				} else {
					randuser = rand() % queryatt->numlight;
					userid = queryatt->light_users[randuser];
				}
			}
		} else {
			// It's possible a cell is totally empty. If so, no point querying.
			if (queryatt->numlight == 0) continue;
			randuser = rand() % queryatt->numlight;
			userid = queryatt->light_users[randuser];
		}

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

		// Query and timer.
		timeint = gettimeofday(&start_time,NULL);
		workquery = PQexec(psql,qstring);
		timeint = gettimeofday(&end_time,NULL);
		total_time += (((double)(end_time.tv_sec - start_time.tv_sec))*1000000.0);
		total_time += end_time.tv_usec - start_time.tv_usec;

		PQclear(workquery);
		free(qstring);
		ret++;
	}
	return ret;
}

double execute_workload(char *argrec, int argtotal, int argheavy,
			int argalpha, int argbeta, int arggamma, int argdelta, PGconn *psql) {
	int i, j, k, numrecs, timeint;
	// These are the parameters that come from the user.
	int total, nheavy, nalpha, nbeta, ngamma, ndelta;
	// These represent thresholds.
	int theavy, talpha, tbeta, tgamma, tdelta;
	// These are derived parameters from the database.
	int alphacells, betacells, gammacells, deltacells;
	char *recommender;
	// Timing variables.
	struct timeval start_time, end_time;
	total_time = 0.0;

	AttributeInfo *head_alpha, *head_beta, *head_gamma, *head_delta;
	AttributeInfo *tail_alpha, *tail_beta, *tail_gamma, *tail_delta;
	AttributeInfo **alpha, **beta, **gamma, **delta;
	head_alpha = NULL; head_beta = NULL; head_gamma = NULL; head_delta = NULL;
	tail_alpha = NULL; tail_beta = NULL; tail_gamma = NULL; tail_delta = NULL;

	// Storing our parameters.
	recommender = argrec;
	total = argtotal;
	nheavy = argheavy;
	nalpha = argalpha;
	nbeta = argbeta;
	ngamma = arggamma;
	ndelta = argdelta;

	if (nalpha+nbeta+ngamma+ndelta != 100) {
		printf("The values for alpha, beta, gamma and delta need to be integers that sum to 100.\n");
		exit(0);
	}

	// Next, we need to query the index of each recommender, to get the attribute information and
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

		if (total < rows) {
			printf("ERROR: There must be at least as many queries as there are cells. Number of cells: %d\n", rows);
			exit(0);
		}

		// Given the number of rows and the proportions of each cell, we can figure out the
		// number of each kind of cell to create.
		talpha = total * nalpha;
		tbeta = total * nbeta;
		tgamma = total * ngamma;
		tdelta = total * ndelta;
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

		// And again for heavy vs. non-heavy users.
		theavy = talpha * nheavy;
		theavy /= 100;

		if (DEBUG) {
			printf("%d alpha queries.\n",talpha);
			printf("%d beta queries.\n",tbeta);
			printf("%d gamma queries.\n",tgamma);
			printf("%d delta queries.\n",tdelta);
			printf("%d heavy alpha queries.\n",theavy);
			printf("%d light alpha queries.\n",talpha-theavy);
		}

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
	if (DEBUG == 2) {
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

	// Now we start issuing the actual queries.

	// Since we need to query each cell at least once, we're going to go ahead
	// and query every cell in a list before we proceed.
	if (nalpha > 0) {
		int removed = queryAll(alpha, alphacells, nheavy, psql);
		total -= removed;
		talpha -= removed;
	}
	if (nbeta > 0) {
		int removed = queryAll(beta, betacells, 0, psql);
		total -= removed;
		tbeta -= removed;
	}
	if (ngamma > 0) {
		int removed = queryAll(gamma, gammacells, 0, psql);
		total -= removed;
		tgamma -= removed;
	}
	if (ndelta > 0) {
		int removed = queryAll(delta, deltacells, 0, psql);
		total -= removed;
		tdelta -= removed;
	}
if (DEBUG) printf("Total is %d, t is %d\n",total,(talpha+tbeta+tgamma+tdelta));
	// Now to issue the given number of queries, with the frequencies established
	// probabilistically.
	for (i = 0; i < total; i++) {
		int randnum, randatt, randuser, userid;
		recathon_cell celltype;
		bool valid = false;
		bool heavycell = false;
		PGresult *workquery;
		char *qstring;
		AttributeInfo *queryatt;
if (DEBUG) printf("Choosing cell.\nHeavy %d, alpha %d, beta %d, gamma %d, delta %d\n",theavy,talpha,tbeta,tgamma,tdelta);
if (DEBUG) printf("Cells: alpha %d, beta %d, gamma %d, delta %d\n",alphacells,betacells,gammacells,deltacells);
		// It's possible one of our buckets will have nothing in it, so
		// we need to continue choosing until we get something valid.
		while (!valid) {
//if (DEBUG) printf("Cell type: %d\n", (int) celltype);
			switch (celltype) {
				case CELL_ALPHA:
					celltype = CELL_BETA;
					if (tbeta > 0 && betacells > 0) {
						valid = true;
						tbeta--;
					}
					break;
				case CELL_BETA:
					celltype = CELL_GAMMA;
					if (tgamma > 0 && gammacells > 0) {
						valid = true;
						tgamma--;
					}
					break;
				case CELL_GAMMA:
					celltype = CELL_DELTA;
					if (tdelta > 0 && deltacells > 0) {
						valid = true;
						tdelta--;
					}
					break;
				default:
					celltype = CELL_ALPHA;
					if (talpha > 0 && alphacells > 0) {
						valid = true;
						talpha--;
						if (theavy > 0) {
							heavycell = true;
							theavy--;
						}
					}
					break;
			}
		}
if (DEBUG) printf("Escape! Cell type: %d\n", (int) celltype);
		// Depending on our cell type, we'll have a different set of possible
		// queries to issue; we can choose from the alpha, beta, gamma or delta
		// buckets. Which item we get is also random.
		switch (celltype) {
			case CELL_ALPHA:
				// We need to make sure we get a cell with heavy users
				// if it's that time.
				valid = false;
				while (!valid) {
					randatt = rand() % alphacells;
					queryatt = alpha[randatt];
					if ((heavycell && queryatt->numheavy > 0) ||
					    (!heavycell && queryatt->numlight > 0))
						valid = true;
				}
				if (heavycell) {
					randuser = rand() % queryatt->numheavy;
					userid = queryatt->heavy_users[randuser];
				} else {
					randuser = rand() % queryatt->numlight;
					userid = queryatt->light_users[randuser];
				}
				break;
			case CELL_BETA:
				valid = false;
				while (!valid) {
					randatt = rand() % betacells;
					queryatt = beta[randatt];
					if (queryatt->numlight > 0) valid = true;
				}
				randuser = rand() % queryatt->numlight;
				userid = queryatt->light_users[randuser];
				break;
			case CELL_GAMMA:
				valid = false;
				while (!valid) {
					randatt = rand() % gammacells;
					queryatt = gamma[randatt];
					if (queryatt->numlight > 0) valid = true;
				}
				randuser = rand() % queryatt->numlight;
				userid = queryatt->light_users[randuser];
				break;
			default:
				valid = false;
				while (!valid) {
					randatt = rand() % deltacells;
					queryatt = delta[randatt];
					if (queryatt->numlight > 0) valid = true;
				}
				randuser = rand() % queryatt->numlight;
				userid = queryatt->light_users[randuser];
				break;
		}
if (DEBUG) printf("Cell chosen\n");
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

		// Query and timer.
		timeint = gettimeofday(&start_time,NULL);
		workquery = PQexec(psql,qstring);
		timeint = gettimeofday(&end_time,NULL);
		total_time += (((double)(end_time.tv_sec - start_time.tv_sec))*1000000.0);
		total_time += end_time.tv_usec - start_time.tv_usec;

		PQclear(workquery);
		free(qstring);
	}

	// Return the time taken (in seconds).
	return total_time / 1000000.0;
}

void celladjust(char *recommender, int alpha, int beta, int gamma, int delta) {
	int i;
	pid_t my_id;
	char **commandvp;

	// Prepare the command.
	commandvp = (char**) malloc(6*sizeof(char*));
	for (i = 0; i < 6; i++)
		commandvp[i] = (char*) malloc(32*sizeof(char));
	sprintf(commandvp[0],"./celladjust");
	sprintf(commandvp[1],"%s",recommender);
	sprintf(commandvp[2],"%d",alpha);
	sprintf(commandvp[3],"%d",beta);
	sprintf(commandvp[4],"%d",gamma);
	sprintf(commandvp[5],"%d",delta);

	my_id = fork();

	if (my_id == 0) {
		execlp(commandvp[0], commandvp[0], commandvp[1], commandvp[2],
			commandvp[3], commandvp[4], commandvp[5], NULL);
	} else {
		int status;
		wait(&status);
	}

	// Free memory.
	for (i = 0; i < 6; i++)
		free(commandvp[i]);
	free(commandvp);
}

void issue_queries(char *recommender, int total, int heavy, int alpha, int beta, int gamma, int delta,
			FILE *fp, PGconn *psql) {
	double ttime;
	char *writestring;

	writestring = (char*) malloc(512*sizeof(char));

	celladjust(recommender, alpha, beta, gamma, delta);
	ttime = execute_workload(recommender, total, heavy, alpha, beta, gamma, delta, psql);
	// Write data.
	sprintf(writestring,"Recommender: %s, heavy: %d%%, alpha: %d%%, beta: %d%%, gamma: %d%%, delta: %d%%\nTotal time for %d queries: %f seconds\nAverage query time: %f seconds\n\n",
			recommender, heavy, alpha, beta, gamma, delta, total, ttime, (ttime/(double)total));
	printf("%s",writestring);
	fwrite(writestring, 1, strlen(writestring), fp);

	free(writestring);
}

int main(int argc, char *argv[]) {
	int i,j;
	char *recommender, *writestring, *outputfile;
	double ttime;
	FILE *fp;
	PGconn *psql;

	if (argc != 4) {
		printf("Usage: ./workload_record recommender total_queries output_file\n");
		exit(0);
	}
	recommender = argv[1];
	int total = atoi(argv[2]);
	outputfile = argv[3];

	// Seed our RNG.
	srand(time(NULL));

	/* Connect to the database. */
	psql = PQconnectdb("host = 'localhost' port = '5432' dbname = 'recathon'");
	if (PQstatus(psql) != CONNECTION_OK) printf("bad conn\n");

if (DEBUG) printf("%s, %s, %s, %s, %s\n",PQdb(psql), PQuser(psql), PQpass(psql), PQhost(psql), PQport(psql));
	if (psql == NULL) {
		printf("connection failed\n");
		exit(0);
	}

	// Open up a file for writing. Careful, this OVERWRITES!
	if ((fp = fopen(outputfile,"w")) == NULL) {
		printf("Could not open queries.dat.\n");
		exit(0);
	}

	// Now we issue our queries.

	// Four "extreme" approaches.
	// Query 1.
//	celladjust(recommender, 100, 0, 0, 0);
	issue_queries(recommender, total, 80, 100, 0, 0, 0, fp, psql);

	// Query 2.
//	celladjust(recommender, 0, 100, 0, 0);
	issue_queries(recommender, total, 80, 0, 100, 0, 0, fp, psql);

	// Query 3.
//	celladjust(recommender, 0, 0, 100, 0);
	issue_queries(recommender, total, 80, 0, 0, 100, 0, fp, psql);

	// Query 4.
//	celladjust(recommender, 0, 0, 0, 100);
	issue_queries(recommender, total, 80, 0, 0, 0, 100, fp, psql);

	// One "even" approach.
//	celladjust(recommender, 25, 25, 25, 25);
	// Query 5.
	issue_queries(recommender, total, 80, 25, 25, 25, 25, fp, psql);

	// Four 40% mixed approaches.
	// Query 6.
	issue_queries(recommender, total, 80, 40, 20, 20, 20, fp, psql);

	// Query 7.
	issue_queries(recommender, total, 80, 20, 40, 20, 20, fp, psql);

	// Query 8.
	issue_queries(recommender, total, 80, 20, 20, 40, 20, fp, psql);

	// Query 9.
	issue_queries(recommender, total, 80, 20, 20, 20, 40, fp, psql);

	// Four 70% mixed approaches.
	// Query 10.
	issue_queries(recommender, total, 80, 70, 10, 10, 10, fp, psql);

	// Query 11.
	issue_queries(recommender, total, 80, 10, 70, 10, 10, fp, psql);

	// Query 12.
	issue_queries(recommender, total, 80, 10, 10, 70, 10, fp, psql);

	// Query 13.
	issue_queries(recommender, total, 80, 10, 10, 10, 70, fp, psql);

/*	// Now for five random queries. Even cell division for these.
	celladjust(recommender, 25, 25, 25, 25);
	for (i = 0; i < 5; i++) {
		int nheavy, nalpha, nbeta, ngamma, ndelta;

		// The proportions of each query are chosen randomly, with caps.
		// Beta can be at most 10%. Delta can be at most 5% on the
		// last query only, 0 otherwise. Heavy must be at least 75%.
		nheavy = (rand() % 25) + 75;
		if (i == 4)
			ndelta = rand() % 5;
		else
			ndelta = 0;
		nbeta = rand() % 10;
		ngamma = rand() % (100 - ndelta - nbeta);
		nalpha = 100 - nbeta - ngamma - ndelta;
printf("Alpha %d, beta %d, gamma %d, delta %d, heavy %d\n",nalpha,nbeta,ngamma,ndelta,nheavy);
		ttime = execute_workload(recommender, total, nheavy, nalpha, nbeta, ngamma, ndelta, psql);
		printf("Total time: %f seconds\n",ttime);
		// Write data.
		sprintf(writestring,"Recommender: %s, heavy: %d%%, alpha: %d%%, beta: %d%%, gamma: %d%%, delta: %d%%\nTotal time for %d queries: %f seconds\n\n",
				recommender, nheavy, nalpha, nbeta, ngamma, ndelta, total, ttime);
		printf("%s",writestring);
		fwrite(writestring, 1, strlen(writestring), fp);
	}*/

	PQfinish(psql);
	fclose(fp);
}

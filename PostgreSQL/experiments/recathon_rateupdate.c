/*
 * recathon_rateupdate.c
 *
 * A program that periodically connects to our database, issuing queries to
 * update the update/query rates for every recommender cell.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libpq-fe.h"

#define true 1
#define false 0
#define INTERVAL 10
#define DEBUG 0
#define VERBOSE 1

typedef struct reclist {
	char		*recname;
	struct reclist	*next;
} reclist;

int main(int argc, char *argv[]) {
	/* We take in the database name and a possible hostname as arguments. */
	if (argc != 2 && argc != 3) {
		printf("Usage: recathon_rateupdate database (hostname)\n");
		exit(0);
	}
	char* database = argv[1];
	char* hostname;
	if (argc == 3) hostname = argv[2];
	else {
		hostname = (char*) malloc(10*sizeof(char));
		strncpy(hostname,"localhost",9);
	}

	/* Temporary(?) parameter values. */
	float QUERY_THRESHOLD = 0.1;
	float UPDATE_THRESHOLD = 0.1;

	/* Connect to the database. */
	PGconn *psql;
	char* connectstring = (char*) malloc(128*sizeof(char));
	sprintf(connectstring,"host = '%s' port = '5432' dbname = '%s'",hostname,database);
	psql = PQconnectdb(connectstring);
	if (PQstatus(psql) != CONNECTION_OK) printf("recathon_rateupdate error: Bad connection.\n");

	if (DEBUG) printf("%s, %s, %s, %s, %s\n",PQdb(psql), PQuser(psql), PQpass(psql), PQhost(psql), PQport(psql));
	if (psql == NULL) printf("recathon_rateupdate error: Connection failed.\n");

	/* Free unneeded memory. */
	free(connectstring);
	if (argc != 3) free(hostname);

	/* We're going to loop until Ctrl+C. */
	while (1) {
		sleep(INTERVAL);

		/* We need to scan our list of recommenders, so we can query
		 * every single one. */
		FILE* fp;
		if ((fp = fopen("recommenders.properties","r")) == NULL) {
			fprintf(stderr,"Could not open input file recommenders.properties.\n");
			exit(1);
		}

		/* We'll store the recommender names in a linked list. */
		reclist *head = NULL;
		reclist *tail = NULL;
		reclist *temp = NULL;

		char buf[1024];
		char *tokline;
		for (;fgets(buf,1024,fp) != NULL;) {
			temp = (reclist*) malloc(sizeof(reclist));
			temp->next = NULL;
			tokline = strtok(buf,":");
			int len = strlen(tokline);
			temp->recname = (char*) malloc((len+1)*sizeof(char));
			sprintf(temp->recname,"%s",tokline);

			if (head == NULL) {
				head = temp;
				tail = temp;
			} else {
				tail->next = temp;
				tail = temp;
			}
		}

		close(fp);

		/* If head is null, there are no recommenders, so go back to sleep. */
		if (head == NULL) continue;

		/* Now we iterate through our list of recommenders, issuing queries. */
		temp = head;
		for (;temp;temp = temp->next) {
			int i, j;

			/* We execute the first query, which requests counters from a
			 * recommender cell. */
			char *querystring = (char*) malloc(256*sizeof(char));
			sprintf(querystring,"select recviewname,querycounter2,updatecounter2 from %sindex;",temp->recname);
//			sprintf(querystring,"select recviewname from %sindex;",temp->recname);
			if (VERBOSE) printf("%s\n",querystring);

			PGresult *query, *updatequery;
			query = PQexec(psql,querystring);
			if (query == NULL) printf("recathon_rateupdate error: Null query.\n");

			/* Get query information. */
			int rows = PQntuples(query);
			int cols = PQnfields(query);
			if (DEBUG) printf("%d tuples returned\n",rows);
			if (DEBUG) printf("%d fields per tuple\n",cols);
			// column name: PQfname(query, colnum)
			// column result: PQgetvalue(query, rownum, colnum)
			if (DEBUG) {
				for (i = 0; i < rows; i++) {
					printf("Tuple %d:\n",i);
					for (j = 0; j < cols; j++)
						printf("%s = %s\n",PQfname(query,j),PQgetvalue(query,i,j));
				}
			}

			/* Now we use another query object to issue queries based on the information
			 * in our recommender index. */
			for (i = 0; i < rows; i++) {
				/* Step one is to derive the update and query rates, and then
				 * the cell type based on that. */
				int querycount = atoi(PQgetvalue(query,i,1));
				int updatecount = atoi(PQgetvalue(query,i,2));
				float queryrate = ((float) querycount) / ((float) INTERVAL);
				float updaterate = ((float) updatecount) / ((float) INTERVAL);

				/* What's the cell type? */
				char *celltype = (char*) malloc(8*sizeof(char));
				if (queryrate >= QUERY_THRESHOLD) {
					if (updaterate >= UPDATE_THRESHOLD) {
						sprintf(celltype,"Alpha");
					} else {
						sprintf(celltype,"Gamma");
					}
				} else if (updaterate >= UPDATE_THRESHOLD)
					sprintf(celltype,"Beta");
				else
					sprintf(celltype,"Delta");

				char *updatestring = (char*) malloc(512*sizeof(char));
				sprintf(updatestring,"update %sindex set querycounter2 = 0, updatecounter2 = 0, queryrate = %f, updaterate = %f, celltype = '%s' where recviewname = '%s';",temp->recname,queryrate,updaterate,celltype,PQgetvalue(query,i,0));
				if (VERBOSE) printf("%s\n",updatestring);

				/* Now that we have a query to execute, do that. */
				updatequery = PQexec(psql,updatestring);

				/* Free memory. */
				PQclear(updatequery);
				free(celltype);
				free(updatestring);
			} // End of update loop

			/* Free memory. */
			PQclear(query);
			free(querystring);
		} // End of recommenders

		/* Free memory. */
		for (tail = head; tail;) {
			temp = tail;
			free(temp->recname);
			tail = temp->next;
			free(temp);
		}
	} // End of infinite loop
	PQfinish(psql);
}

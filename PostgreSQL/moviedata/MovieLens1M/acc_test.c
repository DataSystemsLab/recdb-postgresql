/* A program that periodically connects to our database, issues
 * a RECOMMEND query, gets the results, and calculates accuracy. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libpq-fe.h"

#define true 1
#define false 0
#define USERID 4169

/* Inserting items from test_file into a list, which we will
 * compared returned items to. */
void listInsert(int* items, float* ratings, int item, float rating, int k) {
	int i = 0;
	/* Find one of the three stopping points:
	 * 1. We found a place in the middle of the list to put
	 *    this item.
	 * 2. We hit the end of the list, but have more space.
	 * 3. We run out of spaces. */
	while (i < k && items[i] >= 0 && ratings[i] >= rating) i++;

	int temp_item = item;
	float temp_rating = rating;
	int temp_item2; float temp_rating2;

	/* Do any list adjusting that might be necessary. */
	while (i < k && items[i] >= 0) {
		temp_item2 = items[i];
		temp_rating2 = ratings[i];
		items[i] = temp_item;
		ratings[i] = temp_rating;
		temp_item = temp_item2;
		temp_rating = temp_rating2;
		i++;
	}

	/* If we haven't fallen off the end, add our item to the list. */
	if (i < k) {
		items[i] = temp_item;
		ratings[i] = temp_rating;
	}
}

int checkItem(int* items, int item, int k) {
	int i;
	for (i = 0; i < k; i++) {
		if (items[i] == item) return true;
	}
	return false;
}

int main(int argc, char *argv[]) {
	/* First, we need to take in input from the items file. */
	if (argc != 3 && argc != 4) {
		printf("Usage: acc_test item_file interval (k)\n");
		exit(0);
	}
	char* test_file = argv[1];
	int interval = atoi(argv[2]);
	int k;
	int actual_k = 0;
	if (argc == 4) k = atoi(argv[3]);
	else k = 3883; // the number of unique items

	int i, j;
	int topItems[k];
	for (i = 0; i < k; i++) topItems[i] = -1;
	float topRatings[k];
	for (i = 0; i < k; i++) topRatings[i] = 0;

	FILE* fp;
	if ((fp = fopen(test_file,"r")) == NULL) {
		fprintf(stderr,"Could not open input file %s.\n",test_file);
		exit(1);
	}

	char buf[1024];
	char* tokline;
	for (;fgets(buf,1024,fp) != NULL;) {
		tokline = strtok(buf,";");
		int user = atoi(tokline);
		if (user == USERID) {
			tokline = strtok(NULL,";");
			int item = atoi(tokline);
			tokline = strtok(NULL,";");
			float rating = (float) atof(tokline);
			listInsert(topItems,topRatings,item,rating,k);
			actual_k++;
		}
	}
	if (argc != 3) k = actual_k;

	close(fp);

	/* Connect to the database. */
	PGconn *psql;
	psql = PQconnectdb("host = 'localhost' port = '5432' dbname = 'recathon'");
	if (PQstatus(psql) != CONNECTION_OK) printf("bad conn\n");

	printf("%s, %s, %s, %s, %s\n",PQdb(psql), PQuser(psql), PQpass(psql), PQhost(psql), PQport(psql));
	if (psql == NULL) printf("connection failed\n");

	/* We're going to loop until Ctrl+C. */
	while (1) {
		sleep(interval);
		/* Execute a query. */
		PGresult *query;
		query = PQexec(psql,"select G.itemid, G.ratingval from genderrec G where G.userid=4169 recommend(100) G.userid=4169 and G.gender='M';");
		if (query == NULL) printf("uhoh\n");

		/* Get query information. */
		int rows = PQntuples(query);
		int cols = PQnfields(query);
		printf("%d tuples returned\n",rows);
		printf("%d fields per tuple\n",cols);
		// column name: PQfname(query, colnum)
		// column result: PQgetvalue(query, rownum, colnum)
		for (i = 0; i < rows; i++) {
			printf("Tuple %d:\n",i);
			for (j = 0; j < cols; j++)
				printf("%s = %s\n",PQfname(query,j),PQgetvalue(query,i,j));
		}

		/* Match the query with the file information. */
		int matches = 0;
		int item_k = 0;
		for (i = 0; i < rows; i++) {
			int item = atoi(PQgetvalue(query,i,0));
			if (checkItem(topItems,item,k) == true) {
				printf("Match: %d\n",item);
				matches++;
			}
			item_k++;
		}

		float precision = ((float)matches)/((float)item_k);
		float recall = ((float)matches)/((float)k);
		printf("Precision is %f, recall is %f\n",precision,recall);
	}
	PQfinish(psql);
}

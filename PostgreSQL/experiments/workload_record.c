/* Executes a number of different rec_workload programs, then
 * records the results. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

double execute_workload(char *recommender, int total, int heavy, int alpha, int beta, int gamma, int delta) {
	int i,j;
	struct timeval start_time, end_time;
	double total_time = 0.0;
	pid_t my_id;
	char **commandvp;

	// Prepare the command.
	commandvp = (char**) malloc(8*sizeof(char*));
	for (i = 0; i < 8; i++)
		commandvp[i] = (char*) malloc(32*sizeof(char));
	sprintf(commandvp[0],"./rec_workload");
	sprintf(commandvp[1],"%s",recommender);
	sprintf(commandvp[2],"%d",total);
	sprintf(commandvp[3],"%d",heavy);
	sprintf(commandvp[4],"%d",alpha);
	sprintf(commandvp[5],"%d",beta);
	sprintf(commandvp[6],"%d",gamma);
	sprintf(commandvp[7],"%d",delta);

	my_id = fork();

	// Start the timer. It's fine to do this post-fork, so we don't record the
	// time taken to actually fork.
	j = gettimeofday(&start_time,NULL);

	if (my_id == 0) {
		execlp(commandvp[0], commandvp[0], commandvp[1], commandvp[2], commandvp[3],
			commandvp[4], commandvp[5], commandvp[6], commandvp[7], NULL);
	} else {
		int status;
		wait(&status);
	}

	// Stop the timer.
	j = gettimeofday(&end_time,NULL);
	total_time += (((double)(end_time.tv_sec - start_time.tv_sec))*1000000.0);
	total_time += end_time.tv_usec - start_time.tv_usec;

	// Return the time taken (in seconds).
	return total_time / 1000000.0;
}

int main(int argc, char *argv[]) {
	int i,j;
	char *recommender;
	double time;

	if (argc != 2) {
		printf("Usage: ./workload_record recommender\n");
		exit(0);
	}
	recommender = argv[1];

	time = execute_workload(recommender, 100, 80, 50, 0, 50, 0);
	printf("Total time: %f seconds\n",time);
}

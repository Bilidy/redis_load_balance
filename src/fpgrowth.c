/*
 * fpgrowth.c
 * FP-growth algorithm for finding frequent item sets
 */
#include "redis.h"
#include "tract.h"
#include "fptree.h"


/*----------------------------------------------------------------------
  Preprocessor Definitions
----------------------------------------------------------------------*/
#define PRGNAME     "fpgrowth"
#define DESCRIPTION "find frequent item sets " \
                    "with the fpgrowth algorithm"

#define TIME_FLY(start)  ((clock()-(start)) /(double)CLOCKS_PER_SEC)
int mining();


int NumberOfFrequentSets = 0;               /* like the variable name */
static FILE *in = NULL;                     /* input file */
static FILE *out = NULL;                    /* output file */
static NodeLink *hashTable = NULL;          /* array of Nodes in a hash table */
static FpHeadTable *headTable = NULL;       /* item head table */
static ItemLink *tracts = NULL;             /* array of transactions */
static FpTree *fptree = NULL;               /* frequent pattern tree */

int mining(int transnum)
{
	clock_t start = clock();                /* to compute the execution time */
	char *s;                                /* to traverse the options */
	char *inFile = REDIS_DEFAULT_IB_TRANS_FILENAME;                /* File containing all the transactions(input file name) */
	char *outFile = REDIS_DEFAULT_IB_FIM_FILENAME;              /* output file name */
	double support = 50;                     /* Support Threshold (in percent) */
	int transNum = transnum;				/* transaction number */
	int itemsNum = 500;                     /* distinct item number */
	int i;                                  /* for traversing the argvs */

	if(support > 100) {                       /* check the limit for support threshold */
			printf("invalid minimum support %g%%\n", support);
			return -1;
	}
	support = ceil((support >= 0) ? 
				0.01 * support * transNum : -support);/* compute absolute support value */
	if(inFile && *inFile){
			in = fopen(inFile, "r");
	} else {
			inFile = "<stdin>";
			in = stdin;
	}
	if(outFile && *outFile){
		out = fopen(outFile, "a+");
	} else {
		outFile = "<stdout>";
		out = stdout;
	}
	/*--- initial some structures ---*/
	hashTable = (NodeLink *)calloc(itemsNum, sizeof(NodeLink));
	tracts = (ItemLink *)calloc(transNum, sizeof(ItemLink));

	findFItemset(in, tracts, hashTable, (int)support, transNum, itemsNum);
	deleteUnsupport(tracts, transNum, (int)support, hashTable, itemsNum);
	sortEveryTract(tracts, transNum, hashTable, itemsNum, (int)support);

	int numExceeded = getNumOfExceeded(hashTable, itemsNum);
	headTable = (FpHeadTable *)malloc(sizeof(FpHeadTable));

	if(!headTable){
		perror("out of memory!!!");return -1;;
	}

	createHeadTable(hashTable, itemsNum, headTable, numExceeded);
	fptree = (FpTree *)malloc(sizeof(FpTree));

	if(!fptree){
		perror("out of memory!!!");return -1;;
	}
	createFpTree(fptree, headTable, tracts, transNum);

	fpgrowth(fptree, NULL, (int)support, out);

	//fprintf(out, "Number of frequent sets: [%d]\n", NumberOfFrequentSets);
	//fprintf(out, "Time consume: [%.2fs]\n", TIME_FLY(start));

  	fclose(in);
	fclose(out);
	
	return 0;
}

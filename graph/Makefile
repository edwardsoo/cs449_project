################################################################################
## Parameters
################################################################################
n=4# Number of OS processes
nfg=100# Number of collocated process per OS process
nApps=$(n)# Number of AppNodes
t=0# Number of Tests each App Node will generate
l=32# Max level of SkipList Towers
p=# Other compiler parameters
k=20# Number of sends before an Ssend (for flow-control)
w=10# Window size for AppNodes to oversubscribe the data structure
b=1000# Batch size if batching is used

IP=0# Insert Probability
DP=0# Delete  Probability
RFP=0# Range Find Probability

AS=1# Should we use APP  shortcuts
LS=0# Should we use LIST shortcuts
ZMQ=1# Should we use ZeroMQ based web-app as front-end
ZMQin=127.0.0.1:5555# ZeroMQ source address and port
ZMQdiscovery=127.0.0.1:11111# ZeroMQ Discovery Address

ME=1# Should we do Manual Experiment
MD=2# Mask dimension for Range Query
ST=1# Special Test ON or OFF
SP=100# Span for the Range Query to be used
TP=50# Tower Toss Probability
LT=0# Latency Threshold to increase or decrease Window Limit,W, in app processes
ITER=1000# Number of iterations before an MPI_Wtime() is called

x=0# Boolean variable deciding whether to specify '-enable-x' when running mpiexec
ndb=1# Boolean variable deciding whether to use -DNODEBUG in other parameters 

mf=./nodefile # Machine file to use with mpiexec
prefix=

FILEP=
ifneq ($(FILEP),)
	f=$(FILEP)
else
	f=# Optional Filename for dumping latency and throughput information
endif



################################################################################
## CC and EXEC
################################################################################
GENVLIST=-genv SCHEDULER block -genv NUMBER_OF_TESTS $(t) -genv NUMBER_OF_APP_PROCESSES $(nApps) -genv K $(k) -genv W $(w) -genv B $(b) -genv INSERT_PROBABILITY $(IP) -genv DELETE_PROBABILITY $(DP) -genv RANGE_FIND_PROBABILITY $(RFP) -genv USESHORTCUTS $(AS) -genv USESHORTCUTS_IN_LIST $(LS) -genv MANUAL_EXPERIMENT $(ME) -genv MASK_DIMENSION $(MD) -genv RANGE_QUERY_SPAN $(SP) -genv SPECIAL_TEST $(ST) -genv ITERATIONS_BEFORE_A_TIME $(ITER) -genv TOWER_TOSS_PROBABILITY $(TP) -genv LATENCY_THRESHOLD $(LT) -genv USE_ZERO_MQ $(ZMQ) -genv ZERO_MQ_PULL_SOURCE $(ZMQin) -genv ZERO_MQ_PUSH_SINK $(ZMQdiscovery)  
ifneq ($(f),)
	GENVLIST+= -genv dumpfilename $(f)
endif

ifeq ($(x),1)
	exec=$(prefix)mpiexec -enable-x $(GENVLIST) 
else
	exec=$(prefix)mpiexec $(GENVLIST)
endif

################################################################################
## Executables
################################################################################

exe2b=DistributedSkipListBatched

target=$(exe2b)



################################################################################
## Phony Targer Declaration 
################################################################################
.PHONY: clean cleandump all slb srunb testsrunb cleanconfig

################################################################################
## MAIN TARGETS 
################################################################################
first_target: all

all: zmqdemo


################################################################################
## Demo Targets 
################################################################################
zmqdemo:
	$(MAKE) srunb n=$(n) nfg=$(nfg) b=$(b) w=$(w) k=$(k) t=0 IP=$(IP) DP=$(DP) RFP=$(RFP) AS=$(AS) LS=$(LS) MD=2 ST=1 SP=$(SP) TP=$(TP) LT=$(LT) ITER=$(ITER) demo=0 ME=1 ZMQ=1 ndb=1


################################################################################
## Cleanup Targets 
################################################################################
	


################################################################################
## Run Targets 
################################################################################
srunb: $(exe2b)
	$(exec) -nfg $(nfg) -n $(n) ./$(exe2b)


################################################################################
## Run Targets with Machinefile 
################################################################################
testsrunb: $(exe2b)
	$(exec) -machinefile $(mf) -nfg $(nfg) -n $(n) ./$(exe2b)




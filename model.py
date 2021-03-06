from math_utils import *
from debug_utils import *

## Arrival rate for a given average offered load on G/G/c
## S: Service time
## Sl: Service time slowdown
def ar_MGc_forAGivenRo(ro, c, S, Sl):
	return ro*c/S.mean()/Sl.mean()

## When arriving jobs consist of k tasks
def ar_MGc_wParallelJobs_forAGivenRo(ro, c, k, S, Sl):
	return ro*c/k.mean()/S.mean()/Sl.mean()

def EW_MMc(ar, X, c):
	EX = X.moment(1)
	ro = ar*EX/c
	C = 1/(1 + (1-ro)*G(c+1)/(c*ro)**c * sum([(c*ro)**k/G(k+1) for k in range(c) ] ) )
	# EN = ro/(1-ro)*C + c*ro
	return C/(c/EX - ar)

def EW_MGc(ar, X, c):
	EX, EX2 = X.moment(1), X.moment(2)
	CV = math.sqrt(EX2 - EX**2)/EX
	return (1 + CV**2)/2 * EW_MMc(ar, EX, c)

def EW_MG1(ar, X):
	EX, EX2 = X.moment(1), X.moment(2)
	if ar*EX >= 1:
		return None

	return ar*EX2/2/(1 - ar*EX)

def ET_MG1(ar, X):
	EX = X.moment(1)
	EW = EW_MG1(ar, X)
	return EX + EW if EW is not None else None

"""
Assumes there can be at most one overloaded edge,
and that is edge-0 without loss of generality.
E.g.,
edgeId_info_l = {
	0:
	{
		'ar': ...,
		'c': ...,
		'toEdgeId_probSching_m': {1: ..., 2: ..., },
		'toEdgeId_latency_m': {1: ..., 2: ..., }
	},
	1:
	{
		'ar': ...,
		'c': ...
	}
}

Job service times S ~ Exp(1)
Job transmission times X ~ Exp(1)
"""
def EW_edge_markovian(edgeId_info_l, S, X):
	log(DEBUG, "started;", edgeId_info_l=edgeId_info_l)

	N = len(edgeId_info_l) # number of edge clouds
	check(N > 1, "There needs to be at least two edge clouds.")

	edge0Info_m = edgeId_info_l[0]
	check('toEdgeId_probSching_m' in edge0Info_m, "Info for the overloaded edge node should contain the inter-edge scheduling probabilities.")
	edgeId_probSchingTo_m = edge0Info_m['toEdgeId_probSching_m']

	ar_edge0 = edge0Info_m['ar']
	cumProbSchingTo = 0
	edgeId_netAR_m = {}
	for i in range(1, N):
		check(i in edgeId_probSchingTo_m, "i= {} is not in edgeId_probSchingTo_m".format(i))
		probSchingTo = edgeId_probSchingTo_m[i]
		edgeId_netAR_m[i] = edgeId_info_l[i]['ar'] + probSchingTo*ar_edge0

		cumProbSchingTo += probSchingTo
	edgeId_netAR_m[0] = (1 - cumProbSchingTo) * ar_edge0
	log(DEBUG, "", edgeId_netAR_m=edgeId_netAR_m)

	edgeId_toEdgeId_delayInfo_m = {i: {} for i in range(N)}
	## Delay experienced by jobs that are not moved
	EW = 0
	for i in range(N):
		toEdgeId_delayInfo_m = {}

		netAR = edgeId_netAR_m[i]
		EW = EW_MMc(netAR, S, edgeId_info_l[i]['c'])
		ar = edgeId_info_l[i]['ar']
		toEdgeId_delayInfo_m[i] = {'ar': netAR if i == 0 else ar, 'EDelay': EW}

		if i == 0:
			check('toEdgeId_latency_m' in edge0Info_m, "Info for the overloaded edge node should contain the inter-edge latencies.")
			toEdgeId_latency_m = edge0Info_m['toEdgeId_latency_m']
			toEdgeId_probSching_m = edge0Info_m['toEdgeId_probSching_m']
			for j in range(1, N):
				ar_toEdgeId = toEdgeId_probSching_m[j] * ar
				ENetDelay = 2*toEdgeId_latency_m[j] + ET_MG1(ar_toEdgeId, X)
				toEdgeId_delayInfo_m[j] = {'ar': ar_toEdgeId, 'EDelay': ENetDelay + EW}

		edgeId_toEdgeId_delayInfo_m[i] = toEdgeId_delayInfo_m
	log(DEBUG, "", edgeId_toEdgeId_delayInfo_m=edgeId_toEdgeId_delayInfo_m)

	EDelay = 0
	ar_total = sum(edgeId_info_l[i]['ar'] for i in range(N))
	for i in range(N):
		for j, delayInfo_m in edgeId_toEdgeId_delayInfo_m[i].items():
		EDelay += delayInfo_m['ar']/ar_total * delayInfo_m['EDelay']

	return EDelay

if __name__ == "__main__":
	N, Cap = 10, 10
	b, beta = 10, 5
	a, alpha = 1, 2
	k = BZipf(1, 1)
	r = 1

	S = Exp(1)
	X = Exp(1)
	edgeId_info_l = [
		{
			'ar': ar_MGc_forAGivenRo(ro=0.3, c=10, S=S, Sl=BZipf(1, 1)),
			'c': 10,
			'toEdgeId_probSching_m': {1: 0.2},
			'toEdgeId_latency_m': {1: 20}
		},
		{
			'ar': ar_MGc_forAGivenRo(ro=0.2, c=10, S=S, Sl=BZipf(1, 1)),
			'c': 10
		}
	]

	EW = EW_edge_markovian(edgeId_info_l, S, X)
	log(DEBUG, "EW= {}".format(EW))

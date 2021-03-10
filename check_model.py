from sim_objs import *
from model import *

# #######################################  Sim  ########################################### #
def sim(sinfo_m):
	env = simpy.Environment()
	mapping_m = {'type': 'packing'}
	sching_m = {'type': 'plain'}
	cl = Cluster(env, scher=Scheduler(mapping_m, sching_m), **sinfo_m)
	jg = JobGen(env, out=cl, **sinfo_m)
	env.run(until=cl.wait_for_alljobs)

	numJobs_wFate, numJobs_dropped = 0, 0
	numJobs_waitedInQ = 0
	serviceTime_l = []
	waitTime_l = []
	responseTime_l = []
	jobSlowdown_l, serviceSlowdown_l = [], []
	for jid, info in cl.jid_info_m.items():
		if 'fate' in info:
			numJobs_wFate += 1

			fate = info['fate']
			if fate == 'dropped':
				numJobs_dropped += 1
			elif fate == 'finished':
				serviceTime_l.append(info['run_time'] )
				serviceSlowdown_l.append(info['run_time']/info['expected_run_time'] )
				jobSlowdown_l.append(
					(info['wait_time'] + info['run_time'] )/info['expected_run_time'] )
				waitTime_l.append(info['wait_time'] )
				responseTime_l.append(info['wait_time'] + info['run_time'] )
				if info['wait_time'] > 0: # 0.01:
					numJobs_waitedInQ += 1
	fractionOfJobsWaitedInQ = numJobs_waitedInQ/len(cl.jid_info_m)
	blog(numJobs_dropped=numJobs_dropped, numJobs_wFate=numJobs_wFate, fractionOfJobsWaitedInQ=fractionOfJobsWaitedInQ)

	return {
		'dropRate': numJobs_dropped/len(cl.jid_info_m),
		'servTime_mean': np.mean(serviceTime_l),
		'waitTime_mean': np.mean(waitTime_l),
		'jobSlowdown_mean': np.mean(jobSlowdown_l),
		'serviceSlowdown_mean': np.mean(serviceSlowdown_l),
		'fractionOfJobsWaitedInQ': fractionOfJobsWaitedInQ,
		'responseTime_mean': np.mean(responseTime_l) }

def plot_EW_MGc_simVsModel():
	N = 10
	S = Exp(1) # TPareto(1, 10, 2)
	Sl = BZipf(1, 1)
	ar = ar_MGc_forAGivenRo(ro, N, S, Sl) round(ar_for_ro_pareto(1/2, N, Cap, k, b, beta, a, alpha_gen), 2)

	sinfo_m = {
		'ar': ar, 'njob': 2000*N, 'nworker': N, 'wcap': Cap,
		'lifetime_rv': Pareto(b, beta),
		'reqed_rv': DUniform(1, 1),
		'k_rv': k,
		'straggle_m': {'slowdown': lambda load: Sl.sample() } }
	mapping_m = {'type': 'spreading'}
	sching_m = {'type': 'expand_if_totaldemand_leq', 'r': r, 'threshold': None}

	blog(sinfo_m=sinfo_m, mapping_m=mapping_m, sching_m=sching_m)

	def plot_wrt_d():
		d_l = []
		# ro_wrep_l, Esl_wrep_l = [], []
		ro_wcoding_l, Esl_wcoding_l = [], []
		sim_ro_wcoding_l, sim_Esl_wcoding_l = [], []
		l, u = a*b, 1000
		for d in np.logspace(math.log10(l), math.log10(u), 5):
			d = round(d, 2)
			print("\n>> d= {}".format(d) )
			d_l.append(d)

			# red = 'Rep'
			# ro = ro_pareto(ar, N, Cap, k, r, b, beta, a, alpha_gen, d, red)
			# Esl = Esl_pareto(ro, N, Cap, k, r, b, beta, a, alpha_gen, d, red) if ro is not None else None
			# blog(ro=ro, Esl=Esl)
			# ro_wrep_l.append(ro)
			# Esl_wrep_l.append(Esl)

			red = 'Coding'
			ro = ro_pareto(ar, N, Cap, k, r, b, beta, a, alpha_gen, d, red)
			Esl = Esl_pareto(ro, N, Cap, k, r, b, beta, a, alpha_gen, d, red) if ro is not None else None
			blog(ro=ro, Esl=Esl)
			ro_wcoding_l.append(ro)
			Esl_wcoding_l.append(Esl)

			sching_m['threshold'] = d
			sim_m = sim(sinfo_m, mapping_m, sching_m, 'd{}'.format(sching_m['threshold'] ) )
			blog(sim_m=sim_m)
			sim_ro = sim_m['load_mean']
			sim_Esl = sim_m['serv_sl_mean']
			# blog(sim_ro=sim_ro, sim_Esl=sim_Esl)
			sim_ro_wcoding_l.append(sim_ro)
			sim_Esl_wcoding_l.append(sim_Esl)
		#
		fig, axs = plot.subplots(1, 2)
		fontsize = 14
		ax = axs[0]
		plot.sca(ax)
		# plot.plot(d_l, ro_wrep_l, label='w/ Rep', c='blue', marker=next(marker_c), ls=':', mew=1)
		plot.plot(d_l, ro_wcoding_l, label='w/ Coding', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
		plot.plot(d_l, sim_ro_wcoding_l, label='Sim, w/ Coding', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
		prettify(ax)
		plot.legend()
		plot.xscale('log')
		plot.xlabel('d', fontsize=fontsize)
		plot.ylabel('Average load', fontsize=fontsize)
		ax = axs[1]
		plot.sca(ax)
		# plot.plot(d_l, Esl_wrep_l, label='w/ Rep', c='blue', marker=next(marker_c), ls=':', mew=1)
		plot.plot(d_l, Esl_wcoding_l, label='w/ Coding', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
		plot.plot(d_l, sim_Esl_wcoding_l, label='Sim, w/ Coding', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
		prettify(ax)
		plot.xscale('log')
		plot.legend()
		plot.xlabel('d', fontsize=fontsize)
		plot.ylabel('Average slowdown', fontsize=fontsize)

	def plot_wrt_ar():
		sching_m['threshold'] = float('Inf')
		ar_ub = ar_for_ro_pareto(0.9, N, Cap, k, b, beta, a, alpha_gen)
		sim_ro_wcoding_l, sim_Esl_wcoding_l = [], []
		for ar in np.linspace(ar_ub/10, ar_ub, 5):
			sinfo_m['ar'] = ar
			sim_m = sim(sinfo_m, mapping_m, sching_m)
			blog(sim_m=sim_m)
			sim_ro = sim_m['load_mean']
			sim_Esl = sim_m['serv_sl_mean']
			# blog(sim_ro=sim_ro, sim_Esl=sim_Esl)
			sim_ro_wcoding_l.append(sim_ro)
			sim_Esl_wcoding_l.append(sim_Esl)

	plot.subplots_adjust(hspace=2)
	st = plot.suptitle(r'$N= {}$, $C= {}$, $k \sim$ {}, r= {}'.format(N, Cap, k, r) + '\n' + r'$b= {}$, $\beta= {}$, $a= {}$, $\alpha= {}$'.format(b, beta, a, alpha) )
	plot.gcf().set_size_inches(2*5, 5)
	plot.savefig('plot_ro_Esl.png', bbox_extra_artists=(st,), bbox_inches='tight')
	plot.gcf().clear()
	log(INFO, "done.")

def plot_ET_wrt_d():
	N, Cap = 20, 10
	k = BZipf(1, 5) # DUniform(1, 1)
	R = Uniform(1, 1)
	b, beta = 10, 4
	L = Pareto(b, beta) # TPareto(10, 10**6, 4)
	a, alpha = 1, 10 # 1, 3
	Sl = Pareto(a, alpha) # Uniform(1, 1)
	def alpha_gen(ro):
		return alpha
	ro = 0.6
	red, r = 'Coding', 2
	print("ro= {}".format(ro) )

	ar = round(ar_for_ro(ro, N, Cap, k, R, L, Sl), 2)
	sinfo_m.update({
		'njob': 5000*N,
		'nworker': N, 'wcap': Cap, 'ar': ar,
		'k_rv': k,
		'reqed_rv': R,
		'lifetime_rv': L,
		'straggle_m': {'slowdown': lambda load: Sl.sample() } } )
	sching_m = {'type': 'expand_if_totaldemand_leq', 'r': r, 'threshold': None}
	log(INFO, "", sinfo_m=sinfo_m, sching_m=sching_m, mapping_m=mapping_m)

	def run(d, nrun=1):
		sching_m['threshold'] = d
		sum_ET, sum_EW, sum_Prqing = 0, 0, 0
		for i in range(nrun):
			print("> i= {}".format(i) )
			sim_m = sim(sinfo_m, mapping_m, sching_m, "N{}_C{}".format(N, Cap) )
			blog(sim_m=sim_m)
			sum_ET += sim_m['responsetime_mean']
			sum_EW += sim_m['waittime_mean']
			sum_Prqing += sim_m['fractionOfJobsWaitedInQ']
		return sum_ET/nrun, sum_EW/nrun, sum_Prqing/nrun

	l = L.l_l*Sl.l_l
	u = 40*L.mean()*Sl.mean()
	d_l, sim_ET_l, ET_wMGc_l, approx_ET_wMGc_l, ET_l = [], [], [], [], []
	for d in [0, *np.logspace(math.log10(l), math.log10(u), 20) ]:
	# for d in np.logspace(math.log10(l), math.log10(u), 40):
		print("\n>> d= {}".format(d) )
		sim_ET, sim_EW, sim_Prqing = 0, 0, 0 # run(d)
		if d == 0:
			sim_ET0 = sim_ET

		print("*** sim_ET= {}, sim_EW= {}".format(sim_ET, sim_EW, sim_Prqing) )
		ET_wMGc, EW_wMGc, Prqing_wMGc = ET_EW_Prqing_pareto_wMGc(ro, N, Cap, k, r, b, beta, a, alpha_gen, d, red)
		print("*** ET_wMGc= {}, EW_wMGc= {}, Prqing_wMGc= {}".format(ET_wMGc, EW_wMGc, Prqing_wMGc) )
		approx_ET_wMGc, approx_EW_wMGc, approx_Prqing_wMGc = approx_ET_EW_Prqing_pareto_wMGc(ro, N, Cap, k, r, b, beta, a, alpha_gen, d, red)
		print("*** approx_ET_wMGc= {}, approx_EW_wMGc= {}, approx_Prqing_wMGc= {}".format(approx_ET_wMGc, approx_EW_wMGc, approx_Prqing_wMGc) )
		# ET, EW = ET_EW_pareto(ro, sim_EW0, N, Cap, k, r, b, beta, a, alpha_gen, d, red)
		# print("*** ET= {}, EW= {}".format(ET, EW) )

		d_l.append(d)
		sim_ET_l.append(sim_ET)
		ET_wMGc_l.append(ET_wMGc)
		approx_ET_wMGc_l.append(approx_ET_wMGc)
		# ET_l.append(ET)
		if sim_ET > 3*sim_ET0:
			break
		# elif ET_wMGc is None:
		#   break
	blog(d_l=d_l, sim_ET=sim_ET_l, ET_wMGc_l=ET_wMGc_l, approx_ET_wMGc_l=approx_ET_wMGc_l)
	# plot.plot(d_l, sim_ET_l, label='Sim', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
	plot.plot(d_l, ET_wMGc_l, label='M/G/c model', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
	plot.plot(d_l, approx_ET_wMGc_l, label='Approx M/G/c model', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
	# plot.plot(d_l, ET_l, label='Heavy-tail model', c=next(darkcolor_c), marker=next(marker_c), ls=':', mew=1)
	prettify(plot.gca() )
	plot.legend(loc='best', framealpha=0.5)
	plot.xscale('log')
	fontsize = 14
	plot.xlabel('d', fontsize=fontsize)
	plot.ylabel('E[T]', fontsize=fontsize)
	plot.title(r'$N= {}$, $C= {}$, $\rho_0= {}$, $r= {}$, $k \sim$ {}'.format(N, Cap, ro, r, k) + '\n' + r'$R \sim$ {}, $L \sim$ {}, $Sl \sim$ {}'.format(R, L, Sl) )
	plot.gcf().set_size_inches(5, 5)
	plot.savefig('plot_ET_wrt_d.png', bbox_inches='tight')
	plot.gcf().clear()
	log(INFO, "done.")

if __name__ == "__main__":
	N, Cap = 10, 10
	b, beta = 10, 5
	a, alpha = 1, 2
	k = BZipf(1, 1)
	r = 1
	# log(INFO, "", k=k, r=r, b=b, beta=beta, a=a, alpha=alpha)
	def alpha_gen(ro):
		return alpha
	Sl = Pareto(a, alpha)
	ar = round(ar_for_ro_pareto(1/2, N, Cap, k, b, beta, a, alpha_gen), 2)

	sinfo_m = {
		'ar': ar, 'njob': 2000*N, 'nworker': N, 'wcap': Cap,
		'lifetime_rv': Pareto(b, beta),
		'reqed_rv': DUniform(1, 1),
		'k_rv': k,
		'straggle_m': {'slowdown': lambda load: Sl.sample() } }
	mapping_m = {'type': 'spreading'}
	sching_m = {'type': 'expand_if_totaldemand_leq', 'r': r, 'threshold': None}
	# blog(sinfo_m=sinfo_m, mapping_m=mapping_m, sching_m=sching_m)

	# check_MGc_assumption()
	plot_ET_wrt_d()

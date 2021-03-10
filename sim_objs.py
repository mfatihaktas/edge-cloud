import collections
# import concurrent.futures
from operator import itemgetter

class Task():
	def __init__(self, _id, jobId, reqedResCap, servTime, k, type_=None):
		self._id = _id
		self.jobId = jobId
		self.reqedResCap = reqedResCap
		self.servTime = servTime
		self.k = k
		self.type_ = type_

		self.remServTime = None
		self.prev_hop_id = None
		self.binding_time = None
		self.run_time = None

	def __repr__(self):
		return "Task[id= {}, jobId= {}, remServTime= {}]".format(self._id, self.jobId, self.remServTime)

class Job(object):
	def __init__(self, _id, k, n, reqedResCap, lifetime):
		self._id = _id
		self.k = k
		self.n = n
		self.reqedResCap = reqedResCap
		self.lifetime = lifetime

		self.wait_time = None

	def __repr__(self):
		return "Job[id= {}, k= {}, reqedResCap= {}, lifetime= {}]".format(self._id, self.k, self.reqedResCap, self.lifetime)

class JobGen(object):
	def __init__(self, env, ar, reqedResCap_rv, servTime_rv, k_rv, out, **kwargs):
		self.env = env
		self.ar = ar
		self.reqedResCap_rv = reqedResCap_rv
		self.servTime_rv = servTime_rv
		self.k_rv = k_rv
		self.out = out

		self.numJobsSent = 0

		self.action = self.env.process(self.run_poisson() )

	def run_poisson(self):
		while 1:
			yield self.env.timeout(random.expovariate(self.ar) )
			self.numJobsSent += 1
			k = self.k_rv.sample()
			self.out.put(
				Job(_id = self.numJobsSent,
						k = k, n = k,
						reqed = self.reqedResCap_rv.sample(),
						lifetime = self.servTime_rv.sample() ) )

def map_to_key__val_l(m):
	m = collections.OrderedDict(sorted(m.items() ) )
	k_l, v_l = [], []
	for k, v in m.items():
		k_l.append(k)
		v_l.append(v)
	return k_l, v_l

class Worker():
	def __init__(self, env, _id, cap, out_c, straggle_m):
		self.env = env
		self._id = _id
		self.cap = cap
		self.out_c = out_c
		self.straggle_m = straggle_m

		self.t_l = []
		self.got_busy = None
		self.sinterrupt = None
		self.add_to_serv = False
		self.cancel = False
		self.cancel_jid = None
		env.process(self.run() )

		self.sl = self.straggle_m['slowdown']
		self.t_load_m = {}

	def __repr__(self):
		return "Worker[id= {}]".format(self._id)

	def sched_cap(self):
		if len(self.t_l) == 0:
			return 0
		return sum([t.reqed for t in self.t_l] )

	def nonsched_cap(self):
		return self.cap - self.sched_cap()

	def sched_load(self):
		return self.sched_cap()/self.cap

	def avg_load(self):
		t_load_m = collections.OrderedDict(sorted(self.t_load_m.items() ) )
		load_weighted_sum = 0
		_t, t, _load = 0, 1, 0
		for t, load in t_load_m.items():
			load_weighted_sum += (t - _t)*_load
			_t, _load = t, load
		return load_weighted_sum/t

	def run(self):
		while True:
			if len(self.t_l) == 0:
				time_gotidle = self.env.now
				self.got_busy = self.env.event()
				yield (self.got_busy)
				self.got_busy = None
				slog(DEBUG, self.env, self, "got busy!", None)

				self.t_load_m[time_gotidle] = 0
				self.t_load_m[self.env.now] = 0

			remServTime_l = [t.remServTime for t in self.t_l]
			serv_time = min(remServTime_l)
			i_min = remServTime_l.index(serv_time)
			slog(DEBUG, self.env, self, "back to serv; serv_time= {}".format(serv_time), self.t_l[i_min] )
			start_t = self.env.now

			self.t_load_m[self.env.now] = self.sched_load()
			self.sinterrupt = self.env.event()
			yield (self.sinterrupt | self.env.timeout(serv_time) )
			serv_time_ = self.env.now - start_t
			if self.add_to_serv:
				for t in self.t_l[:-1]:
					t.remServTime -= serv_time_
			else:
				for t in self.t_l:
					t.remServTime -= serv_time_
			#
			if self.add_to_serv:
				slog(DEBUG, self.env, self, "new task added to serv", None)
				self.sinterrupt = None
				self.add_to_serv = False
			elif self.cancel:
				for t in self.t_l:
					if t.jid == self.cancel_jid:
						slog(DEBUG, self.env, self, "cancelled task in serv", t)
						self.t_l.remove(t)
						break
				self.sinterrupt = None
				self.cancel = False
			else:
				t = self.t_l.pop(i_min)
				slog(DEBUG, self.env, self, "serv done", t)

				t.run_time = self.env.now - t.binding_time
				t.prev_hop_id = self._id
				self.out_c.put_c(t)
				slog(DEBUG, self.env, self, "finished", t)

	def put(self, t):
		slog(DEBUG, self.env, self, "put:: starting;", t)
		avail_cap = self.nonsched_cap()
		if t.type_ == 's' and t.reqed > avail_cap:
			tred_l = [t for t in self.t_l if t.type_ == 'r']
			i = 0
			while i < len(tred_l) and avail_cap < t.reqed:
				tred = tred_l[i]
				avail_cap += tred.reqed
				self.t_l.remove(tred)
				i += 1
			if avail_cap < t.reqed:
				slog(ERROR, self.env, self, "could not bind", t)
				return
		elif t.type_ == 'r' and t.reqed > avail_cap:
			return

		_l = len(self.t_l)
		t.binding_time = self.env.now
		t.remServTime = t.lifetime*self.sl(self.sched_load() )
		self.t_l.append(t)
		if _l == 0:
			self.got_busy.succeed()
		else:
			self.add_to_serv = True
			self.sinterrupt.succeed()
		slog(DEBUG, self.env, self, "binded, njob= {}".format(len(self.t_l) ), t)

	def put_c(self, m):
		slog(DEBUG, self.env, self, "received", m)
		if m['message'] == 'remove':
			self.cancel = True
			self.cancel_jid = m['jid']
			self.sinterrupt.succeed()
		else:
			log(ERROR, "Unrecognized message;", m=m)

class Cluster():
	def __init__(self, env, njob, nworker, wcap, straggle_m, scher, **kwargs):
		self.env = env
		self.njob = njob
		self.nworker = nworker
		self.wcap = wcap
		self.straggle_m = straggle_m
		self.scher = scher

		self.w_l = [Worker(env, i, wcap, self, straggle_m) for i in range(nworker) ]

		self.store = simpy.Store(env)
		env.process(self.run() )

		self.njob_finished = 0
		self.store_c = simpy.Store(env)
		self.wait_for_alljobs = env.process(self.run_c() )

		self.jobId_task_l_m = {}
		self.jobId_info_m = {}

	def __repr__(self):
		return 'Cluster'

	def run(self):
		while True:
			j = yield self.store.get()

			while True:
				j.wait_time = self.env.now - j.arrival_time

				w_l = self.scher.schedule(j, self.w_l)
				if w_l is None:
					slog(DEBUG, self.env, self, "w_l is None", j)
					yield self.env.timeout(0.01)
				else:
					break

			self.jobId_info_m[j._id] = {'wait_time': self.env.now - j.arrival_time}
			wid_l = []
			for i, w in enumerate(w_l):
				type_ = 's' if i < j.k else 'r'
				w.put(Task(i+1, j._id, j.reqed, j.lifetime, j.k, type_) )
				yield self.env.timeout(0.0001)
				wid_l.append(w._id)

			self.jobId_task_l_m[j._id] = []
			self.jobId_info_m[j._id].update({'expected_run_time': j.lifetime,
																		 'workerId_l': wid_l})

	def put(self, j):
		slog(DEBUG, self.env, self, "received", j)
		j.arrival_time = self.env.now
		return self.store.put(j)

	def run_c(self):
		while True:
			t = yield self.store_c.get()
			try:
				self.jobId_task_l_m[t.jid].append(t)
			except KeyError: # may happen due to a task completion after the corresponding job finishes
				continue

			t_l = self.jobId_task_l_m[t.jid]
			if len(t_l) > t.k:
				log(ERROR, "len(t_l)= {} > k= {}".format(len(t_l), t.k) )
			elif len(t_l) < t.k:
				continue
			else:
				t_l = self.jobId_task_l_m[t.jid]
				workerId_recvedFrom_l = [t.prev_hop_id for t in t_l]
				workerId_sentTo_l = self.jobId_info_m[t.jid]['workerId_l']
				for w in self.w_l:
					if w._id in workerId_sentTo_l and w._id not in workerId_recvedFrom_l:
						w.put_c({'message': 'remove', 'jid': t.jid} )

				self.jobId_info_m[t.jid].update({
					'fate': 'finished',
					'run_time': max([t.run_time for t in self.jobId_task_l_m[t.jid] ] ) } )
				self.jobId_task_l_m.pop(t.jid, None)
				slog(DEBUG, self.env, self, "finished jid= {}".format(t.jid), t)

				## This causes (s1, a1, r1), (s2, a2, r2) to be interleaved by more than one job
				# self.njob_finished += 1
				# blog(njob_finished=self.njob_finished)
				# if self.njob_finished >= self.njob:
				#   return

				if t.jid <= self.njob:
					self.njob_finished += 1
					# log(WARNING, "job completion;", jid=t.jid, njob=self.njob, njob_finished=self.njob_finished)
					if self.njob_finished >= self.njob:
						return

	def put_c(self, t):
		slog(DEBUG, self.env, self, "received", t)
		return self.store_c.put(t)

class Mapper(object):
	def __init__(self, mapping_m):
		self.mapping_m = mapping_m

		if self.mapping_m['type'] == 'packing':
			self.get_worker_l = lambda j, w_l: self.get_worker_l_w_packing(j, w_l)
		elif self.mapping_m['type'] == 'spreading':
			self.get_worker_l = lambda j, w_l: self.get_worker_l_w_spreading(j, w_l)

	def __repr__(self):
		return 'Mapper[mapping_m= {}]'.format(self.mapping_m)

	def get_worker_l_w_packing(self, job, w_l):
		w_l_ = []
		for w in w_l:
			if job.reqed <= w.nonsched_cap():
				w_l_.append(w)
		return w_l_

	def get_worker_l_w_spreading(self, job, w_l):
		w_load_l = []
		for w in w_l:
			if job.reqed <= w.nonsched_cap():
				w_load_l.append((w, w.sched_load() ) )
		w_load_l.sort(key=itemgetter(1) )
		return [w for w, _ in w_load_l]

class Scheduler(object):
	def __init__(self, mapping_m, sching_m):
		self.sching_m = sching_m
		self.mapper = Mapper(mapping_m)

		self._type = 'Scheduler'
		self._id = 'Scheduler_a={}'.format(sching_m['a'] )
		self.s_len = 1

		if sching_m['type'] == 'plain':
			self.schedule = self.plain
		elif sching_m['type'] == 'expand_if_totaldemand_leq':
			self.schedule = self.expand_if_totaldemand_leq

	def __repr__(self):
		return 'Scheduler[sching_m={}, mapper= {}]'.format(self.sching_m, self.mapper)

	def plain(self, j, w_l, expand_job=False):
		w_l = self.mapper.get_worker_l(j, w_l)
		if len(w_l) < j.k:
			return None

		j.n = j.k
		if expand_job:
			j.n = int(j.n + self.sching_m['job_expansion_factor'])

		return w_l[:j.n] if len(w_l) >= j.n else None

	def expand_if_totaldemand_leq(self, j, w_l):
		D = j.k*j.reqed*j.lifetime

		expand = True if D < self.sching_m['threshold'] else False
		return self.plain(j, w_l, expand)

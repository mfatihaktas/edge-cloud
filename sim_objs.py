import simpy

from debug_utils import *

class Job():
	def __init__(self, _id, serv_time, typ='job', priority=False):
		self._id = _id
		self.serv_time = serv_time
		self.typ = typ
		self.priority = priority

	def __repr__(self):
		return "Job[id= {}, serv_time= {}, typ= {}, priority= {}]".format(self._id, self.serv_time, self.typ, self.priority)

class ProbeJob(Job):
	def __init__(self, _id, src_id):
		super().__init__(_id, serv_time=0, typ='probe')
		self.src_id = src_id

	def __repr__(self):
		return "ProbeJob[id= {}]".format(self._id)

class JobGen(object):
	def __init__(self, env, interarr_time_rv, serv_time_rv, out, **kwargs):
		self.env = env
		self.interarr_time_rv = interarr_time_rv
		self.serv_time_rv = serv_time_rv
		self.out = out

		self.num_jobs_sent = 0

		self.action = self.env.process(self.run())

	def __repr__(self):
		return "JobGen[interarr_time_rv= {}, serv_time_rv= {}]".format(self.interarr_time_rv, self.serv_time_rv)

	def run(self):
		while 1:
			yield self.env.timeout(self.interarr_time_rv.sample())

			self.num_jobs_sent += 1
			self.out.put(Job(_id = self.num_jobs_sent,
											 serv_time = self.serv_time_rv.sample()))

class Q(object):
	def __init__(self, _id, env):
		self._id = _id
		self.env = env

class FCFS(Q): # First Come First Serve
	def __init__(self, _id, env, out, speed=1):
		super().__init__(_id, env)
		self.out = out
		self.speed = speed

		self.store = simpy.Store(env)
		self.customer_l = []
		self.priority_customer_l = []

		self.action = env.process(self.run())

		self.busy_time = 0
		self.idle_time = 0

		self.num_served = 0
		self.avg_wait_time = 0

	def load(self):
		return self.busy_time / (self.busy_time + self.idle_time)

	def put(self, customer):
		slog(DEBUG, self.env, self, "recved", customer)
		customer.arrival_time = self.env.now
		if customer.priority:
			self.priority_customer_l.append(customer)
		else:
			self.customer_l.append(customer)

		return self.store.put(customer._id)

	def run(self):
		while True:
			idle_start = self.env.now
			yield self.store.get()
			self.idle_time += self.env.now - idle_start

			customer = self.priority_customer_l.pop(0) if (len(self.priority_customer_l) > 0) else self.customer_l.pop(0)

			wait_time = self.env.now - customer.arrival_time
			self.avg_wait_time = (self.avg_wait_time * self.num_served + wait_time) / (self.num_served + 1)

			busy_start = self.env.now
			t = customer.serv_time / self.speed
			slog(DEBUG, self.env, self, "will serve for t= {}".format(t), customer)
			yield self.env.timeout(t)
			slog(DEBUG, self.env, self, "done serving", customer)
			self.num_served += 1
			self.busy_time += self.env.now - busy_start

	def sendoff(self, customer):
		self.out.put(customer)

class PacketQ(FCFS):
	def __init__(self, _id, env, out, speed=1):
		super().__init__(_id, env, out, speed)

	def __repr__(self):
		return "PacketQ[_id= {}]".format(self._id)

class JobQ(FCFS):
	def __init__(self, _id, env, out, speed=1):
		super().__init__(_id, env, out, speed)

	def __repr__(self):
		return "JobQ[_id= {}]".format(self._id)

	def sendoff(self, job):
		if job.typ == 'probe':
			self.net.put(ProbePacket())
		elif job.typ == 'job':
			self.out.put(customer)

class Sink():
	def __init__(self, _id, env, num_jobs):
		self._id = _id
		self.env = env
		self.num_jobs = num_jobs

		self.store = simpy.Store(env)
		self.num_jobsRecvedSoFar = 0

		self.wait_forAllJobs = env.process(self.run())

	def __repr__(self):
		return "Sink[_id= {}, num_jobs= {}]".format(self._id, self.num_jobs)

	def put(self, job):
		slog(DEBUG, self.env, self, "recved, num_jobsRecvedSoFar= {}".format(self.num_jobsRecvedSoFar), job)
		return self.store.put(job)

	def run(self):
		while True:
			job = yield self.store.get()

			self.num_jobsRecvedSoFar += 1
			if self.num_jobsRecvedSoFar >= self.num_jobs:
				return

class Packet():
	def __init__(self, _id, src_id, dst_id, payload, typ, priority=False):
		self._id = _id
		self.src_id = src_id
		self.dst_id = dst_id
		self.payload = payload
		self.typ = typ
		self.priority = priority

	def __repr__(self):
		return "Packet[id= {}, src_id= {}, dst_id= {}, typ= {}, priority= {}]".format(self._id, self.src_id, self.dst_id, self.typ, self.priority)

class ProbePacket(Packet):
	def __init__(self, src_id, dst_id, job):
		super().__init__(job._id, src_id, dst_id, payload=job, typ='probe')

	def __repr__(self):
		return "ProbePacket[id= {}, src_id= {}, dst_id= {}]".format(self._id, self.src_id, self.dst_id)

class Network():
	def __init__(self, _id, env):
		self._id = _id
		self.env = env

		self.dst_id__q_m = {}

	def __repr__(self):
		return "Network[_id= {}]".format(self._id)

	def add_dst(self, dst):
		num_dsts = len(self.dst_id__q_m)
		self.dst_id__q_m[dst._id] = FCFS(_id='netQ{}'.format(num_dsts), self.env, speed=1, out=dst)
		log(DEBUG, "Added", dst=dst)

	def put(self, packet):
		slog(DEBUG, self.env, self, "recved", packet)
		check(packet.dst_id in self.dst_id__q_m, "All packets should have a destination.")
		self.dst_id__q_m[packet.dst_id].put(packet)

class EdgeCloud():
	def __init__(self, _id, env, net, out, peer_edgecloud_id_l=None):
		self._id = _id
		self.env = env
		self.net = net
		self.peer_edgecloud_id_l = peer_edgecloud_id_l

		# TODO: Should ideally be a G/G/n
		self.q = JobQ(_id, env, out)

		self.id_job_m = {}

	def __repr__(self):
		return "EdgeCloud[_id= {}]".format(self._id)

	def put(self, packet):
		check(packet.dst_id == self._id, "Packet arrived to wrong edge-cloud.")

		if packet.typ == 'probe':
			slog(DEBUG, self.env, self, "recved back probe", packet)
			if packet.job_id in self.id_job_m:
				job = self.id_job_m[packet.job_id]
				slog(DEBUG, self.env, self, "will send back the job", job)
				del self.id_job_m[packet.job_id]

				p = Packet(packet.job_id, src_id=self._id, dst_id=packet.src_id, payload=job, typ='job', priority=True)
				self.net.put(p)
			else:
				slog(DEBUG, self.env, self, "already sent the job", packet)
		elif packet.typ == 'job':
			job = packet.payload
			slog(DEBUG, self.env, self, "recved", job)

			if self.peer_edgecloud_id_l is None:
				return self.put_local(job)
			else:
				return self.put_wProbing(job)
		else:
			assert_("Unexpected packet.typ= {}".format(packet.typ))

	def put_local(self, job):
		return self.q.put(job)

	def put_wProbing(self, job):
		slog(DEBUG, self.env, self, "recved", job)

		job = packet.payload
		self.id_job_m[job._id] = job

		pj = ProbeJob(job._id, src_id=self._id)
		self.put_local(pj)

		for peer_id in self.peer_edgecloud_id_l:
			probe = ProbePacket(src_id=self._id, dst_id=peer_id, job=pj)
			self.net.put(probe)

class Controller():
	def __init__(self, _id, env, cloud_l):
		self._id = _id
		self.env = env
		self.cloud_l = cloud_l

class Controller_wProbing(Controller):
	def __init__(self, _id, env, cloud_l):
		super().__init__(_id, env, cloud_l)

	def __repr__(self):
		return "Controller_wProbing[_id= {}]".format(self._id)

	def put(self, job):
		slog(DEBUG, self.env, self, "recved", job)

		i = self.to_which_q(job)
		check(i < len(self.q_l), "i= {} should have been < len(q_l)= {}".format(i, len(self.q_l)))

		return self.q_l[i].put(job)

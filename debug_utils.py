import inspect, pprint


# #################################  Log  ################################# #
LOG_LEVEL = 0

INFO = 0
DEBUG = 1
WARNING = 2
ERROR = 3

def log(level: int, message: str, **kwargs):
	if LOG_LEVEL <= level:
		alog(level, message, kwargs)

# Always log
def alog(level: int, message: str, **kwargs):
	try:
		funcname = inspect.stack()[1][3]
	except IndexError:
		funcname = ''

	print("{}] {}:: {}".format(str(level), funcname, message))
	blog(kwargs)

# Block log
def blog(level: int, **kwargs):
	if LOG_LEVEL <= level:
		blog(kwargs)

def blog(**kwargs):
	for k, v in kwargs.items():
		print("  {}: {}".format(k, pprint.pformat(v) ) )

# ###############################  Sim log  ############################### #
def slog(level: int, env, caller: str, action: str, affected, **kwargs):
	"""
	Parameters
	----------
	env= simpy.Environment
	caller= string -- name of the sim component acting
	action= string
	affected= any -- whatever component being acted on/with e.g., packet
	"""
	if LOG_LEVEL <= level:
		print("{} t: {:.2f}] {} {}\n\t{}".format(str(level), env.now, caller, action, affected) )
		blog(kwargs)

# ###############################  Assert  ############################### #
def check(condition: bool, message: str, **kwargs):
	if not condition:
		alog(0, message, kwargs)
		raise AssertionError()

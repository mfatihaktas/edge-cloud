import inspect, pprint

# #################################  Log  ################################# #
DEBUG = 0
INFO = 1
WARNING = 2
ERROR = 3

LOG_LEVEL = INFO

level_label_m = {INFO: "INFO", DEBUG: "DEBUG", WARNING: "WARNING", ERROR: "ERROR"}

def level_to_label(level):
	if level in level_label_m:
		return level_label_m[level]
	return str(level)

def log(level: int, message: str, **kwargs):
	if LOG_LEVEL <= level:
		try:
			funcname = inspect.stack()[1][3]
		except IndexError:
			funcname = ''

		print("{}] {}:: {}".format(level_to_label(level), funcname, message))
		blog(**kwargs)

# Always log
def alog(level: int, message: str, **kwargs):
	try:
		funcname = inspect.stack()[1][3]
	except IndexError:
		funcname = ''

	print("{}] {}:: {}".format(level_to_label(level), funcname, message))
	blog(**kwargs)

# Block log
def blog(level: int, **kwargs):
	if LOG_LEVEL <= level:
		blog(kwargs)

def blog(**kwargs):
	for k, v in kwargs.items():
		print("  {}= {}".format(k, pprint.pformat(v) ) )

# ###############################  Sim log  ############################### #
SLOG_LEVEL = 3

"""
	env: simpy.Environment
	caller: string -- name of the sim component acting
	action: string
	affected: any -- whatever component being acted on/with e.g., packet
"""
def slog(level: int, env, caller: str, action: str, affected, **kwargs):
	if SLOG_LEVEL <= level:
		print("{} t: {:.2f}] {} {}\n\t{}".format(level_to_label(level), env.now, caller, action, affected) )
		blog(**kwargs)

# ###############################  Assert  ############################### #
def check(condition: bool, message: str, **kwargs):
	if not condition:
		try:
			funcname = inspect.stack()[1][3]
		except IndexError:
			funcname = ''

		print("ERROR] {}:: {}".format(funcname, message))
		blog(**kwargs)

		raise AssertionError()

def assert_(message: str, **kwargs):
	try:
		funcname = inspect.stack()[1][3]
	except IndexError:
		funcname = ''

	print("ERROR] {}:: {}".format(funcname, message))
	blog(**kwargs)

	raise AssertionError()

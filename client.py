import paho.mqtt.client as mqtt
import time
import glob
import os
import sys
import subprocess
import datetime
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
sys.path.append("/home/limbo4/parser") # change this!
#from limbo_parser.javad.jps_parser import JPSParser, JPSData
from limbo_parser.javad.greis import CODE_POS_VEL, CODE_SATIND
from limbo_parser.javad.greis import CODE_ELEVATION, CODE_AZIMUTH
from limbo_parser.javad.greis import _1r, _2r, _1p, _2p, rc
from limbo_parser.javad.greis import get_struct
from limbo_parser.javad.jps_stat import JPSStatistics

CODES = [CODE_POS_VEL, CODE_SATIND, CODE_ELEVATION, CODE_AZIMUTH,_1r, _2r, _1p, _2p, rc]

OBS = "obs1" # change this!
SITE = "site1" # change this!
RECPARAMS = ""
MAXDELAY = 900 # seconds
OUTPARAMS = ""
RAWPERIOD = 3600 # seconds
OUTPRODUCTS = ""
PRODPERIOD = 3600 # seconds
#LASTTIME = datetime.datetime.utcnow().timestamp() # seconds
LASTTIME = time.time()
LAT = -1
LON = -1
NEIGHBORS = ""
STARTSIZE = 0
path = '/home/limbo4/.limbo_data/' # path to data
newfile = None


def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print("Connected to broker")
		global Connected
		Connected = True
	else:
		print("Connection failed")

def on_message(client, userdata, message):
	print(message.topic, message.payload)
	if "recparams" in message.topic:
		global RECPARAMS
		RECPARAMS = message.payload.decode()
	if "maxdelay" in message.topic:
		global MAXDELAY
		MAXDELAY = int(message.payload.decode())
	if "outparams" in message.topic:
		global OUTPARAMS
		OUTPARAMS = message.payload.decode()
	if "rawperiod" in message.topic:
		global RAWPERIOD
		RAWPERIOD = int(str(message.payload.decode()))
		scheduler.reschedule_job('outrawparams',trigger='interval',seconds=RAWPERIOD)
	if "outproducts" in message.topic:
		global OUTPRODUCTS
		OUTPRODUCTS = message.payload.decode()
	if "prodperiod" in message.topic:
		global PRODPERIOD
		PRODPERIOD = int(message.payload.decode())
		scheduler.reschedule_job('outproducts',trigger='interval',seconds=PRODPERIOD)
	if "lat" in message.topic:
		global LAT
		LAT = float(message.payload.decode())
	if "lon" in message.topic:
		global LON
		LON = float(message.payload.decode())
	if "neighbors" in message.topic:
		global NEIGHBORS
		NEIGHBORS = message.payload.decode()

def connection_to_broker():
	broker_address = "62.109.10.163"
	port = 1883
	user = "station"
	password = ""

	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(broker_address, port=port)

	client.loop_start()

	while Connected != True:
		time.sleep(0.1)

def on_sub():
	client.subscribe("recparams/" + OBS + '/' + SITE)
	client.subscribe("maxdelay/" + OBS + '/' + SITE)
	client.subscribe("outparams/" + OBS + '/' + SITE)
	client.subscribe("rawperiod/" + OBS + '/' + SITE)
	client.subscribe("outproducts/" + OBS + '/' + SITE)
	client.subscribe("prodperiod/" + OBS + '/' + SITE)
	client.subscribe("lat/" + OBS + '/' + SITE)
	client.subscribe("lon/" + OBS + '/' + SITE)
	client.subscribe("neighbors/" + OBS + '/' + SITE)

def watch_file():
	global stat
	if len(list(glob.iglob(os.path.join(path, '*.jps')))) == 0:
		scheduler.pause()
		client.publish("errorFile/" + OBS + '/' + SITE, "1", retain=True)
		while len(list(glob.iglob(os.path.join(path, '*.jps')))) == 0:
			if time.localtime().tm_min == 0 and time.localtime.tm_sec == 0:
				client.publish("availability" + OBS + '/' + SITE, "0", retain=True)
			print("wait file")
		client.publish("errorFile/" + OBS + '/' + SITE, "0", retain=True)
		scheduler.resume()
	newfile = max(glob.iglob(os.path.join(path, '*.jps')), key=os.path.getctime)
	if not stat or stat.parser.fname != newfile:
		stat = JPSStatistics(newfile, mode='a')

def start_settings():
	#newest = max(glob.iglob(os.path.join(path, '*.jps')), key=os.path.getctime)
	#STARTSIZE = os.stat(newest).st_size

	client.publish("availability/" + OBS + '/' + SITE, "-1", retain=True)
	client.publish("measure/" + OBS + '/' + SITE, "0", retain=True)
	client.publish("lasttime/" + OBS + '/' + SITE, "0", retain=True)
	client.publish("maxdelay/" + OBS + '/' + SITE, MAXDELAY, retain=True)
	update_measure()
	update_lasttime()

#def parser_data():
#	newest = max(glob.iglob(os.path.join(path, '*.jps')), key=os.path.getctime)
#	statinfo = os.stat(newest)
#	tempsize = statinfo.st_size
#	parser.parse(newest, 'r')
#	parser.read(n=-1)
#	params = ",".join(parser.get_codes_in_file())

#	return params, tempsize

#def up_size(temp):
#	if STARTSIZE < temp:
#		return True
#	return False

def update_measure():
	times = stat.parser.get_data('~~')
	if len(times) > 0 and os.path.exists(stat.parser.fname) == True:
		client.publish("measure/" + OBS + '/' + SITE, "1", retain=True)
		global LASTTIME
		LASTTIME = time.time()
	#params, tempsize = parser_data()
	#if up_size(temp=tempsize):
	#	global STARTSIZE
	#	STARTSIZE = tempsize
	#	if len(params.split(",")) > 2:
	#		client.publish("measure/" + OBS + '/' + SITE, "1", retain=True)
	#		global LASTTIME
	#		#LASTTIME = datetime.datetime.utcnow().timestamp()
	#		LASTTIME = time.time()
	#		#client.publish("lasttime/" + OBS + '/' + SITE, str(LASTTIME), retain=True)

	if time.time() - LASTTIME > MAXDELAY:
		client.publish("measure/" + OBS + '/' + SITE, "0", retain=True)
	#return tempsize

def update_lasttime():
	client.publish("lasttime/" + OBS + '/' + SITE, str(LASTTIME), retain=True)

def update_realparams():
	params = ",".join(stat.parser.get_codes_in_file())
	client.publish("realrecparams/" + OBS + '/' + SITE, params, retain=True)

def update_outrawparams():
	print(OUTPARAMS)
	if OUTPARAMS == "":
		client.publish("outrawdata/" + OBS + '/' + SITE, "",  retain=True)
	else:
		#parser_data()
		fields = OUTPARAMS.split(',')
		#ltime = parser.get_data('~~')
		ltime = stat.parser.get_data('~~')
		index = len(ltime) - 1
		out_str = ""
		for field in fields:
			field = field.strip()
			fdata = stat.parser.get_data(field)
			print("field: ", field)
			if fdata == None or len(fdata) <= index:
				out_str += 'None, '
			else:
				out_str += (str(fdata[index]) + ', ')
		#print(out_str[:-2])
		client.publish("outrawdata/" + OBS + '/' + SITE, out_str[:-2], Retain=True)
	#print("Completed outrawdata")

def update_outprodparams():
	if OUTPRODUCTS == "":
		client.publish("outproddata/" + OBS + '/' + SITE, "", retain=True)
	else:
		pass # outproddata

def update_stat():
	print('Updating statistics')
	watch_file()
	stat.update()
	stat.get_data_availability(accumulate=True)

def update_availability():
	#global newfile
	#if os.path.exists(newfile) == False:
	#	scheduler.pause()
	#	newfile = max(glob.iglob(os.path.join(path, '*.jps')), key=os.path.getctime)
	#	global stat
	#	stat = JPSStatistics(newfile, mode='a')
	#	scheduler.resume()
	#stat.update()
	proc = stat.get_data_availability(accumulate=False)
	client.publish("availability/" + OBS + '/' + SITE,str(proc*100),retain=True)

def data_proc():
	scheduler.add_job(func=update_stat,trigger='interval',seconds=600)
	scheduler.add_job(func=update_measure,trigger='interval',seconds=600)
	scheduler.add_job(func=update_realparams,trigger='interval',seconds=3600)
	scheduler.add_job(func=update_outrawparams,trigger='interval',seconds=RAWPERIOD, id='outrawparams')
	scheduler.add_job(func=update_outprodparams,trigger='interval',seconds=PRODPERIOD, id='outproducts')
	scheduler.add_job(func=update_availability,trigger='cron',hour='*')
	scheduler.add_job(func=update_lasttime,trigger='interval',seconds=600)


Connected = False
client = mqtt.Client(OBS + '/' + SITE)

connection_to_broker()
on_sub()
#parser = JPSParser(get_struct(*CODES))
#stat = JPSStatistics(newfile, mode='a')
stat = None
watch_file()
scheduler = BlockingScheduler({'apscheduler.job_defaults.max_instances': '1000', 'apscheduler.job_defaults.coalesce': 'True', 'apscheduler.job_defaults.misfire_grace_time':'600'});

startsize = start_settings()
data_proc()
try:
	scheduler.start()
	print('SCHEDULER', scheduler.state())
except (KeyboardInterrupt, SystemExit):
	client.publish("exit/" + OBS + '/' + SITE, "1", retain=False)
	scheduler.shutdown()
	client.disconnect()
	client.loop_stop()

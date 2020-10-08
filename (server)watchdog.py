import paho.mqtt.client as mqtt
import time
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

MAXDELAY = {}
LASTTIME = {}
RECPARAMS = {}
REALRECPARAMS = {}
TIMEUPDATE = {}

def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print("Connected to broker")
		global Connected
		Connected = True
	else:
		print("Connected failed")

def on_message(client, userdata, message):
	print(message.topic, message.payload.decode())
	data_of_topic = message.topic.split('/')
	station = data_of_topic[1] + '/' + data_of_topic[2]

	if TIMEUPDATE.get(station) == None:
		print("hello", station)
		client.publish("healthy/" + station, "0", retain=True)
		TIMEUPDATE[station] = datetime.datetime.today().timestamp()
		global sch
		sch.add_job(func=update_healthy,trigger='interval',seconds=600,args=[station])
		print("OK")
		global MAXDELAY
		MAXDELAY[station] = 900
		global REALRECPARAMS
		REALRECPARAMS[station] = ""
		global RECPARAMS
		RECPARAMS[station] = ""
		global LASTTIME
		#LASTTIME[station] = datetime.datetime.utcnow().timestamp() # for test
		LASTTIME[station] = time.time()
	if data_of_topic[0] == "maxdelay":
		MAXDELAY[station] = int(message.payload.decode())
	if data_of_topic[0] == "lasttime":
		LASTTIME[station] = float(message.payload.decode())
	if data_of_topic[0] == "recparams":
		RECPARAMS[station] = message.payload.decode()
	if data_of_topic[0] == "realrecparams":
		REALRECPARAMS[station] = message.payload.decode()
	update_healthy(station)

def connection_to_broker():
	global Connected
	broker_address = "62.109.10.163"
	port = 1883
	user = "server"
	password = ""

	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(broker_address, port=port)

	client.loop_start()

	while Connected != True:
		time.sleep(0.1)

def on_sub():
	client.subscribe("lasttime/#")
	client.subscribe("recparams/#")
	client.subscribe("realrecparams/#")
	client.subscribe("maxdelay/#")

def update_healthy(name_station):
	#razn = datetime.datetime.utcnow().timestamp() - LASTTIME[name_station]
	razn = time.time() - LASTTIME[name_station]
	print(LASTTIME[name_station], razn, MAXDELAY[name_station])
	if razn < MAXDELAY[name_station] and check_param(RECPARAMS[name_station],REALRECPARAMS[name_station],name_station) == True:
		client.publish("healthy/" + name_station,'1',retain=True)
		print("update healthy true")
	else:
		print("update healthy false")
		client.publish("healthy/" + name_station,'0',retain=True)

def check_param(rec, real, name):
	checksum = 0
	missing_params = ""
	if rec == "" or rec == 'undefined':
		client.publish("missparams/" + name,"-1",retain=True)
		return False
	recp = rec.split(',')
	for el in recp:
		if el in real:
			checksum += 1
		else:
			missing_params += (el + ',')
	client.publish("missparams/" + name, missing_params[:-1],retain=True)
	if checksum == len(recp):
		return True
	return False

def check_params(rec_params):
	checksum = 0
	missing_params = ""
	print('hello')


Connected = False
client = mqtt.Client("Dogi")

connection_to_broker()
on_sub()
# data_proc()
sch = BlockingScheduler()
#sch.add_job(func=update_healthy, trigger='interval', seconds=600)
try:
	sch.start()
	while (True):
		pass

except (KeyboardInterrupt, SystemExit):
	for key in TIMEUPDATE.keys():
		client.publish("healthy/" + key, "0", retain=True)
	client.disconnect()
	client.loop_stop()


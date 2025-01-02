#!/usr/bin/env python
"""
v0.2
Updated Version tested with OpenDTU and Gen24 Frimware > 1.30

Simulates a Fronius Smart Meter for providing necessary
information to inverters (e.g. Gen24).
Can be used with OpenDTU.
Necessary information is provied via MQTT and translated to MODBUS TCP

Based on
https://www.photovoltaikforum.com/thread/185108-fronius-smart-meter-tcp-protokoll

"""
###############################################################
# Import Libs
###############################################################
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.datastore import ModbusSparseDataBlock
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.transaction import ModbusRtuFramer, ModbusAsciiFramer
import threading
import struct
import time
import json
import getopt
import sys
import socket
import signal
import os

from pymodbus.server import StartTcpServer

from pymodbus.transaction import (
    ModbusAsciiFramer,
    ModbusBinaryFramer,
    ModbusSocketFramer,
    ModbusTlsFramer,
)

###############################################################
# Timer Class
###############################################################
class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = threading.Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


###############################################################
# Configuration
###############################################################
mqttconf = {
            'username':"",
            'password':"",
            'address': "",
            'port': 1883
}
MQTT_TOPIC_CONSUMPTION  = "OpenDTU/ac/power" #OpenDTU Watts
MQTT_TOPIC_TOTAL_IMPORT = "" #Import Wh
MQTT_TOPIC_TOTAL_EXPORT = "OpenDTU/ac/yieldtotal" #Export WH
MQTT_TOPIC_L1_CONSUMPTION = "" # L1 Watts
MQTT_TOPIC_L2_CONSUMPTION= "" # L2 Watts
MQTT_TOPIC_L3_CONSUMPTION = "" # L3 Watts, empty -> L1,L2,L3 i                                                                                                             s calculated
#MQTT_TOPIC_TIME = "FSM/Time" #Timestamp for Check MK

corrfactor = 1 # or 1000 
i_corrfactor = int(corrfactor)

modbus_port = 502

###############################################################
# MQTT service
###############################################################

import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe

lock = threading.Lock()

leistung = "0"
einspeisung = "0"
netzbezug = "0"
rtime = 0
l1 = "0"
l2 = "0"
l3 = "0"

ti_int1 = "0"
ti_int2 = "0"
exp_int1 = "0"
exp_int2 = "0"
ep_int1 = "0"
ep_int2 = "0"
l1_int1 = "0"
l1_int2 = "0"
l2_int1 = "0"
l2_int2 = "0"
l3_int1 = "0"
l3_int2 = "0"

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def init_mqtt():
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,"SmartMeter",clean_session=False)
    #mqttc.username_pw_set(mqttconf['username'], mqttconf['password'])
    mqttc.connect(mqttconf['address'], mqttconf['port'], 60)

    mqttc.subscribe(MQTT_TOPIC_CONSUMPTION)
    if MQTT_TOPIC_TOTAL_IMPORT != "":
        mqttc.subscribe(MQTT_TOPIC_TOTAL_IMPORT)
    if MQTT_TOPIC_TOTAL_EXPORT != "":
        mqttc.subscribe(MQTT_TOPIC_TOTAL_EXPORT)
    if MQTT_TOPIC_L1_CONSUMPTION != "":
        mqttc.subscribe(MQTT_TOPIC_L1_CONSUMPTION)
    if MQTT_TOPIC_L2_CONSUMPTION != "":
        mqttc.subscribe(MQTT_TOPIC_L2_CONSUMPTION)
    if MQTT_TOPIC_L3_CONSUMPTION != "":
        mqttc.subscribe(MQTT_TOPIC_L3_CONSUMPTION)

    #mqttc.subscribe(MQTT_TOPIC_TIME)
    flag_connected = 0

    mqttc.on_disconnect = on_disconnect
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.loop_start()

def on_connect(client, userdata, flags, rc):
   global flag_connected
   flag_connected = 1
   print("MQTT connection.")


def on_disconnect(client, userdata, rc):
   global flag_connected
   flag_connected = 0
   print("Unexpected MQTT disconnection.")

def on_message(client, userdata, message):
    global leistung
    global einspeisung
    global netzbezug
    global l1
    global l2
    global l3
    global rtime

#    print("Received message '" + str(message.payload) + "' on topic '"
#        + message.topic + "' with QoS " + str(message.qos))

    if not isfloat(message.payload):
        return

    lock.acquire()

    if message.topic == MQTT_TOPIC_CONSUMPTION:
        leistung = message.payload
    elif message.topic == MQTT_TOPIC_L1_CONSUMPTION:
        l1 = message.payload
    elif message.topic == MQTT_TOPIC_L2_CONSUMPTION:
        l2 = message.payload
    elif message.topic == MQTT_TOPIC_L3_CONSUMPTION:
        l3 = message.payload
    elif message.topic == MQTT_TOPIC_TOTAL_IMPORT:
        netzbezug = message.payload
    elif message.topic == MQTT_TOPIC_TOTAL_EXPORT:
        einspeisung = message.payload
    elif message.topic == MQTT_TOPIC_TIME:
        rtime = message.payload

    lock.release()


###############################################################
# Update Modbus Registers
###############################################################
def calculate_register(value_float):
    if value_float == 0:
        int1 = 0
        int2 = 0
    else:
        value_hex = hex(struct.unpack('<I', struct.pack('<f', value_float))[0])
        value_hex_part1 = str(value_hex)[2:6] #extract first register part (hex)
        value_hex_part2 = str(value_hex)[6:10] #extract seconds register part (hex)
        int1 = int(value_hex_part1, 16) #convert hex to integer because pymodbus converts back to hex itself
        int2 = int(value_hex_part2, 16) #convert hex to integer because pymodbus converts back to hex itself
    return (int1,int2)

def updating_writer(a_context):
    global leistung
    global einspeisung
    global netzbezug
    global l1
    global l2
    global l3
    global rtime

    global ep_int1
    global ep_int2
    global exp_int1
    global exp_int2
    global ti_int1
    global ti_int2
    global l1_int1
    global l1_int2
    global l2_int1
    global l2_int2
    global l3_int1
    global l3_int2

    global flag_connected

    lock.acquire()
    #Considering correction factor

    float_netzbezug = float(netzbezug)
    netzbezug_corr = float_netzbezug*i_corrfactor
    #print (netzbezug_corr)

    float_einspeisung = float(einspeisung)
    einspeisung_corr = float_einspeisung*i_corrfactor
    #print (einspeisung_corr)

    #Converting values of MQTT payload to Modbus register

    ep_int1, ep_int2 = calculate_register(float(leistung))
    ti_int1, ti_int2 = calculate_register(netzbezug_corr)
    exp_int1, exp_int2 = calculate_register(einspeisung_corr)

    if MQTT_TOPIC_L1_CONSUMPTION == "" or MQTT_TOPIC_L2_CONSUMPTION == "" or MQTT_TOPIC_L3_CONSUMPTION == "":
        l1_int1, l1_int2 = l2_int1, l2_int2 = l3_int1, l3_int2 = calculate_register(float(leistung)/3)
    else:
        l1_int1, l1_int2 = calculate_register(float(l1))
        l2_int1, l2_int2 = calculate_register(float(l2))
        l3_int1, l3_int2 = calculate_register(float(l3))



    #updating the context
    context = a_context[0]
    register = 3
    slave_id = 0x01
    address = 0x9C87
    values = [0, 0,               #Ampere - AC Total Current Value [A]
              0, 0,               #Ampere - AC Current Value L1 [A]
              0, 0,               #Ampere - AC Current Value L2 [A]
              0, 0,               #Ampere - AC Current Value L3 [A]
              0, 0,               #Voltage - Average Phase to Neutral [V]
              0, 0,               #Voltage - Phase L1 to Neutral [V]
              0, 0,               #Voltage - Phase L2 to Neutral [V]
              0, 0,               #Voltage - Phase L3 to Neutral [V]
              0, 0,               #Voltage - Average Phase to Phase [V]
              0, 0,               #Voltage - Phase L1 to L2 [V]
              0, 0,               #Voltage - Phase L2 to L3 [V]
              0, 0,               #Voltage - Phase L1 to L3 [V]
              0, 0,               #AC Frequency [Hz]
              ep_int1, 0,         #AC Power value (Total) [W] ==> Second hex word not needed
              l1_int1, 0,         #AC Power Value L1 [W]
              l2_int1, 0,         #AC Power Value L2 [W]
              l3_int1, 0,         #AC Power Value L3 [W]
              0, 0,               #AC Apparent Power [VA]
              0, 0,               #AC Apparent Power L1 [VA]
              0, 0,               #AC Apparent Power L2 [VA]
              0, 0,               #AC Apparent Power L3 [VA]
              0, 0,               #AC Reactive Power [VAr]
              0, 0,               #AC Reactive Power L1 [VAr]
              0, 0,               #AC Reactive Power L2 [VAr]
              0, 0,               #AC Reactive Power L3 [VAr]
              0, 0,               #AC power factor total [cosphi]
              0, 0,               #AC power factor L1 [cosphi]
              0, 0,               #AC power factor L2 [cosphi]
              0, 0,               #AC power factor L3 [cosphi]
              exp_int1, exp_int2, #Total Watt Hours Exportet [Wh]
              0, 0,               #Watt Hours Exported L1 [Wh]
              0, 0,               #Watt Hours Exported L2 [Wh]
              0, 0,               #Watt Hours Exported L3 [Wh]
              ti_int1, ti_int2,   #Total Watt Hours Imported [Wh]
              0, 0,               #Watt Hours Imported L1 [Wh]
              0, 0,               #Watt Hours Imported L2 [Wh]
              0, 0,               #Watt Hours Imported L3 [Wh]
              0, 0,               #Total VA hours Exported [VA]
              0, 0,               #VA hours Exported L1 [VA]
              0, 0,               #VA hours Exported L2 [VA]
              0, 0,               #VA hours Exported L3 [VA]
              0, 0,               #Total VAr hours imported [VAr]
              0, 0,               #VA hours imported L1 [VAr]
              0, 0,               #VA hours imported L2 [VAr]
              0, 0                #VA hours imported L3 [VAr]
    ]
    #print(values)
    context.setValues(register, address, values)
    time.sleep(1)
    lock.release()


###############################################################
# Config and start Modbus TCP Server
###############################################################
def run_updating_server():
    global modbus_port
    lock.acquire()
    datablock = ModbusSparseDataBlock({

        40001:  [21365, 28243],
        40003:  [1],
        40004:  [65],
        40005:  [70,114,111,110,105,117,115,0,0,0,0,0,0,0,0,0,         #Manufacturer "Fronius
                83,109,97,114,116,32,77,101,116,101,114,32,54,51,65,0, #Device Model "Smart Meter
                0,0,0,0,0,0,0,0,                                       #Options N/A
                0,0,0,0,0,0,0,0,                                       #Software Version  N/A
                48,48,48,48,48,48,48,50,0,0,0,0,0,0,0,0,               #Serial Number: 00000 (should be different if there are more Smart Meters)
                240],                                                  #Modbus TCP Address:
        40070: [213],
        40071: [124],
        40072: [0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0],

        40196: [65535, 0],
    })

    slaveStore = ModbusSlaveContext(
            di=datablock,
            co=datablock,
            hr=datablock,
            ir=datablock,
        )

    a_context = ModbusServerContext(slaves=slaveStore, single=True)

    lock.release()

    ###############################################################
    # Run Update Register every 2 Seconds
    ###############################################################
    time = 2  # 2 seconds delay
    rt = RepeatedTimer(time, updating_writer, a_context)

    print("### start server, listening on " + str(modbus_port))
    address = ("", modbus_port)
    StartTcpServer(
            context=a_context,
            address=address,
            framer=ModbusSocketFramer
            # TBD handler=None,  # handler for each session
            # allow_reuse_address=True,  # allow the reuse of an address
            # ignore_missing_slaves=True,  # ignore request to a missing slave
            # broadcast_enable=False,  # treat unit_id 0 as broadcast address,
            # TBD timeout=1,  # waiting time for request to complete
            # TBD strict=True,  # use strict timing, t1.5 for Modbus RTU
            # defer_start=False,  # Only define server do not activate
        )


values_ready = False
init_mqtt()
while not values_ready:
      print("Warten auf Daten von MQTT Broker")
      time.sleep(1)
      lock.acquire()
      if netzbezug  != '0' or einspeisung != '0' :
         print("Daten vorhanden. Starte Modbus Server")
         values_ready = True
      lock.release()
run_updating_server()

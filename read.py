import json
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import time
import struct
import datetime 
import paho.mqtt.client as mqtt
import threading
import pickle

timeset = 0
data_package = {}
DATA = {}
with open('backup.pickle', 'rb') as f:
    backup = pickle.load(f)
if backup != 0:
    total = backup
    print(total)
else:
    total = 0
timestart = time.time()
irra = 0
nhietdo = 0
status = True

def send_orther(a):
    time.sleep(0.1)
    client = mqtt.Client()
    client.username_pw_set('iot2022', 'iot2022')
    try:
        client.connect("core.ziot.vn", 5000)
        data = json.dumps(DATA)
        mqtt_topic = str(a) + '/reportData'
        client.publish(mqtt_topic, data)
        return
    except:
        print("MQTT connect failed!!!   Try to connect with MQTT server...")

        
def reset_total():
    timereset = (time.time() // 86400 + 1) + 86400
    global total
    while True:
        if time.time() >= timereset:
            total = 0
            with open('backup.pickle', 'wb') as f:
                pickle.dump(total, f)
            timereset = timereset + 86400
        time.sleep(1)

def restart():
    time.sleep(0.1)
    threading.Thread(target=read_data).start()  


def mqtt_reconnect():
    time.sleep(0.1)
    threading.Thread(target=send_data).start()    


def read_data():
    
    global total,timestart, irra, nhietdo , status 
    client1 = ModbusClient('192.168.100.2', port= 502, timeout=1)

    if client1.connect():
            print("connected device")
            while True:
                if int(time.time()) >= int(timeset):

                    read_orther()
                    break                
                try:  
                    read = client1.read_input_registers(address=2, count=5, unit=1)
                    nhietdo =  round(int(read.registers[4])/100,1)
                    a = str(format(read.registers[0], '016b')) + str(format(read.registers[1], '016b'))
                    if a[0] == '1':
                        irra = 0
                    else:
                        irra = int(a,2)/100
                    
                    time.sleep(0.2)
                    total = total + (irra*(time.time()-timestart))/3600
                    timestart = time.time()
                    status = 1
                    with open('backup.pickle', 'wb') as f:
                        pickle.dump(total, f)
                    timestart = time.time()
                    # print(total)
                except:
                    print("Device lost connection")
                    status = 0
                    restart()     
                    break              
    
    else:
        print("Connect failed")
        status = 0
        time.sleep(5)
        print("Connect to the device failed!!!   Try to connect to the device...")
        restart()

def send_data():
    time.sleep(0.1)
    mqtt_broker = "core.ziot.vn"
    mqtt_port = 5000
    mqtt_topic = "DEVportData"
    client = mqtt.Client()
    client.username_pw_set('iot2022', 'iot2022')
    try:
        client.connect(mqtt_broker, mqtt_port)
        print("MQTT connected server.")
        client.disconnect()
        sts = 1
    except:
        sts = 0
    if sts == 1:

        while True:

            try:   
                if int(time.time()) >= int(timeset):
                    break
                delaysec = 10
                a = delaysec * (time.time() // delaysec) + delaysec            
                time.sleep(a - time.time() )
                timeStamp = datetime.datetime.fromtimestamp(a)


                if  status == 1:
                    irr = irra
                    client.connect(mqtt_broker, mqtt_port)
                    DATA = {"type": "smp3", "data": [{"totalIrradce": round(irr)}, {"dailyIrradtn": round(total/1000,3)}, {"ambientTemp": nhietdo}], "timeStamp": str(timeStamp)}
                    data = json.dumps(DATA)
                    client.publish(mqtt_topic, data)
                    time.sleep(0.1)
                    print(data,'\n')
                    client.disconnect()
                    
                elif status == 0 :
                    print("Device lost connection \nMQTT temporarily does not send data")
                    mqtt_reconnect()    
                    break

            except:
                print("MQTT lost connect")
                time.sleep(5)
                print("Try to connect with MQTT server...")
                mqtt_reconnect()
                break
    
    else:
        time.sleep(5)
        print("MQTT connect failed!!!   Try to connect with MQTT server...")
        mqtt_reconnect() 

        

def read_orther():
    global timeset, data_package
    with open("DeviceConfig.conf", "r") as config_file:
        config = json.load(config_file)    
    # while True:
        for device_id, device_info in config.get("modbustcp", {}).items():

            client = ModbusTcpClient(device_info['ip'], port=device_info['port'], timeout=1)
            try:
            
                client.connect()
                
                for register in device_info['tasks'].get("read_registers", []):
                    
                    if device_info['deviceType'] == 'sensor':
                        result = client.read_input_registers(address=register['offSet'], count=5, unit = device_info['unitID'])
                    else:
                        result = client.read_holding_registers(register['offSet'], count=register['size'], unit=device_info['unitID'])
                    
                    data = result.registers
                    
                    if register['dataType'] == 'float32':
                        combined_data = (data[0] << 16) | data[1]
                        value = struct.unpack('f', struct.pack('I', combined_data))[0]
                        value = round(struct.unpack('f', struct.pack('I', combined_data))[0]*(10 ** register['PF']),register['fractionDigit'])

                    elif register['dataType'] == 'int16':
                        value = round((struct.unpack('>h', struct.pack('>H', data[0]))[0])*(10 ** register['PF']),register['fractionDigit'])

                    elif register['dataType'] == 'int32':

                        combined_data = (data[0] << 16) | data[1]
                        value = struct.unpack('>i', struct.pack('>I', combined_data))[0]
                        value = round(value * (10 ** register['PF']), register['fractionDigit'])
                       
                    data_package[register['tagName']] = value
                    # time.sleep(0.1)
                client.close()
                print('Connected',device_info['ip'],device_info['port'],device_info['unitID'] )
                DATA = {"type": device_info['deviceType'], "data": [data_package], "timeStamp": str(datetime.datetime.fromtimestamp((time.time())))}
                print(DATA)
                data_package = {}

                threading.Thread(target=send_orther, args= (device_id,)).start()

            except Exception as e:
                print('Connect to ',device_info['ip'],device_info['port'],device_info['unitID'], 'failed!!!')
                data_package = {}  
            # time.sleep(0.1)  
    timeset = device_info['scanningCycleInSecond'] * (time.time() // device_info['scanningCycleInSecond'] + 1) 
    run_main()


def run_main():  
    threading.Thread(target=reset_total).start()    
    threading.Thread(target=send_data).start()
    threading.Thread(target=read_data).start()

read_orther()
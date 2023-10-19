import json
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import time
import struct
import datetime 
import paho.mqtt.client as mqtt
import threading
import pickle


data_package = {}
DATA = {}
timestart = time.time()
irra = 0
nhietdo = 0
status = None
delaysec = 60
data_queue = []
timeset = None
try:
    with open('backup.pickle', 'rb') as f:
        backup = pickle.load(f)
    if backup != 0:
        total = backup
        print(total)
    else:
        total = 0
except:
    total = 0
    with open('backup.pickle', 'wb') as f:
        pickle.dump(total, f)    
    print(total)

with open("deviceConfig.conf", "r") as config_file:
    config = json.load(config_file)

def queue_data():
    global data_queue
    client = mqtt.Client()
    client.username_pw_set('iot2022', 'iot2022')
    while True:
        if data_queue:
            try:
                data_item = data_queue.pop(0)
                client.connect("core.ziot.vn", 5000) 
                for topic, data in data_item.items():
                    client.publish(topic, json.dumps(data))
                    time.sleep(0.2)
                    print(data)
                print('Data is sent in queue :)))))))')

            except:
                data_queue.insert(0, data_item)
                time.sleep(10)
                
        else:
            time.sleep(10)

def send_orther(a,b):
    global data_queue
    client = mqtt.Client()
    client.username_pw_set('iot2022', 'iot2022')
    mqtt_topic = str(a) + '/reportData'
    data = json.dumps(b)
 
    try:
        client.connect("core.ziot.vn", 5000)
        client.publish(mqtt_topic, data)
    except:
        print("MQTT lost connection ... Data will be sent later")
        data_lost = {mqtt_topic : b}
        data_queue.append(data_lost)        
        print(data_lost)

def reset_total():
    timereset = ((time.time()) // 86400 + 1) * 86400 - 7 * 3600
    global total
    while True:
        if time.time() >= timereset:
            total = 0
            with open('backup.pickle', 'wb') as f:
                pickle.dump(total, f)
            timereset = timereset + 86400
        time.sleep(3600)

def restart(time4):
    time.sleep(0.1)
    threading.Thread(target=read_data, args= (time4,)).start()  

def read_data(time3):
    global status ,total,timestart, irra, nhietdo
    for device_id, device_info in config.get("modbustcp", {}).items():
        if device_info['deviceType'] == "sensor":
            ip = device_info['ip']
            unitID =  device_info['unitID']
            mqtt_topic = device_id
    client1 = ModbusClient(ip, port= 502, timeout=1)

    if client1.connect():
        print('Connected',ip,'502',unitID )
        while time.time() < time3:           
            try:  
                read1 = client1.read_input_registers(address=2, count=5, unit=unitID)
                nhietdo =  round(int(read1.registers[4])/100,1)
                a = str(format(read1.registers[0], '016b')) + str(format(read1.registers[1], '016b'))
                irra = int(a,2)/100
                if irra < 0 or irra > 1500:
                    irra = 0
                # time.sleep(0.2)
                total += (irra*(time.time()-timestart))/3600
                timestart = time.time()
                status = True
                with open('backup.pickle', 'wb') as f:
                    pickle.dump(total, f)
                
            except:
                print("The device is lost connected to conversion")
                status = False
                restart(time3)   
                return
        data_call = {"type": "smp3", "data": [{"totalIrradce": round(irra)}, {"dailyIrradtn": round(total/1000,3)}, {"ambientTemp": nhietdo}], "timeStamp": str(datetime.datetime.fromtimestamp((time.time())))}
        threading.Thread(target=send_orther, args= (mqtt_topic,data_call,)).start()
        print(data_call)
        status = False      
        read_orther() 
        return  
    else:
        time.sleep(1)
        print("Connect to the device failed!!!   Try to connect to the device...")
        if time.time() >= time3:
            read_orther() 
            return
        restart(time3)
        return      

def read_orther():
    global data_package, timeset
    with open("deviceConfig.conf", "r") as config_file:
        config = json.load(config_file)    
    # while True:
        for device_id, device_info in config.get("modbustcp", {}).items():

            client = ModbusTcpClient(device_info['ip'], port=device_info['port'], timeout=1)
            if device_info['deviceType'] == 'sensor':
                continue
            try:
            
                client.connect()
                data_fomat = []

                for register in device_info['tasks'].get("read_registers", []):

                    if device_info['deviceType'] == 'sensor':
                        result = client.read_input_registers(address=register['offSet'], count=5, unit = device_info['unitID'])
                    elif device_info['deviceType'] == 'meter':
                        result = client.read_input_registers(register['offSet'], count=register['size'], unit=device_info['unitID'])                    
                    else:
                        result = client.read_holding_registers(register['offSet'], count=register['size'], unit=device_info['unitID'])

                    data = result.registers

                    if register['dataType'] == 'float32':

                        combined_data = (data[1] << 16) | data[0]
                        value = struct.unpack('f', struct.pack('I', combined_data))[0]
                        value = round(struct.unpack('f', struct.pack('I', combined_data))[0]*(10 ** register['PF']),register['fractionDigit'])

                    elif register['dataType'] == 'int16':

                        value = round((struct.unpack('>h', struct.pack('>H', data[0]))[0])*(10 ** register['PF']),register['fractionDigit'])

                    elif register['dataType'] == 'int32':
                        combined_data = (data[0] << 16) | data[1]
                        value = struct.unpack('>i', struct.pack('>I', combined_data))[0]
                        value = round(value * (10 ** register['PF']), register['fractionDigit'])

                    data_package = {register["tagName"] : value}
                    data_fomat.append(data_package)

                    # time.sleep(0.1)
                client.close()
                print('Connected',device_info['ip'],device_info['port'],device_info['unitID'])
                DATA_1 = {"type": device_info['deviceType'],"data" : data_fomat , "timeStamp": str(datetime.datetime.fromtimestamp((time.time())))}
                print(DATA_1)
                threading.Thread(target=send_orther, args= (device_id,DATA_1,)).start()
                data_package = {}

            except Exception as e:
                print('Connect to ',device_info['ip'],device_info['port'],device_info['unitID'], 'failed!!!')
                data_package = {}  
            time.sleep(0.1)  
    timeset = device_info['scanningCycleInSecond'] * (time.time() // device_info['scanningCycleInSecond'] + 1) 
    # timeset = (time.time() // 30 + 1) * 30

    # if (time.time() <= (((time.time()) // 86400 ) * 86400 + 11 * 3600)) and (time.time() >= (((time.time()) // 86400) * 86400 - 2 * 3600)):
        # run_main(timeset)
    # else:
        # time.sleep(41400)
    threading.Thread(target=read_data, args= (timeset,)).start()
    return

if __name__ == "__main__":
    threading.Thread(target=queue_data).start()
    threading.Thread(target=reset_total).start()  
    read_orther()

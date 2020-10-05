import os
import flask
import base64
import random
import string
import subprocess
import signal
import json
import thread
import copy
from subprocess import PIPE
from flask import render_template
import traceback 
from flask import jsonify
import time
import atexit

from authlib.client import OAuth2Session
import google.oauth2.credentials
import googleapiclient.discovery
import functools
import google_auth


app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.secret_key = "hudabeybi"
app.register_blueprint(google_auth.app)

host = '0.0.0.0'
port = 5555

processes = []

def save_processes_to_file():
    ss = json.dumps(processes, default=obj_dict)
    f = open("sessions.log", "w")
    f.write(ss)
    f.close()

def load_process_from_file():
    global processes
    processes = []
    if os.path.isfile("sessions.log"):
        f = open("sessions.log", "r")
        ss = f.read()
        processes2 = json.loads(ss)
        for item in processes2:
            o = convert_to_process(item)
            processes.append(o)
        
        reload_process()

def convert_to_process(item):
    o = lambda: None
    o.id = item["id"].replace("\n",  "")
    o.pid = item["pid"]
    o.folder = item["folder"]
    o.file = item["file"]
    o.clientId = item["clientId"]
    o.process = None
    o.status = 0

    key = "table"
    if key in item:
        o.table = item["table"]
    else:
        ff = o.folder.split("/")
        o.table = ff[1]

    o.configuration = item["configuration"]  
    return o 

def reload_process():
    global processes
    for item in processes:
        try:
            print("Creating process for " + item.file)
            proc = subprocess.Popen(["python", "kafka2bq.py", "-f" + item.file])
            item.process = proc
            item.pid = proc.pid
            print("Done, PID : " + str(item.pid))
            time.sleep(2)
        except :
            print("Fail to create process")
            traceback.print_exc() 
    
    save_processes_to_file()

def stop_all_process():
    global processes
    for item in processes:
        try:
            print("Kill process for " + item.file)
            os.kill(item.pid, signal.SIGTERM) #or signal.SIGKIL
        except :
            print("Already killed")

    
    save_processes_to_file()


def check_processes(name, num):
    for item in processes:
        print("check_processes")
        proc = item.process
        print(proc)
        try:
            print("Readline")
            text = proc.stdout.read()
            print(item.clientId + " : " + text)
        except:
            print("Error  check_processes")
            traceback.print_exc() 


def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def get_random_number(length):
    letters = '1234567890'
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def check_exists(arr, file):
    print("check_exists")
    for item in arr:
        if item.file == file:
            return True

    return False

def get_pid(arr, file):

    for item in arr:
        if item.file == file:
            return item.pid

    return None

def get_process(arr, file):

    for item in arr:
        if item.file == file:
            return item

    return None

def get_processes_by_folder(arr, folder):
    items  = []
    for item in arr:
        if item.folder == folder:
            items.append(item)

    return items

def remove(arr, file):
    idx = -1
    counter = 0
    for item in arr:
        if item.file == file:
            idx = counter
            break
        counter = counter + 1
    if  idx  >  -1:
        arr.pop(idx)
    return arr

def parse_folder(folder):
    ss = folder.split("_")
    host = ss[0]
    sss  = ss[1].split("/")
    port =  sss[0]

    ss = folder.split("/")
    ss  = ss[1]
    ssss  =  ss.split("_")
    group  =  ssss[0]

    c = 0
    topic = ""
    for sitem in ssss:
        if c > 0:
            topic = topic + ssss[c]  + "_"
        c = c + 1
    
    if len(topic) > 0:
        topic = topic[:-1]

    return [host, port, group, topic]

def check_user(email):
    f  = open("user.logins")
    lines = f.read()
    lines = lines.split("\n")
    for line  in lines:
        if(line.strip() == email ):
            return True
    return False


def obj_dict(obj):
    return obj.__dict__

@app.route('/', methods=['GET'])
def hello():
    return "Hello World"

@app.route('/hello/<name>', methods=['GET'])
def helloname(name):
    return "Hello, " + name

@app.route('/web')
def static_file():
    if google_auth.is_logged_in():
        user_info = google_auth.get_user_info()
        res =  check_user(user_info["email"])
        if(res):
            return render_template('index.html')
        else:
            return render_template('login.html')
    else:
        return render_template('login.html')

@app.route('/web/new')
def new():
    return render_template('new.html')

@app.route('/web/view/<id>')
def view(id):
    return render_template('view.html', id=id)


@app.route('/kafka2bq/start/<server>/<port>/<group>/<topic>/<project>/<table>/<num>/<credential>', methods=['GET'])
def start(server, port, group, topic, project, table, num, credential):
    info = []
    os.system("mkdir " + server + "_" + port + "" )
    folder = server + "_" + port + "/" + group + "_" + topic
    id  = base64.encodestring(folder)
    os.system("mkdir " + folder )
    credential = base64.decodestring(credential)
    os.path.dirname(os.path.realpath(__file__))
    s = ("#Credential for accessing BigQuery"
        "\n-credential " + credential + ""
        "\n\n#Kafka topic "
        "\n-kafka-topic " + topic + ""
        "\n\n#BigQuery project "
        "\n-project " + project + ""
        "\n\n#BigQuery  table output "
        "\n-output-bq-table " + table + ""
        "\n\n#Maximum rows to insert "
        "\n-rows-insert-num 1000"
        "\n\n# Kafka consumer settings "
        "\n-bootstrap-server " + server + ":" + port + ""
        "\n-group-id " + group + ""
        "\n-client-id <clientId>\n")


    for i in range(int(num)):
        clientId = 'Client-' + get_random_number(5)
        ss  = s.replace('<clientId>', clientId)
        file = folder + "/" + clientId + '.conf'
        f = open(file, "wt")
        f.write(ss)
        f.close()

        if check_exists(processes, file)  == True:
            print("\n\nConsumer " + file + " Exists!\nRestarting...\n")
            pid = get_pid(processes, file)
            if pid is not None:
                try:
                    print("\n\nKill " + str(pid))
                    os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
                except:
                    print("Already killed")

            remove(processes, file)

        try:
            print("Starting " + file)
            proc = subprocess.Popen(["python", "kafka2bq.py", "-f" + file])
            print("proc")
            print(proc)
            o = lambda: None
            o.id = id.replace("\n",  "")
            o.pid = proc.pid
            o.folder = folder
            o.file = file
            o.clientId = clientId
            o.process = proc
            o.status = 1
            o.table = table

            o.configuration = s

            processes.append(o)
            print("File "  + file + ", PID : " + str(o.pid))
            #info = info + o.clientId + ", PID: " + str(o.pid) +"\n"

            info.append(o)
        except:
            print("Fail to create process")
            traceback.print_exc()

    """try:
       thread.start_new_thread( check_processes, ("Thread-1", 2, ))
    except Exception, e:
       print "Error: unable to start thread"
       print(e)"""

    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/add/<server>/<port>/<group>/<topic>/<num>', methods=['GET'])
def addclients(server, port, group, topic, num):
    info = []
    error =  lambda: None

    os.path.dirname(os.path.realpath(__file__))
    folder = server + "_" + port + "/" + group + "_" + topic
    id  = base64.encodestring(folder)
    items = get_processes_by_folder(processes, folder)

    if len(items) == 0:
        error.message = "No existing group  and topic "
        sjson = json.dumps(error, default=obj_dict)
        return sjson

    start = len(items)
    end  = start +  int(num)

    itemtemplate = items[0]
    s = itemtemplate.configuration

    for i in range(start, end):

        try:
            clientId = 'Client-' + + get_random_number(5)
            ss  = s.replace('<clientId>', clientId)
            file = folder + "/" + clientId + '.conf'
            f = open(file, "wt")
            f.write(ss)
            f.close()

            print("Starting " + file)
            proc = subprocess.Popen(["python", "kafka2bq.py", "-f" + file])

            o = lambda: None
            o.id = id.replace("\n",  "")
            o.pid = proc.pid
            o.folder = folder
            o.file = file
            o.clientId = clientId
            o.process = proc
            o.configuration = s
            o.status = 1
            o.table = itemtemplate.table
            processes.append(o)
            print("File "  + file + ", PID : " + str(o.pid))
            #info = info + o.clientId + ", PID: " + str(o.pid) +"\n"
            info.append(o)
        except:
            print("Fail creating consumer")
            traceback.print_exc()


    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/add/<id>/<num>', methods=['GET'])
def addclientsbyid(id, num):
    info = []
    error =  lambda: None

    os.path.dirname(os.path.realpath(__file__))
    folder = base64.decodestring(id)
    items = get_processes_by_folder(processes, folder)

    if len(items) == 0:
        error.message = "No existing group  and topic "
        sjson = json.dumps(error, default=obj_dict)
        return sjson

    start = len(items)
    end  = start +  int(num)

    itemtemplate = items[0]
    s = itemtemplate.configuration

    for i in range(start, end):

        try:
            clientId = 'Client-' + get_random_number(5)
            ss  = s.replace('<clientId>', clientId)
            file = folder + "/" + clientId + '.conf'
            f = open(file, "wt")
            f.write(ss)
            f.close()

            print("Starting " + file)
            proc = subprocess.Popen(["python", "kafka2bq.py", "-f" + file])

            o = lambda: None
            o.id = id.replace("\n",  "")
            o.pid = proc.pid
            o.folder = folder
            o.file = file
            o.clientId = clientId
            o.process = proc
            o.configuration = s
            o.status = 1
            o.table = itemtemplate.table
            processes.append(o)
            print("File "  + file + ", PID : " + str(o.pid))
            #info = info + o.clientId + ", PID: " + str(o.pid) +"\n"
            info.append(o)
        except:
            print("Fail creating consumer")
            traceback.print_exc()


    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/status/<server>/<port>/<group>/<topic>', methods=['GET'])
def status(server, port, group, topic):
    info = []
    folder = server + "_" + port + "/" + group + "_" + topic
    for item in processes:
        if item.file.startswith(folder):
            p = item.process
            if p.poll() == None:
                #info = info + item.clientId + ", PID: " + str(item.pid) + " is running\n"
                item.status = 1
            else:
                item.status = 0
            print(item.clientId)
            info.append(item)

    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/status/<id>', methods=['GET'])
def statusbyid(id):
    folder = base64.decodestring(id)


    info = []
    for item in processes:
        if item.file.startswith(folder):
            p = item.process
            if p.poll() == None:
                #info = info + item.clientId + ", PID: " + str(item.pid) + " is running\n"
                item.status = 1
            else:
                item.status = 0
            print(item.clientId)
            info.append(item)

    inf = lambda: None
    if(len(info) > 0):
        inf.id = info[0].id.replace("\n",  "")
        inf.table = info[0].table
        folder =  parse_folder(info[0].folder)
        inf.server = folder[0]
        inf.port = folder[1]
        inf.group = folder[2]
        inf.topic = folder[3]
        inf.workers = info

    sjson = json.dumps(inf, default=obj_dict)
    return sjson

@app.route('/kafka2bq/status/<server>/<port>/<group>/<topic>/<clientid>', methods=['GET'])
def clientstatus(server, port, group, topic, clientid):
    info = []
    folder = server + "_" + port + "/" + group + "_" + topic
    for item in processes:
        print (item.clientId + " === " + clientid)
        if item.clientId == clientid:
            p = item.process
            if p.poll() == None:
                #info = info + item.clientId + ", PID: " + str(item.pid) + " is running\n"
                item.status = 1
            else:
                item.status = 0
            info = item
            break

    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/sessions', methods=['GET'])
def sessions():
    info = []
    ids =  []
    for item in processes:
        if(item.id.replace("\n",  "") not in ids):
            ids.append(item.id.replace("\n",  ""))
            o = lambda: None
            o.id = item.id.replace("\n",  "")
            o.folder =  item.folder
            oo  =  parse_folder(item.folder)
            o.server = oo[0]
            o.port  =  oo[1]
            o.group = oo[2]
            o.topic = oo[3]
            o.table = item.table
            info.append(o)

    sjson = json.dumps(info, default=obj_dict)
    return sjson


@app.route('/kafka2bq/restart/<server>/<port>/<group>/<topic>/<clientid>', methods=['GET'])
def restartclient(server, port, group, topic, clientid):
    info = []
    folder = server + "_" + port + "/" + group + "_" + topic
    for item in processes:
        print (item.clientId + " === " + clientid)
        if item.clientId == clientid:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")
            remove(processes, item.file)

            try:
                print("\nRestarting " + item.file)
                print("\n\n")
                proc = subprocess.Popen(["python", "kafka2bq.py", "-f" + item.file])
                o = lambda: None
                o.id = item.id.replace("\n",  "")
                o.pid = proc.pid
                o.folder = item.folder
                o.file = item.file
                o.clientId = item.clientId
                o.process = proc
                o.configuration = item.configuration
                o.table = item.table
                o.status = 1
                print("New instance PID: " + str(o.pid))
                processes.append(o)
                info = o
            except :
                print("Fail to restart consumer")
                traceback.print_exc()

            break

    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/restart/<id>/<clientid>', methods=['GET'])
def restartclientbyid(id, clientid):
    info = []
    folder = base64.decodestring(id);
    for item in processes:
        print (item.clientId + " === " + clientid)
        if item.clientId == clientid:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")

            remove(processes, item.file)

            try:
                print("\nRestarting " + item.file)
                print("\n\n")
                proc = subprocess.Popen(["python", "kafka2bq.py", "-f" + item.file])
                o = lambda: None
                o.id = item.id.replace("\n",  "")
                o.pid = proc.pid
                o.folder = item.folder
                o.file = item.file
                o.clientId = item.clientId
                o.process = proc
                o.configuration = item.configuration
                o.table = item.table
                o.status = 1
                print("New instance PID: " + str(o.pid))
                processes.append(o)
                info = o
            except:
                print("Fail to create  consumer")
                traceback.print_exc()
                
            break
    
    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/remove/<server>/<port>/<group>/<topic>/<clientid>', methods=['GET'])
def removeclient(server, port, group, topic, clientid):
    info = []
    folder = server + "_" + port + "/" + group + "_" + topic
    for item in processes:
        print (item.clientId + " === " + clientid)
        if item.clientId == clientid:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")

            remove(processes, item.file)
            item.status  =  0
            info = item
            break

    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/remove/<id>/<clientid>', methods=['GET'])
def removeclientbyid(id, clientid):
    info = []
    folder =  base64.decodestring(id);
    for item in processes:
        print (item.clientId + " === " + clientid)
        if item.clientId == clientid:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")
            remove(processes, item.file)
            item.status  =  0
            info = item
            break
    
    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/stop/<id>/<clientid>', methods=['GET'])
def stopclientbyid(id, clientid):
    info = []
    folder =  base64.decodestring(id);
    for item in processes:
        print (item.clientId + " === " + clientid)
        if item.clientId == clientid:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")

            item.status  =  0
            info = item
            break
    
    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/remove/<server>/<port>/<group>/<topic>', methods=['GET'])
def removellgrouptopicclient(server, port, group, topic):
    info = []
    removed  = []
    folder = server + "_" + port + "/" + group + "_" + topic

    for item in processes:
        print(item.folder + " == " + folder)
        if item.folder == folder:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")

            item.status  =  0
            removed.append(item)
            info.append(item)

    for item in removed:
        remove(processes, item.file)

    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/remove/<id>', methods=['GET'])
def removeallbyid(id):
    info = []
    removed  = []
    folder = base64.decodestring(id)

    for item in processes:
        print(item.folder + " == " + folder)
        if item.folder == folder:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")

            item.status  =  0
            removed.append(item)
            info.append(item)

    for item in removed:
        remove(processes, item.file)

    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson

@app.route('/kafka2bq/stop/<id>', methods=['GET'])
def stopallbyid(id):
    info = []
    removed  = []
    folder = base64.decodestring(id)

    for item in processes:
        print(item.folder + " == " + folder)
        if item.folder == folder:
            pid =  item.pid
            try:
                print("\n\nKill " + str(pid))
                os.kill(pid, signal.SIGTERM) #or signal.SIGKILL
            except:
                print("Already killed")

            item.status  =  0
            info.append(item)

    for item in removed:
        remove(processes, item.file)

    save_processes_to_file()
    sjson = json.dumps(info, default=obj_dict)
    return sjson


load_process_from_file()

atexit.register(stop_all_process)

print("Running kafka2bq server on " + host + ":"  + str(port))
app.run(host, port, debug=True, use_reloader=False)



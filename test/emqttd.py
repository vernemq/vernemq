from subprocess import call
import os
import filecmp
from shutil import move, copyfile

BROKER_PATH = "../emqttd1"

FNULL = open(os.devnull, 'w')


def config_file(broker_path):
    return broker_path + "/etc/emqttd.conf"

def bin(broker_path):
    return broker_path + "/bin/emqttd"

def adm(broker_path):
    return broker_path + "/bin/emqttd-admin"


def start(config_file, broker_path=BROKER_PATH):
    call([adm(broker_path), "clean"])
    ensure_alive(config_file, broker_path)


def stop(broker_path=BROKER_PATH):
    pass

def hard_stop(broker_path=BROKER_PATH):
    call([bin(broker_path), "stop"])



def node_alive(broker_path):
    if call([bin(broker_path), "ping"], stdout=FNULL) is 0:
        return True
    else:
        return False


def ensure_alive(conf_file, broker_path):
    if not filecmp.cmp(conf_file, config_file(broker_path)):
        copyfile(conf_file, config_file(broker_path))

    start_(broker_path)

def start_(broker_path):
    print "start broker %s" % broker_path
    if call([bin(broker_path), "start"]) is 0:
        if node_alive(broker_path):
            return True
        else:
            print "can't start broker on %s" % broker_path
            return False

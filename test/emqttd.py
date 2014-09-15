from subprocess import call
import os
import filecmp
from shutil import move, copyfile

BROKER_PATH = "../emqttd"
CONFIG_FILE = BROKER_PATH + "/etc/emqttd.conf"
BIN = BROKER_PATH + "/bin/emqttd"
ADM_BIN = BROKER_PATH + "/bin/emqttd-admin"

FNULL = open(os.devnull, 'w')


def start(config_file):
    call([ADM_BIN, "clean"])
    ensure_alive(config_file)


def stop():
    pass


def node_alive():
    if call([BIN, "ping"], stdout=FNULL) is 0:
        return True
    else:
        return False


def ensure_alive(config_file):
    is_alive = node_alive(),
    if filecmp.cmp(config_file, CONFIG_FILE):
        if is_alive:
            # same conig file and node is alive
            return
        else:
            call([BIN, "start"])
            return
    elif is_alive:
        call([BIN, "stop"])

    copyfile(config_file, CONFIG_FILE)
    call([BIN, "start"])

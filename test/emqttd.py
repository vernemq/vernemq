from subprocess import call
import os
import filecmp
from shutil import copyfile

BROKER_PATH = "../emqttd1"

FNULL = open(os.devnull, 'w')


def config_file(broker_path):
    return broker_path + "/etc/emqttd.conf"


def bin(broker_path):
    return broker_path + "/bin/emqttd"


def adm(broker_path):
    return broker_path + "/bin/emqttd-admin"


def start(config_file, broker_path=BROKER_PATH):
    call([adm(broker_path), "clean-msg-store"], stderr=FNULL)
    call([adm(broker_path), "clean-registry"], stderr=FNULL)
    reload_config(broker_path)
    ensure_alive(config_file, broker_path)


def stop(broker_path=BROKER_PATH):
    pass


def hard_stop(broker_path=BROKER_PATH):
    call([bin(broker_path), "stop"], stderr=FNULL, stdout=FNULL)


def node_alive(broker_path):
    if call([bin(broker_path), "ping"], stderr=FNULL, stdout=FNULL) is 0:
        return True
    else:
        return False


def ensure_alive(conf_file, broker_path):
    conf_file_differs = not filecmp.cmp(conf_file, config_file(broker_path))
    if conf_file_differs:
        copyfile(conf_file, config_file(broker_path))
        hard_stop(broker_path)

    start_(broker_path)


def start_(broker_path):
    if call([bin(broker_path), "start"], stderr=FNULL, stdout=FNULL) is 0:
        if node_alive(broker_path):
            return True
        else:
            print "can't start broker on %s" % broker_path
            return False


def reload_config(broker_path):
    if node_alive(broker_path):
        call([adm(broker_path), "reload-config"], stdout=FNULL)

# _*_ mode: ruby _*_
# vi: set ft=ruby :

$script = <<SCRIPT

if [ "$2" = "apt" ]; then
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y upgrade
    if [ "$1" = "precise"]; then
        # precise comes with a too old version of git, not compatible with rebar3
        sudo apt-get install -y software-properties-common python-software-properties
        sudo add-apt-repository -y ppa:git-core/ppa
        sudo apt-get update
    fi
    sudo apt-get -y install curl build-essential git packaging-dev libssl-dev openssl libncurses5-dev
elif [ "$2" = "yum" ]; then
    sudo yum -y update
    sudo yum -y groupinstall 'Development Tools'
    sudo yum -y install curl git ncurses-devel openssl openssl-devel
fi

    curl -O https://raw.githubusercontent.com/yrashk/kerl/master/kerl
    chmod a+x kerl
    ./kerl update releases
    ./kerl build $4 $4
    mkdir -p erlang
    ./kerl install $4 erlang/
    . erlang/activate
    
    rm -Rf vernemq
    git clone git://github.com/erlio/vernemq
    cd vernemq
    git pull
    git checkout $3
    make rel
SCRIPT

$vernemq_release = '0.12.4'
$erlang_release = '17.5'

$configs = {
    :jessie => {:sys => :apt, :img => 'debian/jessie64'},
    :wheezy => {:sys => :apt, :img => 'debian/wheezy64'},
    :trusty => {:sys => :apt, :img => 'ubuntu/trusty64'},
    :precise => {:sys => :apt, :img => 'ubuntu/precise64'},
    :centos7 => {:sys => :yum, :img => 'puppetlabs/centos-7.0-64-nocm'},
}

Vagrant.configure(2) do |config|
    $configs.each do |dist, dist_config|
        config.vm.define dist do |c|
            c.vm.box = dist_config[:img]
            c.vm.provision :shell do |s|
                s.privileged = false
                s.inline = $script
                s.args = ["#{dist}", "#{dist_config[:sys]}", $vernemq_release, $erlang_release]
            end
        end
    end
end

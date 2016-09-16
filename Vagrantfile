# _*_ mode: ruby _*_
# vi: set ft=ruby :

$script = <<SCRIPT

if [ "$2" = "apt" ]; then
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y upgrade
    if [ "$1" = "precise" ]; then
        # precise comes with a too old version of git, not compatible with rebar3
        sudo apt-get install -y software-properties-common python-software-properties
        sudo add-apt-repository -y ppa:git-core/ppa
        # precise comes with a too old C++ compiler, not compatible with mzmetrics
        sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
        sudo apt-get update
        sudo apt-get -y install build-essential gcc-4.8 g++-4.8
        sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 50
        sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 50
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
    mkdir -p erlang-$4
    ./kerl install $4 erlang-$4/
    . erlang-$4/activate
    
    if cd vernemq; then 
        git checkout master
        git pull
    else 
        git clone git://github.com/erlio/vernemq vernemq 
        cd vernemq
    fi
    git checkout $3

    make rel
SCRIPT

$vernemq_release = '0.14.2'
$erlang_release = '18.3'

$configs = {
    :jessie => {:sys => :apt, :img => 'debian/jessie64'},
    :wheezy => {:sys => :apt, :img => 'debian/wheezy64'},
    :trusty => {:sys => :apt, :img => 'ubuntu/trusty64', :primary => true},
    :precise => {:sys => :apt, :img => 'ubuntu/precise64'},
    :centos7 => {:sys => :yum, :img => 'puppetlabs/centos-7.0-64-nocm'},
    :xenial => {:sys => :apt, :img => 'ubuntu/xenial64'},
}

Vagrant.configure(2) do |config|
    $configs.each do |dist, dist_config|
        config.vm.define dist, 
            primary: dist_config[:primary],
            autostart: dist_config[:primary] do |c|
            c.vm.box = dist_config[:img]
            c.vm.provision :shell do |s|
                s.privileged = false
                s.inline = $script
                s.args = ["#{dist}", "#{dist_config[:sys]}", $vernemq_release, $erlang_release]
            end
        end
    end
end

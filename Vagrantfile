# _*_ mode: ruby _*_
# vi: set ft=ruby :

$script = <<SCRIPT

if [ "$1" = "apt" ]; then
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y upgrade
    sudo apt-get -y install curl build-essential git packaging-dev libssl-dev openssl libncurses5-dev
elif [ "$1" = "yum" ]; then
    sudo yum -y update
    sudo yum -y groupinstall 'Development Tools'
    sudo yum -y install curl git ncurses-devel openssl openssl-devel
fi

    curl -O https://raw.githubusercontent.com/yrashk/kerl/master/kerl
    chmod a+x kerl
    ./kerl update releases
    ./kerl build $3 $3
    mkdir -p erlang
    ./kerl install $3 erlang/
    . erlang/activate
    
    git clone git://github.com/erlio/vernemq
    cd vernemq
    git pull
    git checkout $2
    make rel
SCRIPT

$vernemq_release = '0.12.4'
$erlang_release = '17.5'

Vagrant.configure(2) do |config|

  config.vm.define 'jessie' do |c|
    c.vm.box = 'debian/jessie64'
    c.vm.provision :shell do |s|
        s.privileged = false
        s.inline = $script
        s.args = ['apt', $vernemq_release, $erlang_release]
    end
  end

  config.vm.define 'wheezy' do |c|
    c.vm.box = 'debian/wheezy64'
    c.vm.provision :shell do |s|
        s.privileged = false
        s.inline = $script
        s.args = ['apt', $vernemq_release, $erlang_release]
    end
  end

  config.vm.define 'trusty' do |c|
    c.vm.box = 'ubuntu/trusty64'
    c.vm.provision :shell do |s|
        s.privileged = false
        s.inline = $script
        s.args = ['apt', $vernemq_release, $erlang_release]
    end
  end

  config.vm.define 'precise' do |c|
    c.vm.box = 'ubuntu/precise64'
    c.vm.provision :shell do |s|
        s.privileged = false
        s.inline = $script
        s.args = ['apt', $vernemq_release, $erlang_release]
    end
  end

  config.vm.define 'centos7' do |c|
    c.vm.box = 'puppetlabs/centos-7.0-64-nocm'
    c.vm.provision :shell do |s|
        s.privileged = false
        s.inline = $script
        s.args = ['yum', $vernemq_release, $erlang_release]
    end
  end

  config.vm.define 'centos6' do |c|
    c.vm.box = 'puppetlabs/centos-6.6-64-nocm'
    c.vm.provision :shell do |s|
        s.privileged = false
        s.inline = $script
        s.args = ['yum', $vernemq_release, $erlang_release]
    end
  end

end

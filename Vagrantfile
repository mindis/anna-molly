# -*- mode: ruby -*-
# vi: set ft=ruby :

#
# Usage:
# ~~~~~~
#
#   in development:   vagrant up
#   in stage (ci):    vagrant up --provider=aws
#
#   Note: http://docs.vagrantup.com/v2/providers/default.html;
#         the default provider is virtual box and can only be
#         set using the environment variable 'VAGRANT_DEFAULT_PROVIDER'
#

#
# Description:
# ~~~~~~~~~~~~
#
#   This is a common Vagrantfile for our development and staging area.
#   The staging area works exactly like the development area but is using
#   the "aws" provider instead virtual box.
#
#

# import statements
require 'json'

# Define your service name here!
service_name          = 'anna-molly'
fail 'No service name defined' if service_name.empty?


# variables - virtual box specific
VB_MEMORY             = 1024
VB_CPUS               = 1

# set constants
hostname = "anna-molly"

# vagrant configuration
Vagrant.configure('2') do |config|
  config.vm.box = 'tm-infrastructure-chef-precise64'
  config.vm.box_url = 'https://cloud-images.ubuntu.com/vagrant/precise/current/precise-server-cloudimg-amd64-vagrant-disk1.box'
  config.vm.hostname = hostname
  config.vm.network 'private_network', ip: '192.168.33.10'
  config.vm.synced_folder '.', '/opt/anna-molly'

  # configure the virtualbox provider
  config.vm.provider :virtualbox do |vb|
    vb.customize ['modifyvm', :id, '--ioapic', 'on']
    vb.customize ['modifyvm', :id, '--memory', VB_MEMORY]
    vb.customize ['modifyvm', :id, '--cpus', VB_CPUS]
  end
end

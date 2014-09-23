#!/bin/bash

sudo wget -q http://repos.sensuapp.org/apt/pubkey.gpg -O- | sudo apt-key add -

sudo bash -c 'echo "deb     http://repos.sensuapp.org/apt sensu main" > /etc/apt/sources.list.d/sensu.list'

sudo apt-get update; sudo apt-get install -y sensu


sudo bash -c 'echo '"'"'{
  "rabbitmq": {
    "host": "172.16.0.22",
    "port": 5672,
    "vhost": "/sensu",
    "user": "sensu",
    "password": "sensupass"
  },
  "client": {
    "name": "chicago-'"$1"'-'"$HOSTNAME"'",
    "address": "172.16.0.22",
    "subscriptions": [
      "tests", "chicago-tests", "master-tests"
    ]
  }
}'"'"' > /etc/sensu/config.json'

sudo /etc/init.d/sensu-client restart

#!/bin/bash

# TODO: pass in the master host IP address as argument

if [ ! -f /etc/init.d/sensu-client ]; then

  echo "Installing Sensu!"

  sudo wget -q http://repos.sensuapp.org/apt/pubkey.gpg -O- | sudo apt-key add -

  sudo bash -c 'echo "deb     http://repos.sensuapp.org/apt sensu main" > /etc/apt/sources.list.d/sensu.list'

  sudo apt-get update; sudo apt-get install -y sensu

fi

sudo bash -c 'echo '"'"'{
  "rabbitmq": {
    "host": "172.16.0.22",
    "port": 5672,
    "vhost": "/sensu",
    "user": "sensu",
    "password": "sensupass"
  },
  "client": {
    "name": "chicago-'"$1"'-'"$2"'",
    "address": "'"$3"'",
    "subscriptions": [
      "tests", "chicago-tests", "worker-tests"
    ]
  }
}'"'"' > /etc/sensu/config.json'

sudo /etc/init.d/sensu-client restart

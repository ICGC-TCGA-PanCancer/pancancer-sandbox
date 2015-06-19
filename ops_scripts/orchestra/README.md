### orchestra  (Still not fully tested- I'm poking at this when I have a free minute here and there.)

This is an experimental webservice for managing machines in a subnet.<br>
On your launcher node, clone this repo and navigate to this folder.<br>

It's a very thin and simple webservice that runs on all worker machines.<br>
You can run ```orchestra list``` to poll the entire subnet to find workers.<br>
This allows you to generate lists of workers from the command line to poll or schedule to.<br><br>

#### Provision an Entire Subnet Automatically

To get started, put the CIDR of your subnet in this file:<br>
```vi ~/.orchestra_subnet```<br><br>

Next, edit the install script to point to your ssh keyfile:<br>
```vi install.sh```<br><br>

Once this is in place, you can install orchestra on the whole subnet:<br>
```bash install.sh```<br><br>

This will take some time to complete.<br>
Once it's done, you can do the following to confirm you can manage your machines:<br>
```orchestra list```<br><br>
This command will take some time to run as it tries all IP addresses available to find workers<br>
in your subnet.  Once it has finished, it creates a cache to make subsequent commands execute quickly.<br>
```orchestra busy``` will list all the machines currently running docker containers.<br>
```orchestra lazy``` will list all the machines not currently running workflows.<br><br>


#### Provision A Single Worker
Get the IP of your host.<br>
```bash install/push.sh [ip address]```<br><br>

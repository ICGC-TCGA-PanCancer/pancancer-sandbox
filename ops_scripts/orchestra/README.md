### orchestra  (Still not fully tested- I'm poking at this when I have a free minute here and there.)

This is an light webservice for managing machines in a subnet.<br>
On your launcher node, clone this repo and navigate to this folder.<br>

It's a very thin and simple webservice that runs on all worker machines.<br>
You can leverage the cli to poll machines to find out who is running a workflow, who is idle, who has failed workflows.<br>
This makes scheduling work to your cluster relatively easy and trouble free.  Various mechanisms are used to avoid double booking machines.<br><br>

#### Provisioning Your Fleet

On your launcher, or controll node, clone this repo with the correct branch.<br>
Navigate to the folder containing the ```install.sh``` script.<br>
You need to decide if you are going to auto-provision, or manually list the machines to install on.<br>

Automatic provisioning will attempt to install the webservice on ALL machines in a subnet, except for <br>
the orchestra master node (this meaning your launcher, or control machine.)<br>

To try automatic provisioning, but the CIDR of your subnet into this file:<br>
```vi ~/.orchestra_subnet```<br><br>

To manually provision, create an ansible inventory file of your own like so:<br>
```[ seqware_worker ]```<br>
```192.168.0.1     ansible_ssh_private_key_file=/home/ubuntu/.ssh/myssh.pem```<br>
```192.168.0.2    ansible_ssh_private_key_file=/home/ubuntu/.ssh/myssh.pem```<br>
<br>

Once this is in place, you can install orchestra on the whole subnet:<br>
```bash install.sh```  to use automatic provisioning<br>
```bash install.sh inventoryfile```  to use your custom inventory file<br>
<br>

This will take some time to complete.<br><br>

Now once you have installed the webservice everywhere, you need to create a list of IP's to manage with the cli:<br>
```vi ~/.orchestra_cache```<br><br>

This list of IP's, will be polled by the CLI to allow you to use the following types of commands:<br>
```orchestra busy``` will list all the machines currently running docker containers.<br>
```orchestra lazy``` will list all the machines not currently running workflows.<br>
```orchestra help``` will list the scheduling and other polling commands available to you.<br><br>


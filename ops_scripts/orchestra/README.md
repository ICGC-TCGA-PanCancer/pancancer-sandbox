## orchestra  (It totally works!)

This is an thin webservice and CLI for managing machines in a subnet- it's totally KISS approach based.<br>

You can leverage the cli to poll machines to find out who is running a workflow, who is idle, who has failed workflows.<br>
This makes scheduling work to your cluster relatively easy and trouble-free.  Various mechanisms are used to avoid double booking machines.<br><br>

Just run the install.sh script, it will install the dependencies for you and push the webservice out to hosts.<br>
You only need to decide on what kind of install to try.<br><br>

#### Provisioning Your Fleet  (You'll need to configure some settings first, keep scrolling down.)

On your launcher, or control node, clone this repo with the correct branch.<br>
Navigate to the folder containing the ```install.sh``` script.<br>
You need to decide if you are going to auto-provision, or manually list the machines to install on.<br>
*Automatic provisioning works quite well, so it's the method I'm recommending*<br>

Automatic provisioning will attempt to install the webservice on ALL machines in a subnet, except for <br>
the orchestra master node (this meaning your launcher, or control machine.)<br>

To try automatic provisioning, put the CIDR of your cloud's subnet into this file:<br>
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

The install script will create a cached copy of all hosts the webservice was installed on:<br>
*If you blow away hosts, remove the IP from this cache file*<br>
```~/.orchestra_cache``` <br><br>

This cache will be used by the CLI to communicate with the webservice to run commands:<br>
```orchestra busy``` will list all the machines currently running docker containers.<br>
```orchestra lazy``` will list all the machines not currently running workflows.<br>
```orchestra help``` will list the scheduling and other polling commands available to you.<br><br>

#### Configuration

I hate configuration files.  But I hate editing scripts even more...<br>
This should be considered a demo right now, so you'll have to modify the following files to suit your environment:<br<br>

```vi install.sh``` swap out the pem key path with your own pem file.<br>
```vi scheduling/schedule_docker.py``` modify the pem key, gnos key and ini file paths to match your launcher's environment.  You'll also need to select a workflow from the list at the top of this file.  Once that's done you're good to go.<br><br>

That should be it.<br>


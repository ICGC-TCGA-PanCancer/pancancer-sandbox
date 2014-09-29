use strict;
use Getopt::Long;
use Config;
$Config{useithreads} or die('Recompile Perl with threads to run this program.');
use threads 'exit' => 'threads_only';
#use threads;

# TODO:
# * need to include multiple run check
# * need to include setup of monitoring
# * need to include restart of failed workflows
# * code to restart HDFS daemons
# * params below that are hardcoded need to be arguments

my ($glob_target) = @ARGV;

my $test = 0;
my $verbose = 0;
my $setup_sensu = 0;
my $glob_base = "";
my $glob_target = "target-*";
my $sensu_worker = "/glusterfs/netapp/homes1/BOCONNOR/gitroot/pancancer-sandbox/bionimbus_monitor/setup_sensu_worker.sh";
my $sensu_master = "/glusterfs/netapp/homes1/BOCONNOR/gitroot/pancancer-sandbox/bionimbus_monitor/setup_sensu_master.sh";
my $global_max_it = 30;
my $global_wait_time = 2;
my $cleanup_jobs = 1;
my $restart_workflows = 0;
my $workflow_accession = 2;
my $seqware_oozie_retry = '/glusterfs/netapp/homes1/BOCONNOR/gitroot/seqware-sandbox/seqware-oozie-restart/seqware-oozie-retry.pl';

check_if_running();

if (scalar(@ARGV) < 1 || scalar(@ARGV) > 7) {
 die "USAGE: perl $0 [--test] [--verbose] [--setup-sensu] [--glob-base <path to directory that contains bindle dirs>] [--glob-target <target-*>]\n";
}

GetOptions(
  "test" => \$test,
  "verbose" => \$verbose,
  "setup-sensu" => \$setup_sensu,
  "glob-base=s" => \$glob_base,
  "glob-target=s" => \$glob_target,
);

print "\n\n\n\n";
print "##############################\n";
print "# RUN DATE: ".`date`;
print "##############################\n";

my $glob_path = $glob_target;
if ($glob_base ne "") { $glob_path = "$glob_base/$glob_target"; }

foreach my $target (glob($glob_path)) {
  if (-d $target) {
    my $master_ip;
    my $cluster_name;
    #next if (defined($glob_target) && $glob_target ne '' && $glob_target ne $target );
    print "\n";
    print "##############################\n";
    print "# EXAMINING CLUSTER: $target #\n";
    print "##############################\n";
    foreach my $host ("$target/master", glob("$target/worker*")) {
      if (-d $host) {
        $host =~ /$target\/(\S+)/;
        my $hostname = $1;
        my $ssh_config = `cd $host && vagrant ssh-config 2> /dev/null`;
        #print "SSH CMD: cd $host && vagrant ssh-config 2> /dev/null\n";
        #print "SSH CONFIG: $ssh_config\n";
        $ssh_config =~ /HostName\s+(\d+\.\d+\.\d+\.\d+)/;
        my $curr_ip = $1;
        $ssh_config =~ /User\s+(\S+)/;
        my $curr_username = $1;
        $ssh_config =~ /IdentityFile\s+(\S+)/;
        my $curr_pem = $1;

        if ($hostname eq 'master') {
            $master_ip = $curr_ip;
            print "MASTER IP: $master_ip\n";
            $target =~ /([^\/]+)$/;
            $cluster_name = $1;
        }
        print "\n##############################\n";
        print "#       HOST: $hostname      #\n";
        print "##############################\n\n";

        my $r = 0;
        my $thr = threads->create(\&launch_ssh, "ssh -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no -i $curr_pem $curr_username\@$curr_ip hostname");
        #print "ssh -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no -i $curr_pem $curr_username\@$curr_ip\n";
        my $max_it = $global_max_it;
        while($max_it > 0) {
          print "SSH WAIT LOOP: $max_it\n";
          sleep $global_wait_time;
          if ($thr->is_joinable()) {
            # then we can exit now
            print "SSH THREAD FINISHED EARLY, BREAKING\n";
            $max_it = 0;
          }
          $max_it--;
          #my $r = system("cd $host && vagrant ssh -c 'hostname' 2> /dev/null");
        }
        if ($max_it <= 0 && ($thr->is_running() || defined($thr->error()))) {
          print "SSH THREAD STILL RUNNING OR IN ERROR: ".$thr->is_running()." ".$thr->error()."\n";
          $r = 1;
          # TODO: need to send the interrupt signal here
          $thr->kill('KILL')->detach();
          #threads->exit();
        } elsif ($thr->is_joinable()) {
          $thr->join();
          $r = 0;
        }
        print "SSH RESULT: $r\n";

        #print "  cd $host && vagrant ssh -c 'hostname' 2> /dev/null\n";
        #print "  CMD STATUS: $r\n";
        # need to restart, ssh doesn't work!
        if ($r != 0) {
          print "  REBOOTING HOST\n";
          my $ip = `cd $host && vagrant ssh-config | grep HostName | awk '{print \$2}' 2> /dev/null`;
          chomp $ip;
          if ($ip =~ /\d+\.\d+\.\d+\.\d+/) {
            my $nova_id = `bash -l -c 'nova list | grep "$ip " | awk "{print \\\$2}"'`;
            chomp $nova_id;
            #print "  IP: $ip NOVA_ID $nova_id\n";
            # reboot here
            reboot_host($nova_id);
          }
        } else {

          # now check the hostname
          my $remote_name = `cd $host && vagrant ssh -c 'hostname' 2> /dev/null`;
          #print "cd $host && vagrant ssh -c 'hostname' 2> /dev/null\n";
          print "  REMOTE NAME: $remote_name\n";
          chomp $remote_name;
          $remote_name =~ /(\S+)/;
          $remote_name = $1;
          if ($remote_name ne $hostname) {
            print "  RESETTING HOST: REMOTE: $remote_name LOCAL: $hostname\n";
            reset_host($host, $hostname, $master_ip);
          } else {
            print "  HOST OK\n";
          }

          # install sensu if specified
          if ($setup_sensu && !$test) {
            my $cmd = "cd $host && vagrant ssh -c 'sudo bash $sensu_worker $cluster_name $hostname $curr_ip' 2> /dev/null";
            if ($hostname eq 'master') {
              $cmd = "cd $host && vagrant ssh -c 'sudo bash $sensu_master $cluster_name $hostname $curr_ip' 2> /dev/null";
            }
            my $r = system($cmd);
            if ($r) { print "  SENSU INSTALL FAILED\n"; }
          }

          # checks to cleanup any SGE jobs on dead hosts
          if ($cleanup_jobs && $hostname eq 'master') {
            my $failed_nodes = {};
            my $clean = `cd $host && vagrant ssh -c 'qstat -f'`;
            my @lines = split /\n/, $clean;
            foreach my $line (@lines) {
              if ($line =~ /\s+au\s+/) {
                $line =~ /main.q\@(\S+)/;
                $failed_nodes->{$line} = 1;
              }
            }
            print Dumper($failed_nodes);
            # kill those jobs
            my $kill_list = "";
            $clean = `cd $host && vagrant ssh -c 'qstat'`;
            @lines = split /\n/, $clean;
            foreach my $line (@lines) {
              $line =~ /main.q\@(\S+)/;
              print "EXAMINED HOST: $1\n";
              if ($failed_nodes->{$1}) {
                $line =~ /^\s+(\d+)\s+/;
                $kill_list .= " $1";
              }
            }
            my $cmd = "cd $host && vagrant ssh -c 'sudo qdel -f $kill_list'";
            print "KILL STUCK SGE JOBS: $cmd\n";
            if (!$test && length($kill_list) > 0) {
              my $r = system($cmd);
              if ($r) { print "PROBLEMS KILLING SGE JOBS!\n"; }
            }
          }

          # checks to restart failed workflows
          if ($restart_workflows && $hostname eq 'master') {
            my $clean = `cd $host && vagrant ssh -c 'seqware workflow report --accession $workflow_accession'`;
            my @lines = split /\n/, $clean;
            my $accession;
            my $status;
            my $working_dir;
            my $eid;
            foreach my $line (@lines) {
              if ($line =~ /Workflow Run SWID\s+|\s+(\d+)/) { $accession = $1; }
              elsif ($line =~ /Workflow Run Status\s+|\s+(\S+)/) { $status = $1; }
              elsif ($line =~ /Workflow Run Working Dir\s+|\s+(\S+)/) { $working_dir = $1; }
              elsif ($line =~ /Workflow Run Engine ID\s+|\s+(\S+)/) { $eid = $1; }
              elsif ($line =~ /Library Sample Names/ && $status eq 'failed') {
                my $cmd = "cd $host && vagrant ssh -c '$seqware_oozie_retry $working_dir $eid $accession'";
                print "RESTARTING SEQWARE WORKFLOWS: $cmd\n";
                if (!$test) {
                  my $r = system($cmd);
                  if ($r) {
                    print "PROBLEMS RESTARTING WORKFLOW! '$seqware_oozie_retry $working_dir $eid $accession'\n";
                  }
                }
              }
            }
          }

        }
      }
    }
  }
}

sub reset_host {
  my ($dir, $host, $master_ip) = @_;
  #my $cmd = "cd $dir && vagrant ssh -c 'sudo hostname $host && sudo mount $master_ip:/home /home && sudo mount $master_ip:/mnt/home /mnt/home && sudo mount $master_ip:/mnt/datastore /mnt/datastore && sudo /etc/init.d/gridengine-exec restart; if [ -e /etc/init.d/gridengine-master ]; then sudo /etc/init.d/gridengine-master restart; fi;'";
  # FIXME: for now this will just disable hosts in SGE
  my $cmd = "cd $dir && vagrant ssh -c 'sudo /etc/init.d/gridengine-exec stop; sudo hostname $host && sudo mount $master_ip:/home /home && sudo mount $master_ip:/mnt/home /mnt/home && sudo mount $master_ip:/mnt/datastore /mnt/datastore && sudo /etc/init.d/hadoop-hdfs-datanode restart && sudo /etc/init.d/hadoop-0.20-mapreduce-tasktracker restart && sudo bash $sensu_worker' 2> /dev/null";
  if ($host eq 'master') {
    # FIXME: need to restart the master for sure
    $cmd = "cd $dir && vagrant ssh -c 'sudo /etc/init.d/gridengine-exec stop && sudo /etc/init.d/gridengine-master stop && sudo hostname $host && sudo /etc/init.d/gridengine-master start && sudo /etc/init.d/hadoop-hdfs-namenode restart && sudo /etc/init.d/hadoop-0.20-mapreduce-jobtracker restart && sudo /etc/init.d/hadoop-0.20-mapreduce-tasktracker restart && sudo /etc/init.d/hadoop-hdfs-datanode restart && sudo bash $sensu_master' 2> /dev/null";
  }
  if ($verbose) { print "    RESETTING CMD: $cmd\n"; }
  if (!$test) {
    my $r = system($cmd);
    if ($r) { print "PROBLEMS RESETTING HOST NAME\n"; }
  }
}

sub reboot_host {
  my ($nova_id) = @_;
  my $cmd = "bash -l -c 'nova reboot $nova_id'";
  if ($verbose) { print "    REBOOTING CMD: $cmd\n"; }
  if (!$test) {
    my $r = system($cmd);
    if ($r != 0) { print "PROBLEMS REBOOTING HOST\n"; }
  }
}

sub launch_ssh {
  $SIG{'KILL'} = sub { threads->exit(); };
  my $ssh = $_[0];
  system("$ssh");
  print "DONE WITH SSH\n";
}

sub check_if_running {
  my $r = `ps aux | grep perl | grep $0 | grep -v grep | wc -l`;
  chomp $r;
  if ($r >1) { die "EXIT: more than one $0 is running!\n"; }
}

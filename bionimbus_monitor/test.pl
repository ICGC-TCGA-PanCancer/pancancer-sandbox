use strict;

# TODO: need to include multiple run check


foreach my $target (glob("target-*")) {
  if (-d $target) {
    my $master_ip;
    print "EXAMINING CLUSTER: $target\n";
    foreach my $host ("$target/master", glob("$target/worker*")) {
      if (-d $host) {
        $host =~ /$target\/(\S+)/;
        my $hostname = $1;
        if ($hostname eq 'master') {
            $master_ip = `cd $host && vagrant ssh-config | grep HostName | awk '{print \$2}'`;
            chomp $master_ip;
        }
        print "HOST: $hostname\n";
        my $r = system("cd $host && vagrant ssh -c hostname");
        print "  CMD STATUS: $r\n";
        # need to restart, ssh doesn't work!
        if ($r != 0) {
          print "  REBOOTING HOST\n";
          my $ip = `cd $host && vagrant ssh-config | grep HostName | awk '{print \$2}'`;
          chomp $ip;
          if ($ip =~ /\d+\.\d+\.\d+\.\d+/) {
            my $nova_id = `bash -l -c 'nova list | grep "$ip " | awk "{print \\\$2}"'`;
            chomp $nova_id;
            print "  IP: $ip NOVA_ID $nova_id\n";
            # reboot here
            reboot_host($nova_id);
          }
        } else {
          
          # now check the hostname
          my $remote_name = `cd $host && vagrant ssh -c hostname`;
          print "  REMOTE NAME: $remote_name\n";
          chomp $remote_name;
          $remote_name =~ /(\S+)/;
          $remote_name = $1;
          if ($remote_name ne $hostname) {
            print "  REMOTE NAME AND HOSTNAME DON'T MATCH: REMOTE: $remote_name LOCAL: $hostname\n";
            reset_host($host, $hostname, $master_ip);
          } else {
            print "  REMOTE NAME AND HOSTNAME MATCH: REMOTE: $remote_name LOCAL: $hostname\n";
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
  my $cmd = "cd $dir && vagrant ssh -c 'sudo /etc/init.d/gridengine-exec stop; sudo hostname $host && sudo mount $master_ip:/home /home && sudo mount $master_ip:/mnt/home /mnt/home && sudo mount $master_ip:/mnt/datastore /mnt/datastore'";
  print "  RESETTING HOSTNAME: $cmd\n";
  my $r = system($cmd);
  if ($r) { print "PROBLEMS RESETTING HOST NAME\n"; }
}

sub reboot_host {
  my ($nova_id) = @_;
  my $cmd = "bash -l -c 'nova reboot $nova_id'";
  print "  REBOOTING: $cmd\n";
  my $r = system($cmd);
  if ($r != 0) { print "PROBLEMS REBOOTING HOST\n"; }
}

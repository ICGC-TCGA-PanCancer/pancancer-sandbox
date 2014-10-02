use strict;
use Data::Dumper;
use Getopt::Long;
use Data::UUID;
use JSON;

# NOTES
# http://stackoverflow.com/questions/15233535/perl-regex-substitution-for-a-url
# https://gtrepo-osdc-icgc.annailabs.com/cghub/data/analysis/download/441f1192-16ad-45cb-aefa-f4c3322a73dc


#############
#   USAGE   #
#############

if (scalar(@ARGV) < 6 || scalar(@ARGV) > 11) {
  die "USAGE: perl $0 --url <gnos_download_url> --url-file <url-file> --output <output.json> --pem <key_file.pem> --temp <temp_dir> --test"
}


#############
# VARIABLES #
#############

my @urls;
my $url_file;
my $output = "output.json";
my $pem;
my $tmp = "/mnt/";
my $test = 0;

GetOptions(
     "url=s" => \@urls,
     "url-file=s" => \$url_file,
     "output=s" => \$output,
     "pem=s" => \$pem,
     "temp=s" => \$tmp,
     "test" => \$test,
  );



##############
# MAIN STEPS #
##############

# find all the URLs
my $urls_hash = read_urls($url_file, \@urls);

# run the individual URLs and report individual results
my $url_runtimes = download_urls($urls_hash);

# consolodate runtimes per site
my $url_consol_runtimes = consolodate_runtimes($url_runtimes);

# print report
print_report($url_consol_runtimes, $output);



###############
# SUBROUTINES #
###############

sub read_urls {
  my ($url_file, $urls) = @_;
  my $d = {};
  foreach my $url(@{$urls}) {
    if ($url =~ m!^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?!) {
      $d->{$url} = 1;
    }
  }
  if (defined($url_file) && $url_file ne '' && -e $url_file) {
    open IN, "<$url_file" or die;
    while(<IN>) {
      chomp;
      next if($_ eq '');
      next if (/^\s*#/);
      if (m!^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?!) {
        $d->{$_} = 1;
      }
    }
    close IN;
  }
  return($d);
}

sub download_urls {
  my ($urls) = @_;
  my $d = {};

  foreach my $url (keys %{$urls}) {
    $d->{$url} = gtdownload($url);
  }
}

sub gtdownload {
  my ($url) = @_;
  my $d = {};
  my $temp_dir = mktmpdir($tmp);
  my $start = time;
  #my $cmd = "gtdownload $url -vv -c $pem -p $temp_dir --null-storage";
  # LEFT OFF WITH: test the null storage option
  my $cmd = "gtdownload $url -vv -c $pem -p $temp_dir";
  if ($url =~ /cghub/) {
    # need the public key
    $cmd = "gtdownload $url -vv -c cghub_public.key -p $temp_dir"
  }
  print "DOWNLOADING: $cmd\n";
  my $r = 0;
  if (!$test) {
    system($cmd);
    if ($r) { print " + Problems downloading!\n"; }
    my $stop = time;
    my $duration = $stop - $start;
    my $size = `du -s $temp_dir`;
    chomp $size;
    $size =~ /(\d+)/;
    $size = $1 / 1024 / 1024 / 1024;
    $d->{'GB'} = $size;
    $d->{'GB/s'} = $size / $duration;
    $d->{'start'} = $start;
    $d->{'stop'} = $stop;
    $d->{'duration'} = $duration;
    die "Can't clean up temp dir! $temp_dir\n" if system("rm -rf $temp_dir");
  }
  return($d);
}

sub mktmpdir {
  my ($tmp) = @_;
  my $ug = Data::UUID->new;
  my $uuid = lc($ug->create_str());
  die "Can't make temp dir $tmp/$uuid" if (system("mkdir -p $tmp/$uuid"));
  return("$tmp/$uuid");
}

sub consolodate_runtimes {

}

sub print_report {

}

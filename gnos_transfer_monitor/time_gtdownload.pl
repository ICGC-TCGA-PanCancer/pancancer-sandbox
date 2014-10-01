use strict;
use Data::Dumper;
use Getopt::Long;
use JSON;

# NOTES
# http://stackoverflow.com/questions/15233535/perl-regex-substitution-for-a-url
# https://gtrepo-osdc-icgc.annailabs.com/cghub/data/analysis/download/441f1192-16ad-45cb-aefa-f4c3322a73dc


#############
#   USAGE   #
#############

if (scalar(@ARGV) < 6 || scalar(@ARGV) > 8) {
  die "USAGE: perl $0 --url <gnos_download_url> --url-file <url-file> --output <output.json> --pem <key_file.pem>"
}


#############
# VARIABLES #
#############

my @urls;
my $url_file;
my $output = "output.json";
my $pem;

GetOptions(
     "url=s" => \@urls,
     "url-file=s" => \$url_file,
     "output=s" => \$output,
     "pem=s" => \$pem,
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
  my $r = system("gtdownload ");
}

sub consolodate_runtimes {

}

sub print_report {

}

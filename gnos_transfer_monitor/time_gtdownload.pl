use strict;
use Data::Dumper;
use Getopt::Long;
use JSON;

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

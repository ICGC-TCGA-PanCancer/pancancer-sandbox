use strict;
use Data::Dumper;
use Getopt::Long;
use Data::UUID;
use JSON;
use Template;


# NOTES
#

#############
#   USAGE   #
#############

if (scalar(@ARGV) < 0 || scalar(@ARGV) > 16) {
  die "USAGE: perl $0 --template <template_toolkit_template> --output <output_html> [--s3-url <url_to_json_report_for_input>]\n";
}


#############
# VARIABLES #
#############

my $template;
my $output;
my $s3 = "s3://pancancer-site-data/transfer_timing.json old.transfer_timing.json";

GetOptions(
     "template=s" => \$template,
     "output=s" => \$output,
     "s3=s" => \$s3,
  );



##############
# MAIN STEPS #
##############

# download the JSON input report
my $d = download_url($s3);

# parse the JSON
$d = parse_json($d);

# fill in the template
fill_template($d, $template, $output);



###############
# SUBROUTINES #
###############

sub download_url {
  my $r = system("s3cmd get --force s3://pancancer-site-data/transfer_timing.json old.transfer_timing.json");
  if ($r) { system("echo '{}' > old.transfer_timing.json"); }
  my $old = read_json("old.transfer_timing.json");
  return($old);
}

sub parse_json {
  my ($d) = @_;
  my $n = {};
  foreach my $site (keys %{$d}) {
    foreach my $date (reverse sort keys %{$d->{$site}}) {
      $n->{$site}{$date} = $d->{$site}{$date};
      last;
    }
  }
  return($n);
}

sub fill_template {
  my ($d, $file, $output) = @_;
  print Dumper($d);

  my $template = Template->new();
  $template->process($file, $d)
      || die "Template process failed: ", $template->error(), "\n";
}

sub read_json {
  my ($file) = @_;
  my $json_text;
  {
    local $/ = undef;
    open FILE, "$file" or die "Couldn't open file: $!";
    binmode FILE;
    $json_text = <FILE>;
    close FILE;
  }
  my $json = JSON->new->allow_nonref;
  my $d = $json->decode( $json_text );
  return($d);
}

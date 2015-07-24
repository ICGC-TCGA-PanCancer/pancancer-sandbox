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

if (scalar(@ARGV) < 1 || scalar(@ARGV) > 16) {
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

my %month = (
    '01' => 'Jan',
    '02' => 'Feb',
    '03' => 'Mar',
    '04' => 'Apr',
    '05' => 'May',
    '06' => 'Jun',
    '07' => 'Jul',
    '08' => 'Aug',
    '09' => 'Sep',
    '10' => 'Oct',
    '11' => 'Nov',
    '12' => 'Dec'
    );

# parse the JSON
$d = parse_json($d);
for my $key (keys %$d) {
    unless ($key eq 'virginia') {
	delete $d->{$key};
	next;
    }
    for my $date (keys %{$d->{$key}}) {
	my $data = $d->{$key}->{$date};
	delete  $d->{$key}->{$date};
	my ($year,$month,$day,$time) = $date =~ /^(\d{4})(\d{2})(\d{2})\.(\S+)$/;
	$date = "$month{$month} $day, 2015, $time UTC";
	$d->{$key}->{$date} = $data;
    }
}
my $text = JSON->new->utf8->encode($d);

# fill in the template
fill_template($d, $template, $output);



###############
# SUBROUTINES #
###############

sub download_url {
  my $r = system("s3cmd get --force s3://pancancer-site-data/transfer_timing.json old.transfer_timing.json > /dev/null");
  if ($r) { system("echo '{}' > old.transfer_timing.json"); }
  my $old = read_json("old.transfer_timing.json");
  return($old);
}

sub parse_json {
  my ($d) = @_;
  my $n = {};
  foreach my $site (keys %{$d}) {
    # TODO: add averaging of max x number of previous results
    foreach my $date (sort_dates(%{$d->{$site}})) {
      $n->{$site}{$date} = $d->{$site}{$date};
      last;
    }
  }
  return($n);
}

sub sort_dates {
    my %dates_hash;
    foreach my $datetime (@_) {
        next if ( (length $datetime == 17) and ($dates_hash{$datetime} = $datetime) );

        my ($date, $time) = split /\./, $datetime;
        
        my $padded_date;
        if (length( $date ) == 8) {
            $padded_date = $date;
        }
        else {
            my ($year, $month_day) = $date =~ /^(\d{4})(.*)$/;
            $padded_date = $year; 
            
            my ($month, $day);
            if (substr($month_day, 0, 1) == 1) {
                ($month, $day) = $month_day =~ /^(\d{2})(.*)$/;
            }
            else {
                ($month, $day) =  $month_day =~ /^(\d)(.*)$/;
            }

            $padded_date .= ($month < 10)? "0$month" : $month;
            $padded_date .= ($day < 10)? "0$day" : $day;
        }

        my $padded_datetime; 
        if (length( $time) == 8 ) {
            $padded_datetime = "$padded_date.$time";
        }
        else {
            $padded_datetime = "$padded_date.";
   
            my @time_parts = split /\:/, $time;
            foreach my $time (@time_parts) {
                $padded_datetime .= ($time < 10)? "0$time:": "$time:";
            }
            chop $padded_datetime;
        }

        $dates_hash{$padded_datetime} = $datetime;
    }

    my @sorted_keys = reverse sort keys(%dates_hash);
    my @sorted_original_dates = @dates_hash{@sorted_keys};
 
   return @sorted_original_dates;
} 


sub fill_template {
  my ($d, $file, $output) = @_;
  #print Dumper($d);
  my $data = {};
  $data->{data} = $d;
  my $template = Template->new();
  $template->process($file, $data)
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

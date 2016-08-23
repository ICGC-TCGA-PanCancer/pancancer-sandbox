#!/usr/bin/perl
use common::sense;

use Data::Dumper;
use Getopt::Long;
use Data::UUID;
use JSON;
use Email::MIME;
use Email::Sender::Simple qw(sendmail);

use constant CUTOFF => 1;

my $d = read_json('old.transfer_timing.json');
my ($zeros,$slow) = check_zeros($d);

my $last_json_file = "last_notification.json";
my $last_json = {};
my $last_epoch = 0;
if (-e $last_json_file && ! -z $last_json_file) {
    $last_json = read_json($last_json_file);
    $last_epoch = $last_json->{epoch};
} 

unlink $last_json_file;

my $msg;
my $new_last_json = {};

for my $zero (sort keys %$zeros) {
    my @z = @{$zeros->{$zero}};
    my $zcount = $slow->{$zero};
    my $hours = $zcount * 4;
    $msg .= "GNOS transfers from $zero to AWS Virginia have been below ".CUTOFF." MB/s for $hours of the past 24 hours\n";
    $msg .= join("\n",@z) . "\n";
    my $last_date = $last_json->{$zero};
    if ($last_date) {
	my $epoch = time;
	my $elapsed = sprintf '%.2f', ($epoch - $last_epoch)/3600;
	$msg .= "A previous notication for $zero was sent $elapsed hours ago at $last_date.\n";
    }

    $new_last_json->{$zero} = timestamp();
    $new_last_json->{epoch} ||= time;
    $msg .= "\n";
}

if ($msg) {
    my @emails = (
'sheldon.mckay@gmail.com',
#'briandoconnor@gmail.com',
#'Brian.OConnor@oicr.on.ca',
'Junjun.Zhang@oicr.on.ca',
'Linda.Xiang@oicr.on.ca',
'Christina.Yung@oicr.on.ca',
'mainsworth@annaisystems.com',
);
    my @zeros = grep {s/\.\S+//} keys %$zeros;
    my $slow_repos = join(", ", @zeros);
    for my $email (@emails) {
	my $message = Email::MIME->create(
	    header_str => [
		From    => 'sheldon.mckay@gmail.com',
		To      => $email,
		Subject => "GNOS slowdown for $slow_repos",
	    ],
	    attributes => {
		encoding => 'quoted-printable',
		charset  => 'ISO-8859-1',
	    },
	    body_str => $msg,
	    );


	sendmail($message);
	say $msg;
    }
}


if (keys %$new_last_json) {
    open JSON_FILE, ">$last_json_file" or die $!;
    print JSON_FILE encode_json($new_last_json);
    close JSON_FILE;
}
    



###############
# SUBROUTINES #
###############

sub last_24_hours {
    my ($d) = @_;
    my $n = {};
    my $count = 0;
    foreach my $site ('virginia') {
	foreach my $date (sort_dates(%{$d->{$site}})) {
	    $n->{$site}{$date} = $d->{$site}{$date};
	    last if ++$count == 6;
	}
    }
    return $n;
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

sub check_zeros {
    my $data = shift;
    my $zeros = {};
    my $last_24 = last_24_hours($data)->{'virginia'};
    my @dates = sort keys %$last_24;
    my %slow;
    for my $date (@dates) {
	my $nums = $last_24->{$date};
	my @repos = sort keys %$nums;
	for my $repo (@repos) {
	    my $rate = $nums->{$repo}->{'MB/s'};
	    $slow{$repo}++ if $rate <= CUTOFF;
	    push @{$zeros->{$repo}}, "$date\t$rate MB/s";
	}
    }
    
    for my $repo (keys %{$zeros}) {
	delete $zeros->{$repo} unless $slow{$repo} && $slow{$repo} >= 3;
    }

    return ($zeros,\%slow);
}


sub timestamp {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
    my $nice_timestamp = sprintf ( "%04d%02d%02d %02d:%02d",
                                   $year+1900,$mon+1,$mday,$hour,$min);
    return $nice_timestamp;
}

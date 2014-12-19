#!/usr/bin/perl

use strict;
use warnings FATAL => 'all';
use Data::Dumper;

use WWW::Curl::Easy;
use IO::Uncompress::Gunzip;
use List::Util qw(sum);
use JSON;

use Const::Fast qw(const);

const my $DIV_X => 3_000_000_000;
const my $EXP_GC => 40.9;
const my $MIN_SEQX => 25;
const my $MAX_ABS_GC_DEVIATION => 5;
const my $MAX_ISIZE_SD_FRAC => 0.3;
const my $MAX_DUP_FRAC => 0.15;
const my $READ_MAP_DIST_MAX => 0.25;


const my %HC => ( 'Tumour' => {
                                edit_dist => 1,
                                gc_r1_dev => 4,
                                gc_r2_dev => 16,
                                gc_dist => 64,
                                sd_frac => 256,
                                dup_frac => 1024,
                                end_map_disc => 4096,
                                low_seq => 16384,
                                no_qc => 65536,
                                not_aligned => 262144,
                                none => 1048576,
                                broken => 4194304,
                                },
                  'Normal' => { edit_dist => 2,
                                gc_r1_dev => 8,
                                gc_r2_dev => 32,
                                gc_dist => 128,
                                sd_frac => 512,
                                dup_frac => 2048,
                                end_map_disc => 8192,
                                low_seq => 32768,
                                no_qc => 131072,
                                not_aligned => 524288,
                                none => 2097152,
                                broken => 8388608,
                                },
                  );

# handy for viewing the JSON: http://jsonlint.com/

const my @DONOR_ORDERED_ISSUES => ( 'Normal available',
                                    'Normal low seqX',
                                    'Normal r1_GC deviation',
                                    'Normal r2_GC deviation',
                                    'Normal GC dist',
                                    'Normal isize_sd',
                                    'Normal edit dist',
                                    'Normal dup_frac',
                                    'Normal end map dist',
                                    'Normal qc_metric absent',
                                    'Normal broken',
                                    'Tumour available',
                                    'Tumour low seqX',
                                    'Tumour r1_GC deviation',
                                    'Tumour r2_GC deviation',
                                    'Tumour GC dist',
                                    'Tumour isize_sd',
                                    'Tumour edit dist',
                                    'Tumour dup_frac',
                                    'Tumour end map dist',
                                    'Tumour qc_metric absent',
                                    'Tumour broken',);

## NEED TO HANDLE FILE OUTPUT FOR BOTH TYPES, currently have mixed output, but process_aliquot is receiving a FH

my $raw_data;
if(@ARGV == 1) {
  $raw_data = process($ARGV[0]);
}
else {
  die "Please provide a file path to recieve data from, later versions will support a URL";
}

sub add_aliquot {
  my ($specimen, $aliquot_data, $norm_or_tum, $donor_issues) = @_;
  my %summary;

  my $max_insert_sd = 0;
  my $high_sd_rg;
  my ($mapped_r1, $mapped_r2, $all_r1, $all_r2) = (0,0,0,0);

  for my $rg (@{$specimen->{'alignment'}->{'qc_metrics'}}) {
    # I may want to assess readgroups at this level, but not just yet
    $summary{'#_mapped_bases'} += $rg->{'metrics'}->{'#_mapped_bases'};
    $summary{'#_gc_bases_r1'} += $rg->{'metrics'}->{'#_gc_bases_r1'};
    $summary{'#_gc_bases_r2'} += $rg->{'metrics'}->{'#_gc_bases_r2'};
    $summary{'#_bases_r1'} += $rg->{'metrics'}->{'#_total_reads_r1'} * $rg->{'metrics'}->{'read_length_r1'};
    $summary{'#_bases_r2'} += $rg->{'metrics'}->{'#_total_reads_r2'} * $rg->{'metrics'}->{'read_length_r2'};
    $summary{'#_divergent_bases_r1'} += $rg->{'metrics'}->{'#_divergent_bases_r1'};
    $summary{'#_divergent_bases_r2'} += $rg->{'metrics'}->{'#_divergent_bases_r2'};
    $summary{'#_mapped_bases_r1'} += $rg->{'metrics'}->{'#_mapped_bases_r1'};
    $summary{'#_mapped_bases_r2'} += $rg->{'metrics'}->{'#_mapped_bases_r2'};

    if($rg->{'metrics'}->{'#_mapped_bases'} == 0) {
      $donor_issues->{$norm_or_tum.' broken'} = $HC{$norm_or_tum}{broken};
      next;
    }
    my $sd_frac = $rg->{'metrics'}->{'insert_size_sd'} / $rg->{'metrics'}->{'mean_insert_size'};
    if($sd_frac > $max_insert_sd) {
      $max_insert_sd = $sd_frac;
      $high_sd_rg = $rg;
    }

    $mapped_r1 += $rg->{'metrics'}->{'#_mapped_reads_r1'};
    $mapped_r2 += $rg->{'metrics'}->{'#_mapped_reads_r2'};
    $all_r1 += $rg->{'metrics'}->{'#_total_reads_r1'};
    $all_r2 += $rg->{'metrics'}->{'#_total_reads_r2'};

  }

  push @{$aliquot_data}, sprintf('%.2f', $summary{'#_mapped_bases'}/$DIV_X);
  $donor_issues->{$norm_or_tum.' low seqX'} = $HC{$norm_or_tum}{low_seq} if($aliquot_data->[-1] < $MIN_SEQX);

  push @{$aliquot_data}, sprintf('%.2f', ($summary{'#_gc_bases_r1'} / $summary{'#_bases_r1'})*100 - $EXP_GC);
  $donor_issues->{$norm_or_tum.' r1_GC deviation'} = $HC{$norm_or_tum}{gc_r1_dev} if((abs $aliquot_data->[-1]) > $MAX_ABS_GC_DEVIATION);

  push @{$aliquot_data}, sprintf('%.2f', ($summary{'#_gc_bases_r2'} / $summary{'#_bases_r2'})*100 - $EXP_GC);
  $donor_issues->{$norm_or_tum.' r2_GC deviation'} = $HC{$norm_or_tum}{gc_r2_dev} if((abs $aliquot_data->[-1]) > $MAX_ABS_GC_DEVIATION);

  # GC distance comparing R1vsR2
  push @{$aliquot_data}, sprintf('%.2f', abs ($aliquot_data->[-1] - $aliquot_data->[-2]));
  $donor_issues->{$norm_or_tum.' GC dist'} = $HC{$norm_or_tum}{gc_dist} if((abs $aliquot_data->[-1]) > $MAX_ABS_GC_DEVIATION);

  push @{$aliquot_data}, sprintf('%.5f', $mapped_r1 / $all_r1);
  push @{$aliquot_data}, sprintf('%.5f', $mapped_r2 / $all_r2);

  $donor_issues->{$norm_or_tum.' end map dist'} = $HC{$norm_or_tum}{end_map_disc} if(1-($aliquot_data->[-1] / $aliquot_data->[-2]) > $READ_MAP_DIST_MAX);



  push @{$aliquot_data}, sprintf('%.2f', ($summary{'#_divergent_bases_r1'} / $summary{'#_mapped_bases_r1'})*100);
  push @{$aliquot_data}, sprintf('%.2f', ($summary{'#_divergent_bases_r2'} / $summary{'#_mapped_bases_r2'})*100);

  my @edits = sort {$a<=>$b} @{$aliquot_data}[-2,-1];
  if($edits[1] > $edits[0]*2) {
    $donor_issues->{$norm_or_tum.' edit dist'} = $HC{$norm_or_tum}{edit_dist};
  }

  $donor_issues->{$norm_or_tum.' isize_sd'} = $HC{$norm_or_tum}{sd_frac} if($max_insert_sd > $MAX_ISIZE_SD_FRAC);

  my ($total_dup, $total_mapped) = (0,0);
  for my $lib(@{$specimen->{'alignment'}->{'markduplicates_metrics'}}) {
    # have to make compatible with multi library so:
    $total_dup += $lib->{'metrics'}->{'read_pair_duplicates'} * 2;
    $total_dup += $lib->{'metrics'}->{'unpaired_read_duplicates'};
    $total_mapped += $lib->{'metrics'}->{'read_pairs_examined'} * 2;
    $total_mapped += $lib->{'metrics'}->{'unpaired_reads_examined'};
  }
  my $overall_dup_frac = 1
  $overall_dup_frac = $total_dup / $total_mapped if($total_mapped == 0);
  $donor_issues->{$norm_or_tum.' dup_frac'} = $HC{$norm_or_tum}{dup_frac} if($overall_dup_frac > $MAX_DUP_FRAC);

#if($max_insert_sd > $MAX_ISIZE_SD_FRAC) {
#  print Dumper($high_sd_rg->{'metrics'});
#  exit;
#}

  return $summary{'#_mapped_bases'};
}

sub process_aliquot {
  my ($donor, $specimen, $donor_issues, $norm_or_tum, $fh) = @_;
  my @aliquot_data;
  push @aliquot_data, $donor->{'donor_unique_id'};
  push @aliquot_data, $specimen->{'aliquot_id'};
  push @aliquot_data, $specimen->{'dcc_specimen_type'};

  my $total_x = 0;
  if((scalar @{$specimen->{'alignment'}->{'qc_metrics'}}) > 0) {
    $total_x = add_aliquot($specimen, \@aliquot_data, $norm_or_tum, $donor_issues);
  }
  else {
    $donor_issues->{$norm_or_tum.' qc_metric absent'} = $HC{$norm_or_tum}{no_qc};
    push @aliquot_data, (qw(.)) x 5;
  }

  if(defined $fh) {
    print $fh join("\t", @aliquot_data),"\n";
  }

  return $total_x;
}

sub process {
  my $file = shift;

  my $aliquot_fh;
#  $aliquot_fh = *STDOUT;

  my $donor_count = 0;
  my $in_fh = input_select($file);

  print join("\t", 'GNOS repo', 'GNOS Study', 'Unique DonorId', 'Normal coverage', 'Tumour coverage', 'Normalised coverage', 'Tumours', @DONOR_ORDERED_ISSUES, 'Issue Summary'),"\n";

  while (my $jsonl = <$in_fh>) {
    my $donor = decode_json $jsonl;

    next if($donor->{'is_test'});

    my %donor_issues;
    my @aliquot_data_sets;

    # process normal
    if(!exists $donor->{'normal_specimen'}->{'is_aligned'}) {
      $donor_issues{'Normal available'} = $HC{'Normal'}{none};
    }
    else {
      $donor_issues{'Normal available'} = $donor->{'normal_specimen'}->{'is_aligned'} ? 0 : $HC{'Normal'}{not_aligned};
    }

    my $norm_x = 0;
    if($donor_issues{'Normal available'} == 0) {
      $norm_x = process_aliquot($donor, $donor->{'normal_specimen'}, \%donor_issues, 'Normal', $aliquot_fh);
    }

    my $tum_x = 0;
    if($donor->{'all_tumor_specimen_aliquot_counts'} == 0) {
      $donor_issues{'Tumour available'} = $HC{'Tumour'}{none};
    }
    elsif($donor->{'aligned_tumor_specimen_aliquot_counts'} == 0
          || $donor->{'aligned_tumor_specimen_aliquot_counts'} != $donor->{'all_tumor_specimen_aliquot_counts'}) {
      $donor_issues{'Tumour available'} = $HC{'Tumour'}{not_aligned};
    }
    else {
      $donor_issues{'Tumour available'} = 0;
      for my $specimen (@{$donor->{'aligned_tumor_specimens'}}) {
        $tum_x += process_aliquot($donor, $specimen, \%donor_issues, 'Tumour', $aliquot_fh);
      }
    }

    $donor_count++;
    my @donor_data;
    my $tmp_repo = $donor->{'gnos_repo'};
    $tmp_repo =~ s|^https:/{2}||;
    $tmp_repo =~ s|^gtrepo\-||;
    $tmp_repo =~ s/\.(ucsc\.edu|annailabs\.com)\/$//;
    push @donor_data, $tmp_repo;
    push @donor_data, $donor->{'gnos_study'};
    push @donor_data, $donor->{'donor_unique_id'};

    # this reflects the combined coverage for T/N * number of T/N
    my $normalised_x = ".\t.\t.";
    if($donor_issues{'Normal available'} == 0 && $donor->{'aligned_tumor_specimen_aliquot_counts'} > 0) {
      $normalised_x = sprintf '%.2f', $norm_x / $DIV_X;
      $normalised_x .= "\t" . (sprintf '%.2f', ($tum_x  / $donor->{'aligned_tumor_specimen_aliquot_counts'}) / $DIV_X);
      $normalised_x .= "\t" . (sprintf '%.2f', ($norm_x + ($tum_x*$donor->{'aligned_tumor_specimen_aliquot_counts'})) / ($donor->{'aligned_tumor_specimen_aliquot_counts'} + 1) / $DIV_X);
    }
    push @donor_data, $normalised_x;
    push @donor_data, $donor->{'all_tumor_specimen_aliquot_counts'};

    for my $issue(@DONOR_ORDERED_ISSUES) {
      push @donor_data, exists $donor_issues{$issue} ? $donor_issues{$issue} : 0;
    }
    my $sum_issues = sum @donor_data[5..(scalar @donor_data)-1];

    print join("\t", @donor_data, $sum_issues),"\n";
  }
  close $in_fh; # this may need to be handled differently for URL based input
}

sub input_select {
  my $input = shift;
  my $fh;
  # is this a URL?
  if(index($input, '://') >= 3) {
    die "URL processing has not been implemented yet\n";
  }
  elsif($input =~ m/\.gz$/) {
    $fh = new IO::Uncompress::Gunzip $input;
  }
  else {
    # assume a file
    open $fh, '<', $input;
  }
}

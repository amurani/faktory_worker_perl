#!/usr/local/bin/perl

use strict;
use warnings;
use feature qw(say);


use lib '../';
use FaktoryWorkerPerl::Client;
use FaktoryWorkerPerl::Job;
use Time::HiRes qw< usleep >;
use Data::Dump qw< pp >;

my $client = FaktoryWorkerPerl::Client->new;
do {
    my $job = FaktoryWorkerPerl::Job->new(
        type => 'poc_job',
        args => [ int(rand(10)), int(rand(10)) ],
    );
    $client->push($job);
    say sprintf("pushing new job: %s", $job->jid);

    usleep(1_000_000);
} while (1);

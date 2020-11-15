#!/usr/local/bin/perl

use strict;
use warnings;
use feature qw(say);

use FindBin;
use lib "$FindBin::Bin/../lib";

use FaktoryWorker::Client;
use FaktoryWorker::Job;
use Time::HiRes qw< usleep >;
use Data::Dump qw< pp >;

my $client = FaktoryWorker::Client->new(
    host => "localhost",
    port => 7419,
);
do {
    my $job = FaktoryWorker::Job->new(
        type    => 'poc_job',
        args    => [ int( rand(10) ), int( rand(10) ) ],
        logging => 1,
    );
    $client->push($job);
    say sprintf( "pushing new job: %s", $job->jid );

    usleep(1_000_000);
} while (1);

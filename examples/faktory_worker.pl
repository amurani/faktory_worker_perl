#!/usr/local/bin/perl

use strict;
use warnings;

use feature qw(say);

use lib '../';

use FaktoryWorkerPerl::Client;
use FaktoryWorkerPerl::Worker;
use Data::Dump qw< pp >;

say "starting worker";

my $worker = FaktoryWorkerPerl::Worker->new(
    client => FaktoryWorkerPerl::Client->new,
    queues => [qw< critical default bulk >]
);

$worker->register(
    'poc_job',
    sub {
        my $job = shift;

        say sprintf( "running job: %s", $job->{jid} );

        my $args = $job->{args};
        my ( $a, $b ) = @$args;
        my $sum = $a + $b;

        say sprintf( "sum: %d + %d = %d", $a, $b, $sum );

        return $sum;
    }
);

$worker->run(1);

say "worker running";

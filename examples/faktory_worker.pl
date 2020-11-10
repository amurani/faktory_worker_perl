#!/usr/local/bin/perl

use strict;
use warnings;

use feature qw(say);

use FindBin;
use lib "$FindBin::Bin/../lib";

use FaktoryWorker::Client;
use FaktoryWorker::Worker;
use Data::Dump qw< pp >;

say "starting worker";

my $worker = FaktoryWorker::Worker->new(
    client => FaktoryWorker::Client->new(
        host => "localhost",
        port => 7419,
    ),
    queues  => [qw< critical default bulk >],
    logging => 1,
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

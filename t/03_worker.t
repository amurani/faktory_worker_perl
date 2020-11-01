use strict;
use warnings;
use feature qw(say);

use Test::More;

use Time::HiRes qw< usleep >;

use FindBin;
use lib "$FindBin::Bin/../lib";

require_ok('FaktoryWorkerPerl::Worker');
require_ok('FaktoryWorkerPerl::Client');
require_ok('FaktoryWorkerPerl::Job');

my $client = FaktoryWorkerPerl::Client->new( logging => 0 );
my $worker = FaktoryWorkerPerl::Worker->new(
    client => $client,
    queues => [qw< critical default bulk >],
);

my $is_do_addition_job_called = 0;
$worker->register(
    my $do_addition_job = 'do_addition_job',
    sub {
        return if $is_do_addition_job_called;
        say "worker is running addition job";
        my $job = shift;

        my $args = $job->{args};
        my ( $a, $b ) = @$args;
        my $sum = $a + $b;

        say sprintf( "sum: %d + %d = %d", $a, $b, $sum );
        $is_do_addition_job_called = 1;
    }
);

my $is_do_substraction_job_called = 0;
$worker->register(
    my $do_substraction_job = 'do_substraction_job',
    sub {
        return if $is_do_substraction_job_called;
        say "worker is running substraction job";
        my $job = shift;

        my $args = $job->{args};
        my ( $a, $b ) = @$args;
        my $sum = $a - $b;

        say sprintf( "difference: %d - %d = %d", $a, $b, $sum );
        $is_do_substraction_job_called = 1;
    }
);

ok( $worker, "worker is created okay" );

ok( $worker->client, "worker client is created okay" );

is( scalar @{ $worker->queues }, 3, "worker has 3 queues" );

is( scalar keys %{ $worker->job_types }, 2, "worker has 2 job types" );

$worker->run();

my $addition_job = FaktoryWorkerPerl::Job->new(
    type => $do_addition_job,
    args => [ int( rand(10) ), int( rand(10) ) ],
);
$client->push($addition_job);

my $substraction_job = FaktoryWorkerPerl::Job->new(
    type => $do_substraction_job,
    args => [ int( rand(10) ), int( rand(10) ) ],
);
$client->push($substraction_job);

do {
    ok( $is_do_addition_job_called,     "worker calls addition job okay" )     if $is_do_addition_job_called;
    ok( $is_do_substraction_job_called, "worker calls substraction job okay" ) if $is_do_substraction_job_called;

    if ( $is_do_addition_job_called && $is_do_substraction_job_called ) {
        $worker->stop(1);
    } else {
        say "worker is still waiting for addition job"     unless $is_do_addition_job_called;
        say "worker is still waiting for substraction job" unless $is_do_substraction_job_called;
        usleep(1_000_000);
    }
} while ( !( $is_do_addition_job_called && $is_do_substraction_job_called ) );

done_testing();

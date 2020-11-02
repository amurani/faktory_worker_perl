use feature qw(say);

use Test::Spec;
use Test::More;

use Time::HiRes qw< usleep >;
use Data::Dump qw< pp >;

use FindBin;
use lib "$FindBin::Bin/../lib";

require_ok('FaktoryWorkerPerl::Worker');
require_ok('FaktoryWorkerPerl::Client');
require_ok('FaktoryWorkerPerl::Job');

describe 'FaktoryWorkerPerl::Worker' => sub {
    my ( $client, $worker );
    my ( $do_poc_job, $do_addition_job, $do_substraction_job );

    my $is_do_poc_job_called          = 0;
    my $is_do_addition_job_called     = 0;
    my $is_do_substraction_job_called = 0;

    my @job_processors = (
        sub {
            return if $is_do_poc_job_called;
            say "worker is running poc job";
            $is_do_poc_job_called = 1;
        },
        sub {
            return if $is_do_addition_job_called;
            say "worker is running addition job";
            my $job = shift;

            my $args = $job->{args};
            my ( $a, $b ) = @$args;
            my $sum = $a + $b;

            say sprintf( "sum: %d + %d = %d", $a, $b, $sum );
            $is_do_addition_job_called = 1;
        },
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

    before all => sub {
        $client = FaktoryWorkerPerl::Client->new( logging => 0 );
        $worker = FaktoryWorkerPerl::Worker->new(
            client => $client,
            queues => [qw< critical default bulk >],
        );

        my ( $poc_job_processor, $do_addition_job_processor, $do_substraction_job_processor ) = @job_processors;

        $worker->register( $do_poc_job = 'do_poc_job', sub { $poc_job_processor->(@_) } );

        $worker->register( $do_addition_job = 'do_addition_job', sub { $do_addition_job_processor->(@_) } );

        $worker->register( $do_substraction_job = 'do_substraction_job', sub { $do_substraction_job_processor->(@_) } );
    };

    it "creates job server worker okay" => sub {

        ok( $worker, "worker is created okay" );

        ok( $worker->client, "worker client is created okay" );

        is( scalar @{ $worker->queues }, 3, "worker has 3 queues" );

        my $job_types_count = scalar @job_processors;
        is( scalar keys %{ $worker->job_types },
            $job_types_count, sprintf( "worker has %s job types", $job_types_count ) );

        is( $worker->is_running, 0, "worker is not running" );
    };

    it "processes job server client jobs okay" => sub {
        my $job_to_ack = FaktoryWorkerPerl::Job->new(
            type => $do_poc_job,
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($job_to_ack), $job_to_ack->jid, "client pushes job to ack and returns job id okay" );

        my $job_to_fail = FaktoryWorkerPerl::Job->new(
            type => $do_poc_job,
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($job_to_fail), $job_to_fail->jid, "client pushes job to fail and returns job id okay" );

        my $poc_job = FaktoryWorkerPerl::Job->new(
            type => $do_poc_job,
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($poc_job), $poc_job->jid, "client pushes job and returns job id" );

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

        my $job_json = $client->fetch( [qw< critical bulk >] );
        cmp_deeply( $job_json, {}, "client fetches no jobs for queues with no jobs" );

        do {
            # this blocks the code after it TODO: @amurani pls fix
            $worker->run( my $daemonize = 1 ) unless $worker->is_running;

            if ( $is_do_poc_job_called && $is_do_addition_job_called && $is_do_substraction_job_called ) {
                ok( $is_do_poc_job_called,          "worker calls poc job okay" );
                ok( $is_do_addition_job_called,     "worker calls addition job okay" );
                ok( $is_do_substraction_job_called, "worker calls substraction job okay" );

                $worker->stop(1);
            } else {
                say "worker is still waiting for poc job"          unless $is_do_poc_job_called;
                say "worker is still waiting for addition job"     unless $is_do_addition_job_called;
                say "worker is still waiting for substraction job" unless $is_do_substraction_job_called;

                my $job_json = $client->fetch();
                my $job_to_compare;
                if ( $job_json->{jid} eq $job_to_ack->jid ) {
                    ok( $client->ack( $job_to_ack->jid ), "client acks job to ack okay" );
                } elsif ( $job_json->{jid} eq $job_to_fail->jid ) {
                    ok( $client->fail( $job_to_fail->jid, "Rejecting job for test" ),
                        "client sends failures for job to fail okay" );
                } elsif ( $job_json->{jid} eq $addition_job->jid ) {
                    $job_to_compare = $addition_job;
                } elsif ( $job_json->{jid} eq $substraction_job->jid ) {
                    $job_to_compare = $substraction_job;
                }

                if ($job_to_compare) {
                    cmp_deeply(
                        $job_json,
                        {
                            args        => $job_to_compare->args,
                            jid         => $job_to_compare->jid,
                            jobtype     => $job_to_compare->type,
                            created_at  => ignore(),
                            enqueued_at => ignore(),
                            queue       => ignore(),
                            retry       => ignore(),
                        },
                        "client serializes job to json okay"
                    );
                    ok( $client->ack( $job_to_compare->jid ), "client acks job to compare okay" );
                }

                usleep(1_000_000);
            }
        } while ( !( $is_do_addition_job_called && $is_do_substraction_job_called ) );

    };

};

runtests unless caller;

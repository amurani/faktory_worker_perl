use feature qw(say);

use Test::Spec;
use Test::More;
use Test::Warn;

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
        $client = FaktoryWorkerPerl::Client->new;
        $worker = FaktoryWorkerPerl::Worker->new(
            client => $client,
            queues => [qw< critical default bulk >],
        );

        my ( $poc_job_processor, $do_addition_job_processor, $do_substraction_job_processor ) = @job_processors;

        is( $worker->register( $do_poc_job = 'do_poc_job', sub { $poc_job_processor->(@_) } ),
            1, 'do_poc_job registered okay' );

        is( $worker->register( $do_addition_job = 'do_addition_job', sub { $do_addition_job_processor->(@_) } ),
            1, 'do_addition_job registered ok' );

        is(
            $worker->register(
                $do_substraction_job = 'do_substraction_job',
                sub { $do_substraction_job_processor->(@_) }
            ),
            1,
            'do_substraction_job registered okay'
        );

        my ( $no_name_woker, $no_processor_worker );
        warning_is {
            $no_name_woker = $worker->register( undef, sub { } )
        }
        "An job processor cannot be registered without a job type";
        warnings_are { $no_processor_worker = $worker->register( 'fake_job', undef ) }
        [ "A job processor cannot be undefined", "A job processor must be a runnable subroutine" ];

        is( $no_name_woker,       0, 'fails to register job worker without name okay' );
        is( $no_processor_worker, 0, 'fails to register job worker with no processor okay ' );
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
        my $poc_job = FaktoryWorkerPerl::Job->new(
            jobtype => $do_poc_job,
            args    => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($poc_job), $poc_job->jid, "client pushes do_poc_job job and returns job id okay" );

        my $addition_job = FaktoryWorkerPerl::Job->new(
            jobtype => $do_addition_job,
            args    => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($addition_job),
            $addition_job->jid, "client pushes do_addition_job job and returns job id okay" );

        my $substraction_job = FaktoryWorkerPerl::Job->new(
            jobtype => $do_substraction_job,
            args    => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($substraction_job),
            $substraction_job->jid, "client pushes do_substraction_job job and returns job id okay" );

        my $are_all_jobs_processed = 0;
        do {
            if ($are_all_jobs_processed) {
                ok( $is_do_poc_job_called,          "worker calls poc job okay" );
                ok( $is_do_addition_job_called,     "worker calls addition job okay" );
                ok( $is_do_substraction_job_called, "worker calls substraction job okay" );

                last;
            } else {
                say "worker is still waiting for poc job"          unless $is_do_poc_job_called;
                say "worker is still waiting for addition job"     unless $is_do_addition_job_called;
                say "worker is still waiting for substraction job" unless $is_do_substraction_job_called;

                $worker->run();

                usleep(1_000_000);
            }

            $are_all_jobs_processed =
                $is_do_poc_job_called && $is_do_addition_job_called && $is_do_substraction_job_called;
        } while ( !$are_all_jobs_processed );

    };

    it "processes job server client ack/fail jobs okay" => sub {

        my $job_to_ack = FaktoryWorkerPerl::Job->new(
            jobtype => $do_poc_job,
            args    => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($job_to_ack), $job_to_ack->jid, "client pushes job to ack and returns job id okay" );

        my $job_to_fail = FaktoryWorkerPerl::Job->new(
            jobtype => $do_poc_job,
            args    => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($job_to_fail), $job_to_fail->jid, "client pushes job to fail and returns job id okay" );

        cmp_deeply( $client->fetch( [qw< critical bulk >] ), undef, "client fetches no jobs for queues with no jobs" );

        my $are_all_jobs_processed = 0;
        my ( $is_job_to_ack_processed, $is_job_to_fail_processed );
        do {
            if ($are_all_jobs_processed) {
                ok( $are_all_jobs_processed, "jobs acked/failed okay" );

                last;
            } else {
                say "worker is still waiting ack/fail jobs";

                my $job = $client->fetch();    # get jobs from default queue
                if ($job) {
                    my $job_to_compare;
                    my $job_name;

                    if ( $job->jid eq $job_to_ack->jid ) {
                        ok( $client->ack( $job_to_ack->jid ), "client acks job to ack okay" );
                        $job_to_compare = $job_to_ack;
                        $job_name       = 'job_to_ack';
                    } elsif ( $job->jid eq $job_to_fail->jid ) {
                        ok( $client->fail( $job_to_fail->jid, "Test Exception", "Rejecting job for test", [] ),
                            "client sends failures for job to fail okay" );
                        $job_to_compare = $job_to_fail;
                        $job_name       = 'job_to_fail';
                    }

                    if ($job_to_compare) {
                        my $job_json = $job_to_compare->json_serialized;
                        cmp_deeply(
                            $job->json_serialized,
                            {
                                args        => $job_json->{args},
                                at          => ignore(),
                                backtrace   => 0,
                                created_at  => ignore(),
                                custom      => {},
                                enqueued_at => ignore(),
                                failure     => ignore(),
                                jid         => $job_json->{jid},
                                jobtype     => $job_json->{jobtype},
                                queue       => 'default',
                                retry       => 25,
                            },
                            sprintf( "client serializes %s job to json okay", $job_name )
                        );
                        $is_job_to_ack_processed  = 1 if $job_name eq 'job_to_ack';
                        $is_job_to_fail_processed = 1 if $job_name eq 'job_to_fail';
                    }
                }

                usleep(1_000_000);
            }

            $are_all_jobs_processed = $is_job_to_ack_processed && $is_job_to_fail_processed;
        } while ( !$are_all_jobs_processed );
    };

};

runtests unless caller;

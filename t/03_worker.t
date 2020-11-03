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

        is( $worker->register( undef,      sub { } ), 0, 'fails to register job worker without name okay' );
        is( $worker->register( 'fake_job', undef ),   0, 'fails to register job worker with no processor okay ' );
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
            type => $do_poc_job,
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($poc_job), $poc_job->jid, "client pushes do_poc_job job and returns job id okay" );

        my $addition_job = FaktoryWorkerPerl::Job->new(
            type => $do_addition_job,
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($addition_job),
            $addition_job->jid, "client pushes do_addition_job job and returns job id okay" );

        my $substraction_job = FaktoryWorkerPerl::Job->new(
            type => $do_substraction_job,
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($substraction_job),
            $substraction_job->jid, "client pushes do_substraction_job job and returns job id okay" );

        my $job_json = $client->fetch( [qw< critical bulk >] );
        cmp_deeply( $job_json, {}, "client fetches no jobs for queues with no jobs" );

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

                my $job_json = $client->fetch();
                my $job_to_compare;

=begin
                if ( $job_json->{jid} eq $job_to_ack->jid ) {
                    ok( $client->ack( $job_to_ack->jid ), "client acks job to ack okay" );
                } elsif ( $job_json->{jid} eq $job_to_fail->jid ) {
                    ok( $client->fail( $job_to_fail->jid, "Rejecting job for test" ),
                        "client sends failures for job to fail okay" );
                }
=cut

                if ( $job_json->{jid} && $job_json->{jid} eq $addition_job->jid ) {
                    $job_to_compare = $addition_job;
                } elsif ( $job_json->{jid} && $job_json->{jid} eq $substraction_job->jid ) {
                    $job_to_compare = $substraction_job;
                } elsif ( $job_json->{jid} && $job_json->{jid} eq $poc_job->jid ) {
                    $job_to_compare = $poc_job;
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
                        sprintf( "client serializes %s job to json okay", $job_to_compare->type )
                    );
                }

                usleep(1_000_000);
            }

            $are_all_jobs_processed =
                $is_do_poc_job_called && $is_do_addition_job_called && $is_do_substraction_job_called;
        } while ( !$are_all_jobs_processed );

    };

=begin
    it "processes job server client ack/fail jobs okay" => sub {

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
    };
=cut

};

runtests unless caller;

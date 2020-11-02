use Test::Spec;
use Test::More;
use Test::Deep;

use Data::Dump qw< pp >;

use FindBin;
use lib "$FindBin::Bin/../lib";

describe 'FaktoryWorkerPerl::Client' => sub {

    it "package(s) required ok" => sub {
        require_ok('FaktoryWorkerPerl::Client');
        require_ok('FaktoryWorkerPerl::Job');
    };

    my $client;

    it "creates job server client okay" => sub {
        $client = FaktoryWorkerPerl::Client->new( logging => 0 );
        ok( $client, "client is created okay" );

        is( $client->port, 7419, "client port is 7419" );

        is( $client->host, 'localhost', "client host is localhost" );

        my $connection = $client->connect();
        ok( $connection,                      "client connects to job server okay" );
        ok( $client->disconnect($connection), "client disconnects from job server okay" );
        ok( $client->beat(),                  "client sends beat okay" );
    };

    it "handles job server client jobs okay" => sub {

        my $job = FaktoryWorkerPerl::Job->new(
            type => 'poc_job',
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($job), $job->jid, "client pushes job and returns job id okay" );

        my $job_json = $client->fetch();
        cmp_deeply(
            $job_json,
            {
                args        => $job->args,
                jid         => $job->jid,
                jobtype     => $job->type,
                created_at  => ignore(),
                enqueued_at => ignore(),
                queue       => ignore(),
                retry       => ignore(),
            },
            "json serialization of job is okay "
        );

        my $job_to_ack = FaktoryWorkerPerl::Job->new(
            type => 'poc_job',
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($job_to_ack), $job_to_ack->jid, "client pushes job to ack and returns job id okay" );
        ok( $client->ack( $job_to_ack->jid ), "client acks job to ack okay" );

        my $job_to_fail = FaktoryWorkerPerl::Job->new(
            type => 'poc_job',
            args => [ int( rand(10) ), int( rand(10) ) ],
        );
        is( $client->push($job_to_ack), $job_to_ack->jid, "client pushes job to ack and returns job id okay" );
        ok( $client->fail( $job_to_ack->jid, "Rejecting job for test" ), "client sends failures for job to fail okay" );
    };

};

runtests unless caller;

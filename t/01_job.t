use Test::Spec;
use Test::More;

use FindBin;
use lib "$FindBin::Bin/../lib";

describe 'FaktoryWorker::Job' => sub {

    it "package(s) required ok" => sub {
        require_ok('FaktoryWorker::Job');
    };

    it "creates job server job okay", sub {

        my $job = FaktoryWorker::Job->new(
            jobtype => 'poc_job',
            args    => [ int( rand(10) ), int( rand(10) ) ],
        );

        ok( $job, "job is created okay" );

        is( $job->jobtype, 'poc_job', "job type is named okay" );

        is( scalar @{ $job->args }, 2, "job has 2 arguments" );
    };

};

runtests unless caller;

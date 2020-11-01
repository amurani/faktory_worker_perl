use strict;
use warnings;

use Test::More;

use FindBin;
use lib "$FindBin::Bin/../lib";

require_ok('FaktoryWorkerPerl::Job');

my $job = FaktoryWorkerPerl::Job->new(
    type => 'poc_job',
    args => [ int( rand(10) ), int( rand(10) ) ],
);

ok( $job, "job is created okay" );

is( $job->type, 'poc_job', "job type is named okay" );

is( scalar @{ $job->args }, 2, "job has 2 arguments" );

done_testing();

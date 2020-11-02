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
        is( $client->port, 7419,        "client port is 7419" );
        is( $client->host, 'localhost', "client host is localhost" );
    };

    it "job server client operations works okay" => sub {
        my $connection = $client->connect();
        ok( $connection,                      "client connects to job server okay" );
        ok( $client->disconnect($connection), "client disconnects from job server okay" );

        ok( $client->beat(), "client sends beat okay" );
    };

};

runtests unless caller;

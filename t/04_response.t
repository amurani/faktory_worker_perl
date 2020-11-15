use Test::Spec;
use Test::More;

use FindBin;
use lib "$FindBin::Bin/../lib";

use FaktoryWorker::Types::Constants qw< :ResponseType >;

use Data::Dump qw< pp >;

describe 'FaktoryWorker::Response' => sub {

    it "package(s) required ok" => sub {
        require_ok('FaktoryWorker::Response');
        require_ok('FaktoryWorker::Types::Constants');
    };

    it "creates job server response object okay", sub {
        my ( $raw_response, $response );

        $raw_response = "+OK\r\n";
        $response     = FaktoryWorker::Response->new( raw_response => $raw_response );
        cmp_deeply(
            {
                type    => $response->type,
                message => $response->message,
                data    => $response->data,
            },
            {
                type    => OK,
                message => undef,
                data    => undef
            },
            "OK response is serialized to object okay"
        );

        $raw_response = "\$-1\r\n";
        $response     = FaktoryWorker::Response->new( raw_response => $raw_response );
        cmp_deeply(
            {
                type    => $response->type,
                message => $response->message,
                data    => $response->data,
            },
            {
                type    => NO_JOBS,
                message => undef,
                data    => undef
            },
            "NO JOBS response is serialized to object okay"
        );

        $raw_response = "{\"state\":\"quiet\"}";
        $response     = FaktoryWorker::Response->new( raw_response => $raw_response );
        cmp_deeply(
            {
                type    => $response->type,
                message => $response->message,
                data    => $response->data,
            },
            {
                type    => undef,
                message => undef,
                data    => { state => "quiet" }
            },
            "BEAT state response is serialized to object okay"
        );

        $raw_response = "-ERR Invalid password\r\n";
        $response     = FaktoryWorker::Response->new( raw_response => $raw_response );
        cmp_deeply(
            {
                type    => $response->type,
                message => $response->message,
                data    => $response->data,
            },
            {
                type    => ERROR,
                message => "Invalid password\r",
                data    => undef
            },
            "ERR response is serialized to object okay"
        );

        $raw_response = "+HI {\"v\":2,\"i\":7365,\"s\":\"33415dbd315ae6af\"}\r\n";
        $response     = FaktoryWorker::Response->new( raw_response => $raw_response );
        cmp_deeply(
            {
                type    => $response->type,
                message => $response->message,
                data    => $response->data,
            },
            {
                type    => HI,
                message => undef,
                data    => {
                    v => 2,
                    i => 7365,
                    s => "33415dbd315ae6af"

                }
            },
            "HI response is serialized to object okay"
        );

    };

};

runtests unless caller;

package FaktoryWorkerPerl::Client;
use Moose;
use feature qw(signatures say);
no warnings qw(experimental::signatures);
use IO::Socket::INET;
use JSON;
use Data::Dump qw< pp >;

use constant HOST => 'localhost';
use constant PORT => '7419';

has host => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
    default  => sub { HOST },
);

has port => (
    is       => 'rw',
    isa      => 'Int',
    required => 0,
    default  => sub { PORT },
);

has logging => (
    is       => 'rw',
    isa      => 'Bool',
    required => 0,
    default  => sub { 0 },
);

use constant HELLO => 'HELLO';
use constant PUSH  => 'PUSH';
use constant ACK   => 'ACK';
use constant FAIL  => 'FAIL';
use constant FETCH => 'FETCH';
use constant BEAT  => 'BEAT';

use constant OK      => "+OK\r\n";
use constant NO_JOBS => "\$-1\r\n";

sub fetch ( $self, $queues = [qw<default>] ) {
    my $client_socket = $self->connect();
    my $response      = $self->send( $client_socket, $self->FETCH, join( " ", @$queues ) );
    say sprintf( "send fetch: %s", $response ) if $self->logging;

    my $data;
    if ( $response eq $self->NO_JOBS || $response eq $self->OK ) {
        $data = "{}";
    } else {
        $data = $self->recv($client_socket);
        say sprintf( "recv fetch: %s", $data ) if $self->logging;
    }
    $self->disconnect($client_socket);

    return decode_json($data);
}

sub push ( $self, $job ) {
    my $client_socket = $self->connect();
    my $response      = $self->send( $client_socket, $self->PUSH, encode_json( $job->json_serialized ) );
    say sprintf( "send push: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
}

sub ack ( $self, $job_id ) {
    my $client_socket = $self->connect();
    my $response      = $self->send( $client_socket, $self->ACK, encode_json( { jid => $job_id } ) );
    say sprintf( "send ack: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
}

sub fail ( $self, $job_id ) {
    my $client_socket = $self->connect();
    my $response      = $self->send( $client_socket, $self->FAIL, encode_json( { jid => $job_id } ) );
    say sprintf( "send fail: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
}

sub beat($self) {
    my $client_socket = $self->connect();
    my $response      = $self->send( $client_socket, $self->BEAT, encode_json( { wid => "2" } ) );
    say sprintf( "send beat: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
}

sub connect($self) {
    my $client_socket = IO::Socket::INET->new(
        PeerAddr => $self->host,
        PeerPort => $self->port,
        Proto    => 'tcp',
    ) or die sprintf( "cannot connect to %s:%s", $self->host, $self->port );

    eval {
        my $data = $self->recv($client_socket);
        die "Hi not received :( $data" unless ( $data eq "+HI {\"v\":2}\r\n" );

        my $response = $self->send( $client_socket, $self->HELLO, encode_json( { wid => "2" } ) );
        die "Hi did not get sent :( $response" unless ( $response ne "+OK" );
    } or do {
        my $error = $@;
        die "We have issues man: $error";
    };

    return $client_socket;
}

sub disconnect ( $self, $client_socket ) {
    $client_socket->close();
}

sub recv ( $self, $client_socket ) {
    my $data;

    eval {
        $data = <$client_socket>;
        say sprintf( "recv: %s", $data ) if $self->logging;

        1;
    } or do {
        my $error = $@;
        die "recv failed with : $error";
    };

    return $data;
}

sub send ( $self, $client_socket, $command, $data ) {
    my $response;

    eval {
        my $payload = sprintf( "%s %s\r\n", $command, $data );
        say sprintf( "send payload: %s", $payload ) if $self->logging;
        print $client_socket $payload;
        $response = $self->recv($client_socket);
        say sprintf( "send response: %s", $response ) if $self->logging;

        1;
    } or do {
        my $error = $@;
        die "send failed with : $error";
    };

    return $response;
}

__PACKAGE__->meta->make_immutable;
1;

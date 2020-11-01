package FaktoryWorkerPerl::Client;
use Moose;
use feature qw(signatures say);
no warnings qw(experimental::signatures);
use IO::Socket::INET;
use JSON;
use Data::GUID;
use Data::Dump qw< pp >;

use constant HOST             => 'localhost';
use constant PORT             => '7419';
use constant PROTOCOL_VERSION => 2;
has host                      => (
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

has protocol_version => (
    is       => 'ro',
    isa      => 'Int',
    required => 0,
    default  => sub { PROTOCOL_VERSION },
);

# a unique worker id
has wid => (
    is      => 'ro',
    isa     => 'Str',
    default => sub { Data::GUID->new->as_string },
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

use constant HI      => "+HI";
use constant OK      => "+OK\r\n";
use constant NO_JOBS => "\$-1\r\n";

=item fetch()

Fetch jobs from the faktory job server in a list of queues
Defaults to 'default' if no list is provided

=cut

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

=item push()

Push a job to the faktory worker
Returns the job id once pushed
=cut

sub push ( $self, $job ) {
    my $client_socket = $self->connect();
    my $response      = $self->send( $client_socket, $self->PUSH, encode_json( $job->json_serialized ) );
    say sprintf( "send push: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
    return $job->jid;
}

=item ack()

Sends an acknowledgement that a job has been processed successfully
=cut

sub ack ( $self, $job_id ) {
    my $client_socket = $self->connect();
    my $response      = $self->send( $client_socket, $self->ACK, encode_json( { jid => $job_id } ) );
    say sprintf( "send ack: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
    return $response eq $self->OK;
}

=item fail()

Sends failure information that a job has  not been processed successfully
=cut

sub fail ( $self, $job_id, $error_message ) {
    my $client_socket = $self->connect();
    my $response =
        $self->send( $client_socket, $self->FAIL, encode_json( { jid => $job_id, message => $error_message } ) );
    say sprintf( "send fail: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
    return $response eq $self->OK;
}

=item beat()

Sends a BEAT as required for proof of liveness

=cut

sub beat($self) {
    my $client_socket = $self->connect();
    my $response = $self->send( $client_socket, $self->BEAT, encode_json( { wid => "" . $self->protocol_version } ) );
    say sprintf( "send beat: %s", $response ) if $self->logging;
    $self->disconnect($client_socket);
    return $response eq $self->OK;
}

=item connect()

Opens network connection to faktory job server

=cut

sub connect($self) {
    my $client_socket = IO::Socket::INET->new(
        PeerAddr => $self->host,
        PeerPort => $self->port,
        Proto    => 'tcp',
    ) or die sprintf( "cannot connect to %s:%s", $self->host, $self->port );

    eval {
        my $data = $self->recv($client_socket);
        my $expected_handshake_reponse =
            sprintf( "%s %s\r\n", $self->HI, encode_json( { v => $self->protocol_version } ) );
        die "Handshake: HI not received :( $data"
            unless ( $data eq $expected_handshake_reponse );

        my $response = $self->send( $client_socket, $self->HELLO, encode_json( { v => $self->protocol_version } ) );
        die sprintf( "Handshake: HI did not get sent :( %s", $response || '!! NO RESPNSE RECIEVED!!' )
            unless ( $response eq $self->OK );
    } or do {
        my $error = $@;
        die "We have issues man: $error";
    };

    return $client_socket;
}

=item disconnect()

Closes network connection to faktory job server

=cut

sub disconnect ( $self, $client_socket ) {
    return $client_socket->close();
}

=item recv()

Reads response from faktory job server

=cut

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

=item send()

Sends payload to faktory job server

=cut

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

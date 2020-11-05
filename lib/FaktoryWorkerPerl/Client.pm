package FaktoryWorkerPerl::Client;

=pod

=head1 FaktoryWorkerPerl::Client

Client that handles all communication with the Faktory job server and handles job interactions

=cut

use Moose;
use feature qw(signatures);
no warnings qw(experimental::signatures);
use IO::Socket::INET;
use JSON;
use Data::GUID;
use Sys::Hostname;
use Linux::Pid qw< getpid >;
use Data::Dump qw< pp >;

with 'FaktoryWorkerPerl::Roles::Logger';

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

use constant HELLO => 'HELLO';
use constant PUSH  => 'PUSH';
use constant ACK   => 'ACK';
use constant FAIL  => 'FAIL';
use constant FETCH => 'FETCH';
use constant BEAT  => 'BEAT';

use constant HI      => "+HI";
use constant OK      => "+OK\r\n";
use constant NO_JOBS => "\$-1\r\n";

=over

=item fetch()

Sends a FETCH to request a job from the Faktory job server in a list of queues
Defaults to 'default' if no list is provided

=cut

sub fetch ( $self, $queues = [qw<default>] ) {
    my $client_socket = $self->_connect();
    my $response      = $self->send( $client_socket, $self->FETCH, join( " ", @$queues ) );
    $self->logger->info( sprintf( "%s: %s", $self->FETCH, pp $response ) );

    my $data;
    if ( $response eq $self->NO_JOBS || $response eq $self->OK ) {
        $data = "{}";
        $self->logger->info( sprintf("$self->FETCH returned no job") );
    } else {
        $data = $self->recv($client_socket);
        $self->logger->info( sprintf( "recv fetch: %s", pp $data ) );
    }
    $self->_disconnect($client_socket);

    return decode_json($data);
}

=item push()

Sends a PUSH of a job to the Faktory worker
Returns the job id once pushed
=cut

sub push ( $self, $job ) {
    my $client_socket = $self->_connect();
    my $job_payload   = $job->json_serialized;
    my $response      = $self->send( $client_socket, $self->PUSH, encode_json($job_payload) );
    $self->logger->info( sprintf( "$self->PUSH: %s", pp $response ) );
    $self->_disconnect($client_socket);
    return $job->jid;
}

=item ack()

Sends an ACK when a job has been processed successfully
=cut

sub ack ( $self, $job_id ) {
    my $client_socket = $self->_connect();
    my $ack_payload   = { jid => $job_id };
    my $response      = $self->send( $client_socket, $self->ACK, encode_json($ack_payload) );
    $self->logger->info( sprintf( "$self->ACK: %s", pp $response ) );
    $self->_disconnect($client_socket);
    return $response eq $self->OK;
}

=item fail()

Sends a FAIL when a job has  not been processed successfully
=cut

sub fail ( $self, $job_id, $error_type, $error_message, $backtrace ) {
    my $client_socket = $self->_connect();
    my $fail_payload  = { jid => $job_id, errtype => $error_type, message => $error_message, backtrace => $backtrace };
    my $response      = $self->send( $client_socket, $self->FAIL, encode_json($fail_payload) );
    $self->logger->info( sprintf( "$self->FAIL: %s", pp $response ) );
    $self->_disconnect($client_socket);
    return $response eq $self->OK;
}

=item beat()

Sends a BEAT as required for proof of liveness

=cut

sub beat($self) {
    my $client_socket = $self->_connect();
    my $beat_payload  = { wid => $self->wid };
    my $response      = $self->send( $client_socket, $self->BEAT, encode_json($beat_payload) );
    $self->logger->info( sprintf( "$self->BEAT: %s", pp $response ) );
    $self->_disconnect($client_socket);

    if ( $response =~ m/^{.*$/ ) {
        return decode_json($response)->{state};
    } else {
        return $response eq $self->OK;
    }
}

=item recv()

Reads response from Faktory job server

=cut

sub recv ( $self, $client_socket ) {
    my $data;

    eval {
        $data = <$client_socket>;
        $self->logger->info( sprintf( "recv: %s", pp $data ) );

        1;
    } or do {
        my $error = $@;
        die "recv failed with : $error";
    };

    return $data;
}

=item send()

Sends payload to Faktory job server

=cut

sub send ( $self, $client_socket, $command, $data ) {
    my $response;

    eval {
        my $payload = sprintf( "%s %s\r\n", $command, $data );
        $self->logger->info( sprintf( "send payload: %s", pp $payload ) );
        print $client_socket $payload;
        $response = $self->recv($client_socket);
        $self->logger->info( sprintf( "send response: %s", pp $response ) );

        1;
    } or do {
        my $error = $@;
        die "send failed with : $error";
    };

    return $response;
}

=item _connect()

Opens TCP network connection to Faktory job server.
Returns instance of socket connection

=cut

sub _connect($self) {
    my $client_socket = IO::Socket::INET->new(
        PeerAddr => $self->host,
        PeerPort => $self->port,
        Proto    => 'tcp',
    ) or die sprintf( "cannot connect to %s:%s", $self->host, $self->port );

    eval {
        my $data = $self->recv($client_socket);

        my $handshake_payload          = { v => $self->protocol_version };
        my $expected_handshake_reponse = sprintf( "%s %s\r\n", $self->HI, encode_json($handshake_payload) );
        die "Handshake: HI not received :( $data"
            unless ( $data eq $expected_handshake_reponse );
        my $hello_payload = {
            v        => $self->protocol_version,
            wid      => $self->wid,
            hostname => hostname,
            pid      => getpid,
            labels   => [qw< perl >],
        };
        my $response = $self->send( $client_socket, $self->HELLO, encode_json($hello_payload) );
        die sprintf( "Handshake: HI did not get sent :( %s", $response || '!! NO RESPNSE RECIEVED!!' )
            unless ( $response eq $self->OK );
    } or do {
        my $error = $@;
        die "Error connecting to Faktory job server: $error";
    };

    return $client_socket;
}

=item _disconnect()

Closes TCP network connection to Faktory job server

=cut

sub _disconnect ( $self, $client_socket ) {
    return $client_socket->close();
}

__PACKAGE__->meta->make_immutable;
1;

=back

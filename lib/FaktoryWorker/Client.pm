package FaktoryWorker::Client;

=pod

=head1 NAME

C<FaktoryWorker::Client> - handles all communication with the Faktory job server

=head1 SYNOPSIS

    use FaktoryWorker::Client;
    use FaktoryWorker::Job;

    # send heartbeat to Faktory job server
    $client->beat();

    # push job to Faktory job server
    my $job = FaktoryWorker::Job->new(
        type    => 'poc_job',
        args    => [ int( rand(10) ), int( rand(10) ) ],
        logging => 1,
    );
    $client->push($job);

=head1 DESCRIPTION

C<FaktoryWorker::Client> represents a client that handles all communication with the Faktory job server and handles job interactions

Please see L<job payload options|https://github.com/contribsys/faktory/wiki/The-Job-Payload#options> and L<job oayload metadata|https://github.com/contribsys/faktory/wiki/The-Job-Payload#options> for more details on attributes.

=head1 METHODS

=cut

use FindBin;
use lib "$FindBin::Bin/lib";

use Moose;
use namespace::autoclean;
use feature qw(signatures);
no warnings qw(experimental::signatures);
use IO::Socket::INET;
use JSON;
use Data::GUID;
use Digest::SHA qw< sha256 >;
use Sys::Hostname;
use Linux::Pid qw< getpid >;
use Data::Dump qw< pp >;
use FaktoryWorker::Job;
use FaktoryWorker::Response;
use FaktoryWorker::Types::Constants qw< :RequestCommand :ResponseType >;
with 'FaktoryWorker::Roles::Logger';

use constant HOST             => $ENV{FAKTORY_HOST};
use constant PORT             => $ENV{FAKTORY_PORT};
use constant PASSWORD         => $ENV{FAKTORY_PASSWORD};
use constant PROTOCOL_VERSION => 2;

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

has password => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
    default  => sub { PASSWORD },
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

has labels => (
    is      => 'rw',
    isa     => 'ArrayRef[Str]',
    default => sub { [qw<perl>] },
);

=over

=item C<fetch($queues)>

Sends a FETCH to request a job from the Faktory job server in a list of queues
Defaults to 'default' if no list is provided

Returns an instance of C<FaktoryWorker::Job> on success

Takes an array ref of job queues as an argument

=cut

sub fetch ( $self, $queues = [qw<default>] ) {
    my $client_socket = $self->_connect();
    my $response      = $self->send( $client_socket, FETCH, join( " ", @$queues ) );

    my $job;
    if ( $response->has_no_jobs || $response->is_okay ) {
        $self->logger->info( sprintf("${\FETCH} returned no job") );
    } else {
        my $data = $self->recv($client_socket);
        $self->logger->info( sprintf( "recv fetch: %s", pp $data ) );
        $job = FaktoryWorker::Job->new( %{ decode_json($data) } );
    }
    $self->_disconnect($client_socket);

    return $job;
}

=item C<push($job)>

Sends a PUSH of a job to the Faktory worker
Returns the job id once pushed

Take an instance of C<FaktoryWorker::Job> as an argument
=cut

sub push ( $self, $job ) {
    my $client_socket = $self->_connect();
    my $job_payload   = $job->json_serialized;
    my $response      = $self->send( $client_socket, PUSH, encode_json($job_payload) );
    $self->_disconnect($client_socket);

    if ( $response->is_okay ) {
        return $job->jid;
    } else {
        warn sprintf( "Failed to send job to Faktory job server. Job id: %s", $job->jid );
        return undef;
    }
}

=item C<ack()>

Sends an ACK when a job has been processed successfully
=cut

sub ack ( $self, $job_id ) {
    my $client_socket = $self->_connect();
    my $ack_payload   = { jid => $job_id };
    my $response      = $self->send( $client_socket, ACK, encode_json($ack_payload) );
    $self->_disconnect($client_socket);

    return $response->is_okay;
}

=item C<fail()>

Sends a FAIL when a job has  not been processed successfully
=cut

sub fail ( $self, $job_id, $error_type, $error_message, $backtrace ) {
    my $client_socket = $self->_connect();
    my $fail_payload  = {
        jid       => $job_id,
        errtype   => $error_type,
        message   => $error_message,
        backtrace => $backtrace
    };
    my $response = $self->send( $client_socket, FAIL, encode_json($fail_payload) );
    $self->_disconnect($client_socket);

    return $response->is_okay;
}

=item C<beat()>

Sends a BEAT as required for proof of liveness

=cut

sub beat($self) {
    my $client_socket = $self->_connect();
    my $beat_payload  = { wid => $self->wid };
    my $response      = $self->send( $client_socket, BEAT, encode_json($beat_payload) );
    $self->_disconnect($client_socket);

    if ( $response->data ) {
        return $response->data->{state};
    } else {
        return $response->is_okay;
    }
}

=item C<recv()>

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

=item C<send($client_socket, $command, $data)>

Sends payload to Faktory job server
Returns Faktory job server response as an instance of FaktoryWorker::Response

Take the current TCP socket connection to the Faktory server, a command and an encoded json payload as arguments
=cut

sub send ( $self, $client_socket, $command, $data ) {
    my $response;

    eval {
        my $payload = sprintf( "%s %s\r\n", $command, $data );
        $self->logger->info( sprintf( "$command [request]: %s", pp $payload ) );
        print $client_socket $payload;
        my $raw_response = $self->recv($client_socket);
        $self->logger->info( sprintf( "$command [response]: %s", pp $raw_response ) );

        $response = FaktoryWorker::Response->new( raw_response => $raw_response );

        1;
    } or do {
        my $error = $@;
        die "send failed with : $error";
    };

    return $response;
}

=item C<_connect()>

Opens TCP network connection to Faktory job server.
Returns instance of socket connection

=cut

sub _connect($self) {
    my $client_socket;

    eval {
        $client_socket = IO::Socket::INET->new(
            PeerAddr => $self->host,
            PeerPort => $self->port,
            Proto    => 'tcp',
        ) or die sprintf( "Failed to establish connection on %s:%s", $self->host, $self->port );
        my $handshake_response = $self->recv($client_socket);
        $self->logger->info( sprintf( "${\HI} [response]: %s", pp $handshake_response ) );

        my $response = FaktoryWorker::Response->new( raw_response => $handshake_response );
        die sprintf( "Handshake: ${\HI} failed due to: %s", $response->message || "!! NO MESSAGE RECIEVED!!" )
            unless $response->is_handshake;

        my $handshake_payload          = $response->data;
        my $is_authentication_required = exists $handshake_payload->{s} && exists $handshake_payload->{i};

        die sprintf("Handshake: ${\HELLO} requires a password")
            if $is_authentication_required && !$self->password;

        my $hello_payload = {
            hostname => hostname,
            pid      => getpid,
            v        => $self->protocol_version,
            wid      => $self->wid,
            scalar @{ $self->labels } ? ( labels => $self->labels ) : (),
            $is_authentication_required
            ? ( pwdhash => $self->_generate_password_hash(
                    $handshake_payload->{s},
                    $handshake_payload->{i}
                )
                )
            : (),
        };
        $response = $self->send( $client_socket, HELLO, encode_json($hello_payload) );
        die sprintf( "Handshake: ${\HELLO} failed due to: %s", $response->message || '!! NO RESPNSE RECIEVED!!' )
            unless $response->is_okay;

        1;
    } or do {
        my $error = $@;
        die sprintf( "Error connecting to Faktory job server due to: %s", $error );
    };

    return $client_socket;
}

=item C<_disconnect($client_socket)>

Closes TCP network connection to Faktory job server

Take the current TCP socket connection to the Faktory job server object as an argument

=cut

sub _disconnect ( $self, $client_socket ) {
    return $client_socket->close();
}

=item C<_generate_password_hash($salt, $iterations)>

Calculates the password hash needed for authentication on the Faktory job server

Takes the salt and iterations values from the Faktory job server initial handshake as arguments

=cut

sub _generate_password_hash ( $self, $salt, $iterations ) {
    my $password_hash = Digest::SHA->new(256)->add( sprintf( "%s%s", $self->password, $salt ) )->digest;
    for ( 1 .. $iterations - 1 ) {
        my $sha         = Digest::SHA->new(256)->add($password_hash);
        my $is_last_run = $_ == $iterations - 1;
        $password_hash = $is_last_run ? $sha->hexdigest : $sha->digest;
    }
    return $password_hash;
}

__PACKAGE__->meta->make_immutable;
1;

=back

=head1 AUTHORS

Kevin Murani - L<https://github.com/amurani>

=cut

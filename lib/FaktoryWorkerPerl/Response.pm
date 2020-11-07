package FaktoryWorkerPerl::Response;

=pod

=head1 FaktoryWorkerPerl::Response

Response object for a parsed version of Faktory job server response

=cut

use FindBin;
use lib "$FindBin::Bin/lib";

use Moose;
use namespace::autoclean;
use feature qw(signatures);
no warnings qw(experimental::signatures);
use JSON;
use Data::Dump qw< pp >;
use FaktoryWorkerPerl::Types::Constants qw< :ResponseType >;

has raw_response => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has type => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has message => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has data => (
    is       => 'rw',
    isa      => 'HashRef',
    required => 0,
);

=item BUILDARGS hook

Parses responses from the Faktory job server
Please see L<Redis protocol format|https://redis.io/topics/protocol> for references on the format
=cut

around BUILDARGS => sub {
    my ( $orig, $class, $attribute_name, $raw_response ) = @_;
    my $parsed_response = {};
    if ( $raw_response =~ m/^{.*$/ ) {
        $parsed_response = { data => decode_json($raw_response) };
    } else {
        my ( $type, $data ) = split /\s+(.*)/, $raw_response;
        $parsed_response = {
            $type ? ( type => $type ) : (),
            $data ? ( $data =~ m/^{.*$/ ? ( data => decode_json($data), ) : ( message => $data ), ) : ()
        };
    }

    return $class->$orig( raw_response => $raw_response, %$parsed_response );
};

=item is_handshake()

Indicates if an initial handshake has been established

=cut

sub is_handshake($self) { $self->type eq HI }

=item is_okay()

Indicates if a response from the Faktory job server is OK

=cut

sub is_okay($self) { $self->type eq OK }

=item recv()

Indicates if a fetch call to the Faktory job server returned no jobs

=cut

sub has_no_jobs($self) { $self->type eq NO_JOBS }

__PACKAGE__->meta->make_immutable;
1;

=back

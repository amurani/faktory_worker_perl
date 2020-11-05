package FaktoryWorkerPerl::Job;

=pod

=head1 FaktoryWorkerPerl::Job

A single unit of work to be pushed to the Faktory job server and processed by the worker

TODO: factor in the job options for faktopry worker https://github.com/contribsys/faktory/wiki/The-Job-Payload#options
=cut

use FindBin;
use lib "$FindBin::Bin/lib";

use Moose;
use namespace::autoclean;
use feature qw(signatures);
no warnings qw(experimental::signatures);
use FaktoryWorkerPerl::Types::Queue;
use Data::GUID;

has jid => (
    is      => 'ro',
    isa     => 'Str',
    builder => '_build_jid',
    lazy    => 1,
);

has jobtype => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has created_at => (
    is  => 'rw',
    isa => 'Str',
);

has enqueued_at => (
    is  => 'rw',
    isa => 'Str',
);

has queue => (
    is  => 'rw',
    isa => 'Queue',
);

has retry => (
    is  => 'rw',
    isa => 'Int',
);

has args => (
    is       => 'rw',
    isa      => 'ArrayRef',
    default  => sub { [] },
    required => 0,
);

has json_serialized => (
    is      => 'ro',
    isa     => 'HashRef',
    builder => '_build_json_serialization',
    lazy    => 1,
);

=over

=item _build_jid ()

Generate a unique job id

=cut

sub _build_jid($self) {
    return Data::GUID->new->as_string;
}

=item _build_json_serialization ()

Generate a json serialization for the job

=cut

sub _build_json_serialization($self) {
    my %job = map { $_ => $self->$_ } qw< jid jobtype created_at enqueued_at queue retry args >;
    return \%job;
}

__PACKAGE__->meta->make_immutable;
1;

=back

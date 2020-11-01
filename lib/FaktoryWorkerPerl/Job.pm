package FaktoryWorkerPerl::Job;
use Moose;
use feature qw(signatures);
no warnings qw(experimental::signatures);
use Data::GUID;

has type => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has args => (
    is       => 'rw',
    isa      => 'ArrayRef',
    default  => sub { [] },
    required => 0,
);

has jid => (
    is      => 'ro',
    isa     => 'Str',
    builder => '_build_jid',
    lazy    => 1,
);

has json_serialized => (
    is      => 'ro',
    isa     => 'HashRef',
    builder => '_build_json_serialization',
    lazy    => 1,
);

=begin
TODO: factor in the job metadata https://github.com/contribsys/faktory/wiki/The-Job-Payload#metadata
TODO: factor in the job options for faktopry worker https://github.com/contribsys/faktory/wiki/The-Job-Payload#options
=cut

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
    return {
        jid     => $self->jid,
        jobtype => $self->type,
        args    => $self->args,
    };
}

__PACKAGE__->meta->make_immutable;
1;

package FaktoryWorker::Job;

=pod

=head1 NAME

C<FaktoryWorker::Job> - job to be processed by the Faktory job server

=head1 SYNOPSIS

    use FaktoryWorker::Job;
    use FaktoryWorker::Client;

    my $client = FaktoryWorker::Client->new;
    my $job = FaktoryWorker::Job->new(
        type    => 'poc_job',
        args    => [ int( rand(10) ), int( rand(10) ) ],
        logging => 1,
    );
    $client->push($job);

=head1 DESCRIPTION

C<FaktoryWorker::Job> represents a single unit of work to be pushed to the Faktory job server and processed by the worker

Please see L<job payload options|https://github.com/contribsys/faktory/wiki/The-Job-Payload#options> and L<job oayload metadata|https://github.com/contribsys/faktory/wiki/The-Job-Payload#options> for more details on attributes.

=head1 METHODS

=cut

use FindBin;
use lib "$FindBin::Bin/lib";

use Moose;
use namespace::autoclean;
use feature qw(signatures);
no warnings qw(experimental::signatures);
use FaktoryWorker::Types::Queue;
use Data::GUID;

has args => (
    is       => 'rw',
    isa      => 'ArrayRef',
    default  => sub { [] },
    required => 0,
);

has at => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has backtrace => (
    is      => 'rw',
    isa     => 'Int',
    default => sub { 0 },
);

has created_at => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has custom => (
    is       => 'rw',
    isa      => 'HashRef',
    default  => sub { {} },
    required => 0,
);

has enqueued_at => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has failure => (
    is       => 'rw',
    isa      => 'HashRef',
    default  => sub { {} },
    required => 0,
);

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

has json_serialized => (
    is      => 'ro',
    isa     => 'HashRef',
    builder => '_build_json_serialization',
    lazy    => 1,
);

has queue => (
    is      => 'rw',
    isa     => 'Queue',
    default => sub { 'default' }
);

has retry => (
    is      => 'rw',
    isa     => 'Int',
    default => sub { 25 },
);

=over

=item C<_build_jid()>

Generate a unique job id

=cut

sub _build_jid($self) {
    return Data::GUID->new->as_string;
}

=item C<_build_json_serialization()>

Generate a json serialization for the job

=cut

sub _build_json_serialization($self) {
    my %job = map { $_ => $self->$_ } qw<
        args at backtrace created_at custom enqueued_at failure jid jobtype queue retry
    >;
    return \%job;
}

__PACKAGE__->meta->make_immutable;
1;

=back

=head1 AUTHORS

Kevin Murani - L<https://github.com/amurani>

=cut

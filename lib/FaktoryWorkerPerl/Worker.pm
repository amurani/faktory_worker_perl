package FaktoryWorkerPerl::Worker;
use Moose;
use Moose::Util::TypeConstraints;
use feature qw(signatures say);
no warnings qw(experimental::signatures);
use Time::HiRes qw< usleep >;
use Data::Dump qw< pp >;

class_type Client => { class => 'FaktoryWorkerPerl::Client' };
has client        => (
    is       => 'rw',
    isa      => 'Client',
    required => 1,
);

enum Queue => [qw< critical default bulk >];
has queues => (
    is      => 'rw',
    isa     => 'ArrayRef[Queue]',
    default => sub { [qw< default >] },
);

has job_types => (
    is      => 'rw',
    isa     => 'HashRef',
    default => sub { {} },
);

has stop => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
);

has logging => (
    is       => 'rw',
    isa      => 'Bool',
    required => 0,
    default  => sub { 0 },
);

use constant SLEEP_INTERVAL => 250_000;

=item register()

Registers a job processor for each job type

=cut

sub register ( $self, $job_type, $callable ) {
    $self->job_types->{$job_type} = $callable;
}

=item run()

Processes jobs in the fakory job server
Can be daemonized or run once

=cut

sub run ( $self, $daemonize = 0 ) {

    do {
        $self->client->beat();

        my $job = $self->client->fetch( $self->queues );
        if ( $job && keys %$job ) {
            eval {
                my $callable = $self->job_types->{ $job->{jobtype} }
                    or die sprintf( "A worker for job type: %s has not been registered", $job->{jobtype} );
                $callable->($job);
                $self->client->ack( $job->{jid} );
            } or do {
                my $error = $@;
                say sprintf( "An error occured: %s for job: %s", $error, pp $job);
                $self->client->fail( $job->{jid}, $error );
            };
        } else {
            say "no jobs to run atm" if $self->logging;
        }

        usleep( $self->SLEEP_INTERVAL );
        say "worker is running as a daemon" if $daemonize;
        say "worker has not been asked to stop" if !$self->stop;
    } while ( $daemonize && !$self->stop );
}

__PACKAGE__->meta->make_immutable;
1;

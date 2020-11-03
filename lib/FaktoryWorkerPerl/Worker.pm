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
    default => sub { 0 },
);

has is_running => (
    is      => 'rw',
    isa     => 'Bool',
    default => sub { 0 },
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
    unless ($job_type) {
        warn "An job processor cannot be registered without a job type" unless $job_type;
        return 0;
    }

    unless ($callable) {
        warn "A job processor cannot be undefined" unless $callable;
        warn "A job processor must be a runnable subroutine" unless ref $callable eq 'CODE';
        return 0;
    }

    $self->job_types->{$job_type} = $callable;
    return exists $self->job_types->{$job_type};
}

=item run()

Processes jobs in the fakory job server
Can be daemonized or run once

=cut

sub run ( $self, $daemonize = 0 ) {
    do {
        unless ( $self->is_running ) {
            $self->is_running(1);
            say "worker is running as a daemon" if $self->logging && $daemonize;
        }
        $self->is_running(0) if $self->stop;

        my $heartbeat = $self->client->beat();
        last if ( $heartbeat eq 'terminate' );
        next if ( $heartbeat eq 'quiet' );

        my $job = $self->client->fetch( $self->queues );
        if ( $job && keys %$job ) {
            eval {
                my $callable = $self->job_types->{ $job->{jobtype} }
                    or die sprintf( "No worker for job type: %s has been registered", $job->{jobtype} );

                $callable->($job);
                $self->client->ack( $job->{jid} );
            } or do {
                my $error = $@;
                say sprintf( "An error occured: %s for job: %s", $error, pp $job) if $self->logging;

                my @backtrace = ();
                my $i         = 1;
                while ( ( my @caller_details = ( caller( $i++ ) ) ) ) {
                    my ( $package, $filename, $line, $subroutine ) = @caller_details;
                    push @backtrace, sprintf( "%s:%s in %s at %s", $package, $subroutine, $filename, $line );
                }
                $self->client->fail( $job->{jid}, "Exception", $error, [ reverse @backtrace ] );
            };
        } else {
            say "no jobs to run atm" if $self->logging;
        }

        say "worker has not been asked to stop" if $self->logging && !$self->stop;
        usleep( $self->SLEEP_INTERVAL );
    } while ( $daemonize && !$self->stop );
}

__PACKAGE__->meta->make_immutable;
1;

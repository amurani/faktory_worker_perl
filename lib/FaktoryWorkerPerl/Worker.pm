package FaktoryWorkerPerl::Worker;

=pod

=head1 FaktoryWorkerPerl::Worker

Worker that handles fetching jobs from the Faktory job server and processes them

=cut

use FindBin;
use lib "$FindBin::Bin/lib";

use Moose;
use namespace::autoclean;
use feature qw(signatures say);
no warnings qw(experimental::signatures);
use Time::HiRes qw< usleep >;
use Data::Dump qw< pp >;
use FaktoryWorkerPerl::Types::Queue;

with 'FaktoryWorkerPerl::Roles::Logger';

has client => (
    is       => 'rw',
    isa      => 'FaktoryWorkerPerl::Client',
    required => 1,
);

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

has concurrency => (
    is      => 'rw',
    isa     => 'Int',
    default => sub {
        20;
    },
);

use constant SLEEP_INTERVAL => 250_000;

=over

=item register()

Registers job processors for each job type

=cut

sub register ( $self, $job_type, $callable ) {
    unless ($job_type) {
        warn "An job processor cannot be registered without a job type" unless $job_type;
        return 0;
    }

    unless ($callable) {
        warn "A job processor cannot be undefined"           unless $callable;
        warn "A job processor must be a runnable subroutine" unless ref $callable eq 'CODE';
        return 0;
    }

    $self->job_types->{$job_type} = $callable;
    return exists $self->job_types->{$job_type};
}

=item run()

Processes jobs in the faktory job server.
It can be daemonized to run periodically or just run once

=cut

sub run ( $self, $daemonize = 0 ) {
    do {
        unless ( $self->is_running ) {
            $self->is_running(1);
            $self->logger->info("worker is running as a daemon") if $daemonize;
        }
        $self->is_running(0) if $self->stop;

        my $heartbeat = $self->client->beat();
        last if ( $heartbeat eq 'terminate' );
        next if ( $heartbeat eq 'quiet' );

        # TODO: @amurani add thread logic here
        $self->_process_job;

        $self->logger->info("worker has not been asked to stop") unless $self->stop;

        usleep( $self->SLEEP_INTERVAL );
    } while ( $daemonize && !$self->stop );
}

=item _process_job()

Processes a single job from the faktory job server.

=cut

sub _process_job($self) {
    my $job = $self->client->fetch( $self->queues );
    if ( $job && keys %$job ) {
        eval {
            my $callable = $self->job_types->{ $job->{jobtype} }
                or die sprintf( "No worker for job type: %s has been registered", $job->{jobtype} );

            $callable->($job);
            $self->client->ack( $job->{jid} );
        } or do {
            my $error = $@;
            $self->logger->info( sprintf( "An error occured: %s for job: %s", $error, pp $job) );

            my @backtrace = ();
            my $i         = 1;
            while ( ( my @caller_details = ( caller( $i++ ) ) ) ) {
                my ( $package, $filename, $line, $subroutine ) = @caller_details;
                push @backtrace, sprintf( "%s:%s in %s at %s", $package, $subroutine, $filename, $line );
            }
            $self->client->fail( $job->{jid}, "Exception", $error, [ reverse @backtrace ] );
        };
    } else {
        $self->logger->info("no jobs to run at present");
    }
}

__PACKAGE__->meta->make_immutable;

1;

=back

package FaktoryWorker::Worker;

=pod

=head1 NAME

C<FaktoryWorker::Worker> - processes jobs from the Faktory job server


=head1 SYNOPSIS

    use FaktoryWorker::Worker;
    use FaktoryWorker::Client;

    my $worker = FaktoryWorker::Worker->new(
        client => FaktoryWorker::Client->new,
        queues  => [qw< critical default bulk >],
    );

    $worker->register(
        'poc_job',
        sub {
            my $job = shift;

            say sprintf( "running job: %s", $job->{jid} );

            my $args = $job->{args};
            my ( $a, $b ) = @$args;
            my $sum = $a + $b;

            say sprintf( "sum: %d + %d = %d", $a, $b, $sum );

            return $sum;
        }
    );

    $worker->run(my $daemonize = 1);

=head1 DESCRIPTION

C<FaktoryWorker::Worker> is the worker that handles fetching jobs from the Faktory job server and processes them

=head1 METHODS

=cut

use FindBin;
use lib "$FindBin::Bin/lib";

use Moose;
use namespace::autoclean;
use feature qw(signatures say);
no warnings qw(experimental::signatures);
use Time::HiRes qw< usleep >;
use Data::Dump qw< pp >;
use FaktoryWorker::Types::Queue;

with 'FaktoryWorker::Roles::Logger';

has client => (
    is       => 'rw',
    isa      => 'FaktoryWorker::Client',
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

use constant SLEEP_INTERVAL => 250_000;

=over

=item C<register($job_type, $callable)>

Registers job processors for each job type

Takes a job type name string and a code ref callback for how to process the job

The callback takes a serialized hash of the FaktoryWorker::Job object as the only argument

=cut

sub register ( $self, $job_type, $callable ) {
    unless ($job_type) {
        warn "An job processor cannot be registered without a job type"
            unless $job_type;
        return 0;
    }

    unless ($callable) {
        warn "A job processor cannot be undefined" unless $callable;
        warn "A job processor must be a runnable subroutine"
            unless ref $callable eq 'CODE';
        return 0;
    }

    $self->job_types->{$job_type} = $callable;
    return exists $self->job_types->{$job_type};
}

=item C<run($daemonize)>

Processes jobs in the faktory job server.
It can be daemonized to run periodically or just run once

Takes a boolean (1|0) value as an argument to indicate if it is daemonized or not

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

        $self->logger->info("worker has not been asked to stop")
            unless $self->stop;

        usleep( $self->SLEEP_INTERVAL );
    } while ( $daemonize && !$self->stop );
}

__PACKAGE__->meta->make_immutable;

1;

=back

=head1 AUTHORS

Kevin Murani - L<https://github.com/amurani>

=cut

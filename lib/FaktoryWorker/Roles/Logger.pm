package FaktoryWorker::Roles::Logger;

use Moose::Role;
use Log::Log4perl qw< :levels get_logger >;

has 'logger' => (
    is      => 'ro',
    isa     => 'Log::Log4perl::Logger',
    lazy    => 1,
    builder => '_build_logger'
);

sub _build_logger {
    my $self = shift;

    Log::Log4perl->easy_init($INFO);
    return get_logger(__PACKAGE__);
}

1;

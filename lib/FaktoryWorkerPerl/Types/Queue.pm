package FaktoryWorkerPerl::Types::Queue;

use Moose;
use Moose::Util::TypeConstraints;
use namespace::autoclean;

enum Queue => [qw< critical default bulk >];

1;

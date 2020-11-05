## Faktory Perl Library

A perl client and worker library for the Faktory job server. The provided client allows one to push jobs to the Faktory server and the provided worker fetches background jobs from the Faktory server and processes them based on registered job processors.


## Installation
TODO: @amurani follow up on this section
```

```

## Links

* [Worker Lifecycle](https://github.com/contribsys/faktory/wiki/Worker-Lifecycle)
* [contribsys/faktory](https://github.com/contribsys/faktory)


## Usage

### Pushing jobs

```perl
use FaktoryWorkerPerl::Client;
use FaktoryWorkerPerl::Job;

my $client = FaktoryWorkerPerl::Client->new;
my $job = FaktoryWorkerPerl::Job->new(
    type => 'test_job',
    args => [ int(rand(10)), int(rand(10)) ],
);
my $job_id = $client->push($job);

```

### Processing jobs

```perl
use FaktoryWorkerPerl::Client;
use FaktoryWorkerPerl::Worker;

my $worker = FaktoryWorkerPerl::Worker->new(
    client => FaktoryWorkerPerl::Client->new,
    queues => [ qw< critical default bulk > ]
);

$worker->register('test_job', sub {
    my $job = shift;

    say sprintf("running job: %s", $job->{jid});

    my $args = $job->{args};
    my $a = $args->[0];
    my $b = $args->[0];
    my $sum = $a + $b;

    say sprintf("sum: %d + %d = %d", $a, $b, $sum);
});

$worker->run(my $daemonize = 1 );

```

## Tests

The tests can be run via

```bash
make test
```

## Acknowledgement


The majority of the structure of this library is based on the [faktory_worker_php5](https://github.com/jcs224/faktory_worker_php5) client so I think it's worth recognizing.

### Authors

Kevin Murani [@amurani](https://github.com/amurani)


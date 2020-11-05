## Faktory Perl Library

Faktory job queue library for perl. 

Most of the code is based on the [faktory_worker_php5](https://github.com/jcs224/faktory_worker_php5) client


## Usage

### Pushing jobs

```
use FaktoryWorkerPerl::Client;
use FaktoryWorkerPerl::Job;

my $client = FaktoryWorkerPerl::Client->new;
do {
    my $job = FaktoryWorkerPerl::Job->new(
        type => 'test_job',
        args => [ int(rand(10)), int(rand(10)) ],
    );
    $client->push($job);

    usleep(1_000_000);	# or however you want to handle delays
} while (1);


```

### Processing jobs

```
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

    return $sum;
});

$worker->run(1);

```


#### Authors
[@amurani](https://github.com/amurani)

package FaktoryWorker::Types::Constants;

use constant ACK   => 'ACK';
use constant BEAT  => 'BEAT';
use constant FAIL  => 'FAIL';
use constant FETCH => 'FETCH';
use constant HELLO => 'HELLO';
use constant PUSH  => 'PUSH';

use constant ERROR   => "-ERR";
use constant HI      => "+HI";
use constant NO_JOBS => "\$-1";    # actual value: \$-1\r\n
use constant OK      => "+OK";     # actual value: +OK\r\n

use Exporter qw< import >;
our @EXPORT_OK   = (qw< ACK BEAT FAIL FETCH HELLO PUSH ERROR HI NO_JOBS OK >);
our %EXPORT_TAGS = (
    RequestCommand => [qw< ACK BEAT FAIL FETCH HELLO PUSH >],
    ResponseType   => [qw< ERROR HI NO_JOBS OK >],
);

1;

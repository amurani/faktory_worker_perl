# Install needed perl dependencies
install_deps:
	cpanm --installdeps --notest .

# Run Faktory job server
run_job_server:
	docker-compose -f docker-compose.yml up

# Run tests for Faktory Perl Library
tests:
	echo "Check that Faktory server is running"
	curl --verbose --silent --output /dev/null http://localhost:7420/
	prove -vr  t/

# Run tests for Faktory Perl Library with Faktory job server daemonized
run_tests:
	echo "Starting FaktoryWorkerPerl tests"
	docker-compose -f docker-compose.yml up -d
	make tests
	docker-compose down
	echo "Finshed FaktoryWorkerPerl tests"



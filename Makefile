# Include dotenv file
ENV ?= development    # set this as the default end
-include .env        # load default environment values
-include .env.$(ENV) # load any environments values based on specific environments
export

get_environment:
	echo Running in $(ENV)

# Install needed perl dependencies
install_deps:
	cpanm --installdeps --notest .

# Run Faktory job server
run_job_server:
	docker-compose -f docker-compose.yml up

is_job_server_running:
	echo "Check that Faktory server is running"
	curl --verbose --silent --output /dev/null http://${FAKTORY_HOST}:${FAKTORY_WEB_PORT}/

# Run tests for Faktory Perl Library
tests:
	prove -vr  t/

# Run tests for Faktory Perl Library with Faktory job server daemonized
run_tests:
	echo "Starting FaktoryWorker tests"
	docker-compose -f docker-compose.yml up -d
	make tests
	docker-compose down
	echo "Finshed FaktoryWorker tests"



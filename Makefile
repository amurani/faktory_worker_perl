# Run tests for Faktory Perl Library
test:
	echo "Starting FaktoryWorkerPerl tests"
	docker-compose up -d
	prove -vr  t/
	docker-compose down
	echo "Finshed FaktoryWorkerPerl tests"

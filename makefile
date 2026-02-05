.PHONY: install server test benchmark clean cluster

install:
	pip install -r requirements.txt

server:
	python run_server.py --port 5000

test:
	python -m pytest tests/ -v --tb=short -x

test-basic:
	python -m pytest tests/test_basic.py -v

test-search:
	python -m pytest tests/test_search.py -v

test-durability:
	python -m pytest tests/test_durability.py -v

test-acid:
	python -m pytest tests/test_acid.py -v

benchmark:
	python benchmarks/benchmark.py

cluster-node1:
	python cluster/run_cluster.py --node-id 1

cluster-node2:
	python cluster/run_cluster.py --node-id 2

cluster-node3:
	python cluster/run_cluster.py --node-id 3

masterless-node1:
	python cluster/run_masterless.py --node-id 1

masterless-node2:
	python cluster/run_masterless.py --node-id 2

masterless-node3:
	python cluster/run_masterless.py --node-id 3

clean:
	rm -rf data/ test_data* data_node_* data_masterless_* bench_data_*
	rm -rf __pycache__ */__pycache__ */*/__pycache__
	rm -rf .pytest_cache
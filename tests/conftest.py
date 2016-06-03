import logging

logging.basicConfig(format='%(levelname)s - %(name)s - %(message)s')


def pytest_addoption(parser):
    parser.addoption("--quick", action="store_true", help="Run only quick tests.")

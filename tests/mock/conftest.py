import pytest
import mongomock
from mongodantic import MongoModel
from mongodantic.connection import connect, set_connection_env, _DBConnection
from typing import get_type_hints, Optional


def _can_initialize_with_none(model):
    hints = get_type_hints(model)
    return all(
        getattr(typ, '__origin__', None) is Optional or typ is Optional for typ in hints.values()
    )


@pytest.fixture(scope="session", autouse=True)
def setup_mongodb_mock():
    """Setup MongoDB mock for tests."""
    # Set test environment
    set_connection_env("test")

    # Create mock MongoDB client
    mock_client = mongomock.MongoClient()

    # Patch _DBConnection to use our mock client
    original_init = _DBConnection._init_mongo_connection
    _DBConnection._init_mongo_connection = lambda self, connect=False: mock_client

    # Connect using mock client
    connect(
        connection_str="mongodb://localhost:27018",
        dbname="test",
        env_name="test"
    )

    yield

    # Restore original method
    _DBConnection._init_mongo_connection = original_init

    # Cleanup after tests
    for model in MongoModel.__subclasses__():
        if hasattr(model, 'Q'):
            try:
                if _can_initialize_with_none(model):
                    default_values = {
                        field: None for field in model.__annotations__}
                    _ = model(**default_values)
                if model.Q is not None:
                    model.Q.drop_collection(force=True)
            except Exception as e:
                print(f"Error cleaning up model {model.__name__}: {str(e)}")


@pytest.fixture(scope="function")
def clean_collections():
    """Clean all collections before each test."""
    for model in MongoModel.__subclasses__():
        if hasattr(model, 'Q'):
            try:
                if _can_initialize_with_none(model):
                    default_values = {
                        field: None for field in model.__annotations__}
                    _ = model(**default_values)
                if model.Q is not None:
                    model.Q.drop_collection(force=True)
            except Exception as e:
                print(
                    f"Error cleaning collection for {model.__name__}: {str(e)}")
    yield

import os
import pytest
from mongodantic import MongoModel
from mongodantic.connection import connect, set_connection_env, _connections
from bson.objectid import ObjectId


@pytest.fixture(scope="session", autouse=True)
def setup_mongodb():
    """Setup MongoDB connection for tests."""
    # Set test environment
    set_connection_env("test")

    # Connect to MongoDB
    connect(
        connection_str="mongodb://localhost:27017",
        dbname="test",
        env_name="test"
    )

    yield

    # Cleanup after tests
    for model in MongoModel.__subclasses__():
        if hasattr(model, 'Q'):
            try:
                # Initialize with default values to avoid validation errors
                if model.__name__ == 'Product':
                    _ = model(title='', cost=0.0, quantity=0,
                              product_type='', config={})
                elif model.__name__ == 'ProductImage':
                    _ = model(url='', product_id=str(ObjectId()))
                elif model.__name__ == 'Ticket':
                    _ = model(name='', position=0, config={})
                else:
                    # For other models, try to create with empty dict
                    try:
                        _ = model()
                    except Exception:
                        # If that fails, skip this model
                        continue
                if model.Q is not None:
                    model.Q.drop_collection(force=True)
            except Exception as e:
                print(f"Error cleaning up model {model.__name__}: {str(e)}")

    # Close all connections at the end of the session
    for conn in list(_connections.values()):
        try:
            conn.close()
        except Exception as e:
            print(f"Error closing connection: {str(e)}")
    _connections.clear()


@pytest.fixture(scope="function", autouse=True)
def clean_collections():
    """Clean all collections before each test."""
    for model in MongoModel.__subclasses__():
        if hasattr(model, 'Q'):
            try:
                _ = model()  # Ensure Q is initialized
                if model.Q is not None:
                    model.Q.drop_collection(force=True)
            except Exception as e:
                print(
                    f"Error cleaning collection for {model.__name__}: {str(e)}")
    yield

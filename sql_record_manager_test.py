
import os
import unittest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from _sql_record_manager import SQLRecordManager, UpsertionRecord, Base
from dotenv import load_dotenv

load_dotenv()

collection_name = "vectordb"
namespace = f"TiDB/{collection_name}"
TIDB_URL = os.environ.get('TIDB_URL')

class TestSQLRecordManager(unittest.TestCase):

    def setUp(self):
        # Setup an in-memory SQLite engine for testing
        self.engine = create_engine('sqlite:///:memory:')
        self.Session = sessionmaker(bind=self.engine)
        self.record_manager = SQLRecordManager(namespace="test_ns", engine=self.engine)
        self.record_manager.create_schema()

    def tearDown(self):
        # Drop all tables after each test
        Base.metadata.drop_all(self.engine)

    def test_create_schema(self):
        """Test if the schema is created correctly."""
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        self.assertIn('dev_ai_agent_record_manager', tables)

    def test_upsert_record(self):
        """Test the upsertion of records."""
        # Upsert two records
        keys = ['key1', 'key2']
        self.record_manager.update(keys)
        
        # Check if the records exist
        exists = self.record_manager.exists(keys)
        self.assertEqual(exists, [True, True])

        # Upsert the same keys with group_ids
        group_ids = ['group1', 'group2']
        self.record_manager.update(keys, group_ids=group_ids)
        
        # Check if the records are updated with group_ids
        with self.Session() as session:
            records = session.query(UpsertionRecord).all()
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0].group_id, 'group1')
            self.assertEqual(records[1].group_id, 'group2')

    def test_key_existence(self):
        """Test if exists() method correctly checks for key existence."""
        keys = ['key1', 'key2', 'key3']
        self.record_manager.update(keys)
        
        exists = self.record_manager.exists(['key1', 'key4'])
        self.assertEqual(exists, [True, False])

    def test_list_keys(self):
        """Test if list_keys() returns correct results."""
        keys = ['key1', 'key2', 'key3']
        self.record_manager.update(keys)
        
        # List all keys
        listed_keys = self.record_manager.list_keys()
        self.assertEqual(set(listed_keys), set(keys))

        # Test with a limit
        listed_keys_limited = self.record_manager.list_keys(limit=2)
        self.assertEqual(len(listed_keys_limited), 2)

    def test_delete_keys(self):
        """Test if delete_keys() correctly deletes the records."""
        keys = ['key1', 'key2', 'key3']
        self.record_manager.update(keys)

        # Delete one key
        self.record_manager.delete_keys(['key2'])
        
        # Check if 'key2' was deleted
        exists = self.record_manager.exists(['key1', 'key2', 'key3'])
        self.assertEqual(exists, [True, False, True])

if __name__ == '__main__':
    unittest.main()
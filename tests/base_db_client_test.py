import pytest
import datetime
import time
from abc import ABC, abstractmethod
from kptn.caching.models import TaskState


class BaseDbClientTest(ABC):
    """
    Base test class for database client implementations.
    
    This class defines a common set of tests that should be implemented
    by all database client implementations. Subclasses should provide
    the concrete database client fixture via the `db` fixture.
    """

    @abstractmethod
    @pytest.fixture
    def db(self):
        """
        Abstract fixture that should return a configured database client instance.
        Must be implemented by subclasses.
        """
        pass

    def test_create_and_get_task(self, db):
        """Test creating and retrieving a task."""
        task = TaskState(start_time='3')
        db.create_task("A", task)
        task = db.get_task("A")
        assert task.start_time == '3'

    def test_create_taskdata_str(self, db):
        """Test creating and retrieving string task data."""
        db.create_task("A", TaskState(start_time='3'))
        db.create_taskdata("A", "blah")
        taskdata = db.get_taskdata("A")
        assert taskdata == "blah"

    def test_create_taskdata_dict(self, db):
        """Test creating and retrieving dictionary task data."""
        db.create_task("A", TaskState(start_time='3'))
        db.create_taskdata("A", {"blah": 3})
        taskdata = db.get_taskdata("A")
        assert taskdata == {"blah": 3}

    def test_create_taskdata_list(self, db):
        """Test creating and retrieving large list task data."""
        list_of_three_thousand_items = [
            {"i": i, "startTime": datetime.datetime.now().isoformat()} 
            for i in range(3000)
        ]
        db.create_task("A", TaskState(start_time='3'), data=list_of_three_thousand_items)
        taskdata = db.get_taskdata("A")
        assert isinstance(taskdata, list)
        assert len(taskdata) == 3000

    def test_get_task_with_data(self, db):
        """Test retrieving a task with its associated data."""
        list_of_three_thousand_items = [i for i in range(3000)]
        db.create_task("A", TaskState(start_time='3'), data=list_of_three_thousand_items)
        task = db.get_task("A", include_data=True)
        assert task.taskdata_count == 3000
        assert task.start_time == '3'
        assert isinstance(task.data, list)
        assert len(task.data) == 3000

    def test_set_subtask_started(self, db):
        """Test setting a subtask as started."""
        db.create_task("A", TaskState(start_time='3'))
        db.create_subtasks("A", ["X", "Y", "Z"])
        db.set_subtask_started("A", 1)
        subtasks = db.get_subtasks("A")
        task = db.get_task("A")
        assert task.subtask_count == 3
        assert len(subtasks) == 3
        # Confirm startTime is later than 2024-01-01
        assert datetime.datetime.fromisoformat(subtasks[1].startTime) > datetime.datetime(2024, 1, 1)

    def test_set_subtask_started_second_bin(self, db):
        """Test setting a subtask as started in a large collection (tests binning)."""
        db.create_task("A", TaskState(start_time='3'))
        list_of_three_thousand_items = [f"{i}" for i in range(3000)]
        db.create_subtasks("A", list_of_three_thousand_items)
        db.set_subtask_started("A", 2000)
        subtasks = db.get_subtasks("A")
        assert len(subtasks) == 3000
        # Confirm startTime is later than 2024-01-01
        assert datetime.datetime.fromisoformat(subtasks[2000].startTime) > datetime.datetime(2024, 1, 1)

    def test_set_subtask_ended(self, db):
        """Test setting a subtask as ended."""
        db.create_task("A", TaskState(start_time='3'))
        db.create_subtasks("A", ["X", "Y", "Z"])
        db.set_subtask_started("A", 1)
        time.sleep(0.05)
        db.set_subtask_ended("A", 1)
        subtasks = db.get_subtasks("A")
        item = subtasks[1]
        assert item.key == "Y"
        assert datetime.datetime.fromisoformat(item.startTime) < datetime.datetime.fromisoformat(item.endTime)

    def test_delete_task(self, db):
        """Test deleting a task and all its associated data."""
        db.create_task("A", TaskState(start_time='3'))
        big_list = [i for i in range(30000)]
        db.create_taskdata("A", big_list)
        db.create_subtasks("A", ["X", "Y", "Z"])
        db.delete_task("A")
        assert db.get_task("A") == None
        assert db.get_taskdata("A", bin_ids=["0"]) == []
        assert db.get_subtasks("A", bin_ids=["0"]) == []

    def test_create_and_subset(self, db):
        """Test creating a task result and then creating a subset of it."""
        task_name = "A"
        db.create_task(task_name, TaskState(start_time='3'))
        result = [1, 2, 3]
        db.set_task_ended(task_name, result, result_hash="X")
        subset_result = [1, 2]
        db.set_task_ended(task_name, subset_result, subset_mode=True)
        assert db.get_taskdata("A") == [1, 2, 3]
        assert db.get_taskdata("A", subset_mode=True) == [1, 2]
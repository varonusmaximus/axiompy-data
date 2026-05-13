"""
Unit tests for axiompy.data.pipeline module.

Tests Pipeline and Task for ETL workflow orchestration.
"""

import time

import pytest

from axiompy.data.pipeline import Pipeline, Task
from axiompy.data.types import TaskStatus


class TestTask:
    """Test Task class."""

    def test_task_creation(self):
        """Test creating a basic task."""

        def my_func():
            return "result"

        task = Task(name="test_task", func=my_func)

        assert task.name == "test_task"
        assert task.func == my_func
        assert task.status == TaskStatus.PENDING
        assert task.result is None
        assert task.error is None

    def test_task_with_dependencies(self):
        """Test creating task with dependencies."""
        task = Task(name="dependent_task", func=lambda: "result", depends_on=["task1", "task2"])

        assert task.depends_on == ["task1", "task2"]

    def test_task_execute_success(self):
        """Test successful task execution."""

        def my_func():
            return "success"

        task = Task(name="test", func=my_func)
        context = {}

        result = task.execute(context)

        assert result == "success"
        assert task.status == TaskStatus.SUCCESS
        assert task.result == "success"
        assert task.error is None
        assert task.start_time is not None
        assert task.end_time is not None

    def test_task_execute_with_context(self):
        """Test task execution with context parameter."""

        def my_func(context):
            return context.get("value", 0) * 2

        task = Task(name="test", func=my_func)
        context = {"value": 5}

        result = task.execute(context)

        assert result == 10
        assert task.status == TaskStatus.SUCCESS

    def test_task_execute_failure(self):
        """Test task execution with error."""

        def failing_func():
            raise ValueError("Task failed!")

        task = Task(name="test", func=failing_func)
        context = {}

        with pytest.raises(ValueError, match="Task failed!"):
            task.execute(context)

        assert task.status == TaskStatus.FAILED
        assert task.error == "Task failed!"
        assert task.end_time is not None

    def test_task_get_duration(self):
        """Test getting task duration."""

        def slow_func():
            time.sleep(0.1)
            return "done"

        task = Task(name="test", func=slow_func)
        context = {}

        task.execute(context)
        duration = task.get_duration()

        assert duration is not None
        assert duration >= 0.1


class TestPipeline:
    """Test Pipeline class."""

    def test_pipeline_creation(self):
        """Test creating a pipeline."""
        pipeline = Pipeline("test_pipeline")

        assert pipeline.name == "test_pipeline"
        assert len(pipeline.tasks) == 0
        assert len(pipeline.context) == 0

    def test_add_task(self):
        """Test adding tasks to pipeline."""
        pipeline = Pipeline("test")

        task1 = Task(name="task1", func=lambda: "result1")
        task2 = Task(name="task2", func=lambda: "result2")

        pipeline.add_task(task1)
        pipeline.add_task(task2)

        assert len(pipeline.tasks) == 2
        assert "task1" in pipeline.tasks
        assert "task2" in pipeline.tasks

    def test_add_task_duplicate_name(self):
        """Test error when adding task with duplicate name."""
        pipeline = Pipeline("test")

        task1 = Task(name="same_name", func=lambda: "result1")
        task2 = Task(name="same_name", func=lambda: "result2")

        pipeline.add_task(task1)

        with pytest.raises(ValueError, match="already exists"):
            pipeline.add_task(task2)

    def test_add_tasks_batch(self):
        """Test adding multiple tasks at once."""
        pipeline = Pipeline("test")

        tasks = [
            Task(name="task1", func=lambda: "result1"),
            Task(name="task2", func=lambda: "result2"),
            Task(name="task3", func=lambda: "result3"),
        ]

        pipeline.add_tasks(tasks)

        assert len(pipeline.tasks) == 3

    def test_execution_order_no_dependencies(self):
        """Test execution order with no dependencies."""
        pipeline = Pipeline("test")

        task1 = Task(name="task1", func=lambda: "result1")
        task2 = Task(name="task2", func=lambda: "result2")
        task3 = Task(name="task3", func=lambda: "result3")

        pipeline.add_tasks([task1, task2, task3])

        order = pipeline._get_execution_order()

        # All tasks should be in order (no dependencies)
        assert len(order) == 3
        assert set(order) == {"task1", "task2", "task3"}

    def test_execution_order_with_dependencies(self):
        """Test execution order respects dependencies."""
        pipeline = Pipeline("test")

        task1 = Task(name="task1", func=lambda: "result1")
        task2 = Task(name="task2", func=lambda: "result2", depends_on=["task1"])
        task3 = Task(name="task3", func=lambda: "result3", depends_on=["task2"])

        pipeline.add_tasks([task3, task1, task2])  # Add in random order

        order = pipeline._get_execution_order()

        # Should be ordered: task1 -> task2 -> task3
        assert order.index("task1") < order.index("task2")
        assert order.index("task2") < order.index("task3")

    def test_execution_order_circular_dependency(self):
        """Test error on circular dependency."""
        pipeline = Pipeline("test")

        task1 = Task(name="task1", func=lambda: "result1", depends_on=["task2"])
        task2 = Task(name="task2", func=lambda: "result2", depends_on=["task1"])

        pipeline.add_tasks([task1, task2])

        with pytest.raises(ValueError, match="Circular dependency"):
            pipeline._get_execution_order()

    def test_run_simple_pipeline(self):
        """Test running a simple pipeline."""
        pipeline = Pipeline("test")

        task1 = Task(name="task1", func=lambda: 10)
        task2 = Task(name="task2", func=lambda context: context["task1"] * 2)
        task3 = Task(name="task3", func=lambda context: context["task2"] + 5)

        task2.depends_on = ["task1"]
        task3.depends_on = ["task2"]

        pipeline.add_tasks([task1, task2, task3])

        results = pipeline.run()

        assert results["success"] is True
        assert len(results["tasks"]) == 3
        assert results["tasks"]["task1"]["status"] == "success"
        assert results["tasks"]["task2"]["status"] == "success"
        assert results["tasks"]["task3"]["status"] == "success"
        assert pipeline.get_task_result("task1") == 10
        assert pipeline.get_task_result("task2") == 20
        assert pipeline.get_task_result("task3") == 25

    def test_run_with_failure_fail_fast(self):
        """Test pipeline stops on failure with fail_fast=True."""
        pipeline = Pipeline("test")

        task1 = Task(name="task1", func=lambda: "success")
        task2 = Task(name="task2", func=lambda: 1 / 0, depends_on=["task1"])  # Will fail
        task3 = Task(name="task3", func=lambda: "success", depends_on=["task2"])

        pipeline.add_tasks([task1, task2, task3])

        results = pipeline.run(fail_fast=True)

        assert results["success"] is False
        assert len(results["errors"]) > 0
        assert results["tasks"]["task1"]["status"] == "success"
        assert results["tasks"]["task2"]["status"] == "failed"
        # task3 might not be in results if pipeline stopped early

    def test_run_with_failure_continue(self):
        """Test pipeline continues on failure with fail_fast=False."""
        pipeline = Pipeline("test")

        # Independent tasks, one fails
        task1 = Task(name="task1", func=lambda: "success")
        task2 = Task(name="task2", func=lambda: 1 / 0)  # Will fail
        task3 = Task(name="task3", func=lambda: "success")

        pipeline.add_tasks([task1, task2, task3])

        results = pipeline.run(fail_fast=False)

        assert results["success"] is False
        assert len(results["errors"]) == 1
        assert results["tasks"]["task1"]["status"] == "success"
        assert results["tasks"]["task2"]["status"] == "failed"
        assert results["tasks"]["task3"]["status"] == "success"

    def test_skip_dependent_tasks_on_failure(self):
        """Test dependent tasks are skipped when dependency fails."""
        pipeline = Pipeline("test")

        task1 = Task(name="task1", func=lambda: 1 / 0)  # Will fail
        task2 = Task(name="task2", func=lambda: "success", depends_on=["task1"])

        pipeline.add_tasks([task1, task2])

        results = pipeline.run(fail_fast=False)

        assert pipeline.tasks["task1"].status == TaskStatus.FAILED
        assert pipeline.tasks["task2"].status == TaskStatus.SKIPPED

    def test_task_retry(self):
        """Test task retry functionality."""
        attempt_count = {"count": 0}

        def flaky_func():
            attempt_count["count"] += 1
            if attempt_count["count"] < 3:
                raise ValueError("Not yet!")
            return "success"

        pipeline = Pipeline("test", settings={"retry_delay": 0.01})
        task = Task(name="flaky", func=flaky_func, retry_count=2)

        pipeline.add_task(task)
        results = pipeline.run()

        assert results["success"] is True
        assert attempt_count["count"] == 3  # Initial + 2 retries

    def test_get_task_status(self):
        """Test getting task status."""
        pipeline = Pipeline("test")
        task = Task(name="task1", func=lambda: "result")
        pipeline.add_task(task)

        # Before execution
        assert pipeline.get_task_status("task1") == TaskStatus.PENDING

        # After execution
        pipeline.run()
        assert pipeline.get_task_status("task1") == TaskStatus.SUCCESS

    def test_get_task_result(self):
        """Test getting task result."""
        pipeline = Pipeline("test")
        task = Task(name="task1", func=lambda: "my_result")
        pipeline.add_task(task)

        pipeline.run()

        assert pipeline.get_task_result("task1") == "my_result"

    def test_reset_pipeline(self):
        """Test resetting pipeline state."""
        pipeline = Pipeline("test")
        task = Task(name="task1", func=lambda: "result")
        pipeline.add_task(task)

        # Run pipeline
        pipeline.run()
        assert task.status == TaskStatus.SUCCESS
        assert task.result == "result"

        # Reset
        pipeline.reset()
        assert task.status == TaskStatus.PENDING
        assert task.result is None
        assert task.error is None
        assert len(pipeline.context) == 0

    def test_visualize_pipeline(self):
        """Test pipeline visualization."""
        pipeline = Pipeline("test_pipeline")

        task1 = Task(name="extract", func=lambda: "data")
        task2 = Task(name="transform", func=lambda: "transformed", depends_on=["extract"])
        task3 = Task(name="load", func=lambda: "loaded", depends_on=["transform"])

        pipeline.add_tasks([task1, task2, task3])

        viz = pipeline.visualize()

        assert "Pipeline: test_pipeline" in viz
        assert "extract" in viz
        assert "transform" in viz
        assert "load" in viz
        assert "depends on: extract" in viz


class TestPipelineIntegration:
    """Integration tests for complete pipeline workflows."""

    def test_etl_pipeline(self):
        """Test a complete ETL pipeline."""
        pipeline = Pipeline("etl")

        # Extract
        def extract():
            return [1, 2, 3, 4, 5]

        # Transform
        def transform(context):
            data = context["extract"]
            return [x * 2 for x in data]

        # Load
        def load(context):
            data = context["transform"]
            return {"count": len(data), "sum": sum(data)}

        extract_task = Task(name="extract", func=extract)
        transform_task = Task(name="transform", func=transform, depends_on=["extract"])
        load_task = Task(name="load", func=load, depends_on=["transform"])

        pipeline.add_tasks([extract_task, transform_task, load_task])

        results = pipeline.run()

        assert results["success"] is True
        assert pipeline.get_task_result("extract") == [1, 2, 3, 4, 5]
        assert pipeline.get_task_result("transform") == [2, 4, 6, 8, 10]
        assert pipeline.get_task_result("load") == {"count": 5, "sum": 30}

    def test_parallel_branches(self):
        """Test pipeline with parallel branches that merge."""
        pipeline = Pipeline("parallel")

        # Two independent branches
        branch_a = Task(name="branch_a", func=lambda: "result_a")
        branch_b = Task(name="branch_b", func=lambda: "result_b")

        # Merge both branches
        merge = Task(
            name="merge",
            func=lambda context: f"{context['branch_a']}+{context['branch_b']}",
            depends_on=["branch_a", "branch_b"],
        )

        pipeline.add_tasks([branch_a, branch_b, merge])

        results = pipeline.run()

        assert results["success"] is True
        assert pipeline.get_task_result("merge") == "result_a+result_b"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

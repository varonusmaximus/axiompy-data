"""
ETL pipeline framework for orchestrating data workflows.

Provides a simple but powerful pipeline framework for building and
running data transformation workflows with dependency management.
"""

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from axiompy.loggers import LoggerFactory

from axiompy.data.types import TaskStatus

logger = LoggerFactory.create_logger(__name__)


@dataclass
class Task:
    """
    A single task in a data pipeline.

    Tasks can depend on other tasks and will only execute after their
    dependencies complete successfully.
    """

    name: str
    func: Callable
    depends_on: List[str] = field(default_factory=list)
    retry_count: int = 0
    timeout: Optional[int] = None
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the task function.

        Args:
            context: Shared context dictionary with results from previous tasks

        Returns:
            Task result

        Raises:
            Exception: If task execution fails
        """
        self.status = TaskStatus.RUNNING
        self.start_time = datetime.now()

        logger.info(f"Executing task: {self.name}")

        try:
            # Pass context to function if it accepts it
            import inspect

            sig = inspect.signature(self.func)
            result = self.func(context=context) if "context" in sig.parameters else self.func()

            self.result = result
            self.status = TaskStatus.SUCCESS
            self.end_time = datetime.now()

            duration = (self.end_time - self.start_time).total_seconds()
            logger.info(f"Task '{self.name}' completed successfully in {duration:.2f}s")

            return result

        except Exception as e:
            self.status = TaskStatus.FAILED
            self.error = str(e)
            self.end_time = datetime.now()

            logger.error(f"Task '{self.name}' failed: {e}")
            raise

    def get_duration(self) -> Optional[float]:
        """Get task execution duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class Pipeline:
    """
    ETL pipeline for orchestrating data workflows.

    Manages task dependencies, execution order, and error handling.
    """

    def __init__(self, name: str, settings: Optional[Dict] = None):
        """
        Initialize the pipeline.

        Args:
            name: Pipeline name
            settings: Optional configuration settings
        """
        self.name = name
        self.settings = settings or {}
        self.tasks: Dict[str, Task] = {}
        self.context: Dict[str, Any] = {}
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    def add_task(self, task: Task) -> None:
        """
        Add a task to the pipeline.

        Args:
            task: Task to add

        Raises:
            ValueError: If task with same name already exists
        """
        if task.name in self.tasks:
            raise ValueError(f"Task '{task.name}' already exists in pipeline")

        # Validate dependencies exist
        for dep in task.depends_on:
            if dep not in self.tasks:
                logger.warning(f"Task '{task.name}' depends on '{dep}' which hasn't been added yet")

        self.tasks[task.name] = task
        logger.debug(f"Added task '{task.name}' to pipeline '{self.name}'")

    def add_tasks(self, tasks: List[Task]) -> None:
        """Add multiple tasks to the pipeline."""
        for task in tasks:
            self.add_task(task)

    def _get_execution_order(self) -> List[str]:
        """
        Determine task execution order based on dependencies.

        Returns:
            List of task names in execution order

        Raises:
            ValueError: If circular dependency detected
        """
        # Topological sort
        visited = set()
        temp_visited = set()
        order = []

        def visit(task_name: str):
            if task_name in temp_visited:
                raise ValueError(f"Circular dependency detected involving '{task_name}'")
            if task_name in visited:
                return

            temp_visited.add(task_name)
            task = self.tasks[task_name]

            for dep in task.depends_on:
                if dep not in self.tasks:
                    raise ValueError(f"Task '{task_name}' depends on unknown task '{dep}'")
                visit(dep)

            temp_visited.remove(task_name)
            visited.add(task_name)
            order.append(task_name)

        for task_name in self.tasks:
            if task_name not in visited:
                visit(task_name)

        return order

    def run(self, fail_fast: bool = True) -> Dict[str, Any]:
        """
        Execute the pipeline.

        Args:
            fail_fast: If True, stop on first task failure

        Returns:
            Dictionary with pipeline execution results
        """
        logger.info(f"Starting pipeline: {self.name}")
        self.start_time = datetime.now()

        try:
            execution_order = self._get_execution_order()
            logger.info(f"Execution order: {' -> '.join(execution_order)}")
        except ValueError as e:
            logger.error(f"Pipeline configuration error: {e}")
            return {"success": False, "error": str(e), "tasks": {}}

        results = {"success": True, "tasks": {}, "errors": []}

        for task_name in execution_order:
            task = self.tasks[task_name]

            # Check if dependencies succeeded
            deps_failed = False
            for dep_name in task.depends_on:
                if self.tasks[dep_name].status != TaskStatus.SUCCESS:
                    logger.warning(
                        f"Skipping task '{task_name}' due to failed dependency '{dep_name}'"
                    )
                    task.status = TaskStatus.SKIPPED
                    deps_failed = True
                    break

            if deps_failed:
                continue

            # Execute task with retries
            attempts = 0
            max_attempts = task.retry_count + 1

            while attempts < max_attempts:
                try:
                    result = task.execute(self.context)
                    self.context[task_name] = result
                    results["tasks"][task_name] = {
                        "status": "success",
                        "duration": task.get_duration(),
                        "result": result,
                    }
                    break

                except Exception as e:
                    attempts += 1
                    if attempts < max_attempts:
                        logger.warning(
                            f"Task '{task_name}' failed (attempt {attempts}/{max_attempts}), "
                            "retrying..."
                        )
                        time.sleep(self.settings.get("retry_delay", 1))
                    else:
                        logger.error(f"Task '{task_name}' failed after {attempts} attempts")
                        results["success"] = False
                        results["errors"].append({"task": task_name, "error": str(e)})
                        results["tasks"][task_name] = {
                            "status": "failed",
                            "duration": task.get_duration(),
                            "error": str(e),
                        }

                        if fail_fast:
                            logger.error("Stopping pipeline due to task failure (fail_fast=True)")
                            self.end_time = datetime.now()
                            return results

        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()

        logger.info(f"Pipeline '{self.name}' completed in {duration:.2f}s")
        logger.info(
            f"Tasks completed: "
            f"{sum(1 for t in self.tasks.values() if t.status == TaskStatus.SUCCESS)}"
            f"/{len(self.tasks)}"
        )

        results["duration"] = duration
        return results

    def get_task_status(self, task_name: str) -> TaskStatus:
        """Get the status of a specific task."""
        if task_name not in self.tasks:
            raise ValueError(f"Task '{task_name}' not found")
        return self.tasks[task_name].status

    def get_task_result(self, task_name: str) -> Any:
        """Get the result of a specific task."""
        if task_name not in self.tasks:
            raise ValueError(f"Task '{task_name}' not found")
        return self.tasks[task_name].result

    def reset(self) -> None:
        """Reset all tasks to pending state."""
        for task in self.tasks.values():
            task.status = TaskStatus.PENDING
            task.result = None
            task.error = None
            task.start_time = None
            task.end_time = None
        self.context.clear()
        self.start_time = None
        self.end_time = None
        logger.info(f"Pipeline '{self.name}' reset")

    def visualize(self) -> str:
        """
        Generate a simple text visualization of the pipeline.

        Returns:
            String representation of the pipeline DAG
        """
        lines = [f"Pipeline: {self.name}", "=" * 50]

        try:
            execution_order = self._get_execution_order()
        except ValueError as e:
            return f"Pipeline: {self.name}\nError: {e}"

        for i, task_name in enumerate(execution_order, 1):
            task = self.tasks[task_name]
            status_symbol = {
                TaskStatus.PENDING: "⏸",
                TaskStatus.RUNNING: "▶",
                TaskStatus.SUCCESS: "✓",
                TaskStatus.FAILED: "✗",
                TaskStatus.SKIPPED: "○",
            }.get(task.status, "?")

            deps = f" (depends on: {', '.join(task.depends_on)})" if task.depends_on else ""
            lines.append(f"{i}. {status_symbol} {task_name}{deps}")

        return "\n".join(lines)

"""
Duckling ORM — Synchronous Usage Example
==========================================

Duckling provides sync alternatives for every async method,
so you don't need asyncio if you prefer synchronous code.
"""

from duckling import Document, init_duckling_sync, Indexed
from duckling.operators import In, Between
from duckling.query import Count, Avg
from typing import Annotated, Optional
from duckling.fields import IndexSpec


class Task(Document):
    title: str
    description: Optional[str] = None
    priority: int = 0
    done: bool = False

    class Settings:
        table_name = "tasks"


def main():
    # Initialize synchronously
    init_duckling_sync(
        database=":memory:",
        document_models=[Task],
    )

    # Insert
    task = Task(title="Write docs", priority=3)
    task.insert_sync()
    print(f"Created: {task}")

    # Bulk insert
    Task.insert_many_sync([
        Task(title="Fix bug", priority=5),
        Task(title="Add tests", priority=4),
        Task(title="Deploy", priority=2, done=True),
    ])

    # Find
    high_priority = Task.find(Task.priority >= 4).sort("-priority").to_list_sync()
    print(f"\nHigh priority tasks:")
    for t in high_priority:
        print(f"  [{t.priority}] {t.title}")

    # Find one
    bug = Task.find_one_sync(Task.title == "Fix bug")
    print(f"\nFound: {bug.title} (priority={bug.priority})")

    # Update
    bug.done = True
    bug.save_sync()
    print(f"Marked '{bug.title}' as done")

    # Count
    pending = Task.find(Task.done == False).count_sync()
    print(f"\nPending tasks: {pending}")
    print(f"Total tasks: {Task.count_sync()}")

    # Delete
    bug.delete_sync()
    print(f"\nDeleted '{bug.title}'. Total now: {Task.count_sync()}")

    print("\n✓ Sync example complete!")


if __name__ == "__main__":
    main()

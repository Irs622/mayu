class Retry(Exception):
    """Raise inside a task to retry it, optionally after `countdown` seconds."""

    def __init__(self, countdown: float | None = None, exc: BaseException | None = None):
        super().__init__(countdown, exc)
        self.countdown = countdown
        self.exc = exc


class TaskFailed(Exception):
    """The task raised, was dead-lettered, or exhausted its retries."""

    def __init__(self, task_id: str, error: str, traceback: str):
        super().__init__(f"task {task_id} failed: {error}")
        self.task_id = task_id
        self.error = error
        self.traceback = traceback


class SoftTimeLimitExceeded(Exception):
    """Raised inside a task that ran past its soft_time_limit; catch it to clean up."""


class TimeLimitExceeded(Exception):
    """The task ran past its hard_time_limit and was abandoned (its thread may still be running)."""


class DiscardInstance(Exception):
    """Raise inside a task to say the pool instance it was given is broken; it is closed and rebuilt."""

    def __init__(self, message: str = "", *, pool: str | None = None):
        super().__init__(message)
        self.pool = pool  # None discards every instance the task holds

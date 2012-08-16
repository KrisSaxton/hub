""" Convenience decorators """
from __future__ import with_statement
from functools import wraps
import tasks

def task(*args, **kwargs):
    """ Decorator declaring the wrapped function to be a task.
    May be invoked as a simple, argument-less decorator (i.e. ``@task``) or
    with arguments customizing its behavior (e.g. ``@task(serialiser='json')``).
    """
    # Returns true if @task is passed arguments
    invoked = bool(not args or kwargs)

    # The class used to wrap the function, defaults to WrappedCallableTask
    task_class = kwargs.pop("task_class", tasks.WrappedCallableTask)

    if not invoked:
        func, args = args[0], ()

    def wrapper(func):
        # This will ultimately return a class with the wrapped function bound as the run method
        return task_class(func, *args, **kwargs)

    # If args were passed to the decorator then we need an additional wrapping layer to handle these args
    return wrapper if invoked else wrapper(func)

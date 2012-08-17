""" Convenience decorators """
from functools import wraps
import tasks

def task(*args, **kwargs):
    """ Decorator declaring the wrapped function to be a task.
    May be invoked as a simple, argument-less decorator (i.e. ``@task``) or
    with arguments customizing its behavior (e.g. ``@task(serialiser='json')``).
    """
    # Returns true if @task is passed arguments other than just the 
    #  plugin's main function (which is implied anyway)
    invoked = bool(not args or kwargs)

    # The class used to wrap the function, defaults to WrappedCallableTask
    task_class = kwargs.pop("task_class", tasks.WrappedCallableTask)

    if not invoked:
        func, args = args[0], ()

    def wrapper(func):
        # This will ultimately return a class with the wrapped function bound as the run method
        return task_class(func, *args, **kwargs)

    # If additional args were passed to the decorator then we return the 
    # wrapper function so the extra args can be unwrapped
    # Otherwise just return the results of the wrapper function directly
    if invoked:
        return wrapper
    else:
        return wrapper(func)

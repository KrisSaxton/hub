# Hub 

Hub is an orchestration engine written in python.

## Intro

Hub accepts 'jobs' which are comprised of one or more 'tasks'.

Jobs are managed by a subsystem of Hub call the 'dispatcher'.  The dispatcher breaks up the job into its component tasks and hands them off to 'workers'.  Workers handle the task execution and return the associated results to the dispatcher.

Communication between clients and the dispatcher and between the dispatcher and  its workers is implemented using message queues and AMQP.

## Dispatcher

The dispatcher is a long running (daemonised) python process.  The dispatcher receives new jobs from clients, decomposes jobs into their component tasks, determines the order of task execution and submits tasks to workers for execution.  The dispatcher receives task results from workers and handles any passing of returned data to subsequent tasks.  Finally, the dispatcher processes job management requests from clients such as requests for a job's current status.

## Worker

The worker is a long running (daemonised) python process which process incoming tasks submitted by the dispatcher.  The worker loads python modules from its filesystem and matches the incoming task with a corresponding python module.  The python module is then 'executed' with the arguments defined in the task and the results then returned to the dispatcher.

## Jobs

Jobs are written in JSON so they can be easily generated and read by both humans and software.

Below is an example of a simple job.

```
{ "name": "sum_product",
  "tasks":[
    {
        "args": [1, 2],
        "name": "add"
    },
    {
        "depends": ["add"],
        "args": ["_add.data", 2],
        "name": "multiply_this",
        "task_name": "multiply"
    }
  ],
  "output": ["_multiply_this.data"] }
```

Jobs have the following attributes:
 * Name: the Job's name
 * Output: where the output of the job is stored
 * Tasks: a list of the tasks which comprise the Job

Task dependancies and some of the strange syntax (e.g. leading underscores in for args are output) are explained in the Tasks section. 

## Tasks

'Task' describes both a python module on the worker used to expedite a task and the reference to that module as defined in the job's JSON definition (along with its arguments, dependancies and any other metadata).

Below is an example of a task expressed in JSON:

```
{
  "name": "multiply_this",
  "task_name": "multiply",
  "args": ["_add.data", 2],
  "depends": ["add"]
}
```

Tasks have the following attributes:
 * Name: the Task's name
 * Task_name: the module on the worker used to expedite the tasks.  Where this is missing, the 'name' attribute is used.
 * Args: the Task's arguments
 * Depends: a list of tasks upon which this task depends (i.e. tasks which must have completed successfully before this task can be started)

### Parameterisation

Data can be shared between tasks in the same job, allowing the data returned by one task to form all or part of the arguments of a subsequent task.  This is acheived using parameterisation and a couple of conventions.

 1. Task arguments that begin with an underscore (_) are recognised as parametrised arguments by the dispatcher.  These special arguments have the form '_<task>.data' where '<task>' is the name of the task whose output will provide the value to substitute the argument for.
 2. All tasks results are stored in a special 'data' attribute which can always be safely referred to by other tasks.

Example:

In the example task described above task 'multiply_this' depends on the task 'add'.  One of the arguments of the task uses the special syntax '_add.data'.  The dipatcher will recognise this and substitue this argument for results from the task 'add' once it has run.  For example if the task 'add' returns result '3', the arguments for the task 'multiply_this' after parameterisation will be [3,2].  

### Task modules

Task modules are python modules which carry out particular actions when run by a worker.  They are most easily written in python but techniques exist to support writing them in any language.  Hub uses a python decorator to do most of the heavy-lifting and make writing tasks very simple.

Below is an example of a simple Hub task module:

```python
from hub.lib.api import task

@task()
def add(arg1, arg2):
    return arg1 + arg2
```

The example above shows the minimum code required to implement a Hub task.

 1. from hub.lib.api import task <- this gives access to the @task decorator
 2.@task() <- this python decorator wraps your custom function and turns it into a Hub task
 3. def add(arg1, arg2): <- this is your custom function which must have the same name as your module
 4.     return arg1 + arg2: <- generally you want your function to return something

Task arguments and return values can be strings, integers, lists or dictionaries.  The @task decorator can also take arguments to modify the task's behaviour (see asynchronous tasks).

Task modules should be saved with a '.py' extension and placed in the 'tasks_dir' directory as defined in the worker configuration (see 'Configure the worker' below).

### Asynchronous tasks

### Task modules in other languages

TODO

## Installation

You can find the Hub source at: https://github.com/KrisSaxton/hub

### Dependencies

Hub requires the following:

 * Python (=>2.5, <3.0): http://python.org/
 * pika (=> 0.9.8): http://pika.readthedocs.org/en/latest/
 * An AMQP-compliant broker (e.g. RabbitMQ, ActiveMQ)

### Checkout code

Clone the Hub repository to somewhere on your local filesystem:

```
mkdir /Users/kris/dev/hub
cd /Users/kris/dev/hub
git clone https://github.com/KrisSaxton/hub.git hub
Cloning into 'hub'...
remote: Counting objects: 289, done.
remote: Compressing objects: 100% (198/198), done.
remote: Total 289 (delta 178), reused 199 (delta 88)
Receiving objects: 100% (289/289), 58.62 KiB, done.
Resolving deltas: 100% (178/178), done.
```

### Adjust PYTHONPATH

You need to ensure your python interpreter can find the Hub libraries.  Until Hub is properly packaged, the easiest way to do this is to adjust your PYTHONPATH to include the root of your Hub checkout.

```
export PYTHONPATH=$PYTHONPATH:/Users/kris/dev/hub
```

Test this worked:

```
$ python
Python 2.7.2 (default, Oct 11 2012, 20:14:37) 
[GCC 4.2.1 Compatible Apple Clang 4.0 (tags/Apple/clang-418.0.60)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> from hub.lib.api import task
>>> 
```

If you get an import error then check your cloned working directory and PYTHONPATH

### Configure the dispatcher

Configuring the dispatcher involves creating a configuration file to specify parameters such as the hostname/IP of the broker and the logging behaviour of the dispatcher.  An example is show below:

```
[HUB]
broker=localhost
pid_file=/Users/kris/dev/hub/hub/var/run/dispatcher.pid

[LOGGING]
log_file=/Users/kris/dev/hub/hub/var/log/dispatcher.log
log_level=debug
log_max_size=5242880
log_retain=5
```

### Configure the worker

Configuring the worker involves creating a configuration file to specify parameters such as the hostname/IP of the broker, logging behaviour, and the directory where the worker should scan for task modules ('tasks_dir').  An example is show below:

```
[HUB]
broker=localhost
pid_file=/Users/kris/dev/hub/hub/var/run/worker.pid
tasks_dir=/Users/kris/dev/hub-tasks

[LOGGING]
log_file=/Users/kris/dev/hub/hub/var/log/worker.log
log_level=debug
log_max_size=5242880
log_retain=5
```

### Start the dispatcher

Start the dispatcher, passing the location of the configuration file you just created as a parameter.

```
cd /Users/kris/dev/hub/hub
./bin/ctrl-hub-dispatcher -c ./etc/dispatcher.conf start
```

See ctrl-hub-dispatcher --help for further options

Verify the dispatcher started correctly by inpsecting the log file.  You should see something like:

```
INFO  Starting dispatcher, connecting to broker localhost...
INFO  Starting dispatcher, listening for jobs and results...
```

### Start the worker

Start the worker, passing the location of the configuration file you just created as a parameter.

```
cd /Users/kris/dev/hub/hub
./bin/ctrl-hub-worker -c ./etc/worker.conf start
```

See ctrl-hub-worker --help for further options

Verify the worker started correctly by inpsecting the log file.  You should see something like:

```
INFO  Starting worker, waiting for tasks...
```

### Submit a job

Copy the 'add' and 'multiply' tasks to the 'tasks_dir' on the worker (you will need to restart the worker whenever you add or modify a task module:

#### Add module

```
from hub.lib.api import task

@task()
def add(arg1, arg2):
    return arg1 + arg2
```

#### Multiply module

```
from hub.lib.api import task

@task
def multiply(arg1, arg2):
    return arg1 * arg2
```

####  Sum_product Job

```
{ "name": "sum_product",
  "tasks":[
    {
        "args": [1, 2],
        "name": "add"
    },    {        "depends": ["add"],
        "args": ["_add.data", 2],
        "name": "multiply_this",        "task_name": "multiply"    }  ],
  "output": ["_multiply_this.data"] }
```
Now submit the following job with the 'hub-client' utility:

``` 
cat /tmp/job.json | ./bin/hub-client -b localhost -C
Submitting job to broker localhost...
Submitting new job to queue
Successfully submitted job: 904adb59-9b8d-11e2-95fd-98fe943f85f6
```

In the example above the JSON job was piped into hub-client using cat.  For more hub-client options, such as reading JSON jobs from files and specify configuration options via config files, see hub-client --help.

### Query a job result

Query the job result using the hub-client utility and -S option, passing the job id returned by the dispatcher during the job submission as a parameter:

```
/bin/hub-client -b localhost -S 904adb59-9b8d-11e2-95fd-98fe943f85f6
Requesting status for job 904adb59-9b8d-11e2-95fd-98fe943f85f6
{"status": "SUCCESS", "tasks": [{"status": "SUCCESS", "name": "add", "args": [1, 2], "parent_id": "904adb59-9b8d-11e2-95fd-98fe943f85f6", "task_name": "", "data": 3, "id": "904c080c-9b8d-11e2-a8da-98fe943f85f6"}, {"status": "SUCCESS", "name": "multiply_this", "args": [3, 2], "parent_id": "904adb59-9b8d-11e2-95fd-98fe943f85f6", "depends": ["add"], "task_name": "multiply", "data": 6, "id": "904c0bf5-9b8d-11e2-99a7-98fe943f85f6"}], "name": "sum_product", "task_name": "", "output": [6], "id": "904adb59-9b8d-11e2-95fd-98fe943f85f6"}
```

The output is pretty raw (just a dictionary) but you should be able to determine that the job completed successfully and returned a value of 6.  That's a lot of work to produce something that can add 1 and 2 and then multiply the results by 3.

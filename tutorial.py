'''
An Airflow Python script is really just a configuration file
specifying the DAG's structure as code. The actual tasks defined
here will run in a different context of this script. Different 
tasks run on different workers at different points in time, which
means that this script cannot be used to cross communicate between
tasks. 
- For this purpose, there is a more advanced feature called XCom
'''


'''
An Airflow pipeline is just a Python script that happens to define
an Airflow DAG object.
- Start by importing the libraries that you need.
'''

from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

'''
We then need to create a DAG and some tasks, and we have the 
choice to explicitly pass a set of arguments to each tasks' 
constructor (which would become redundant), or we can define
a dictionary of default parameters that we can use when creating
tasks.

* You can easily define different sets of arguments that would 
serve different purposes. An example of that would be to have
different settings between a production and development environment
'''

# These args will get passed ont o each operator
# You can override them on a per-task basis during operator initialization
default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': days_ago(2),
	'emai': ['kevin.g.grimm1@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
	# 'queue': 'bash_queue',
	# 'pool': 'backfill',
	# 'priority_weight': 10,
	# 'end_date': datetime(2020, 6, 1),
	# 'wait_for_downstream': False,
	# 'dag': dag,
	# 'sla': timedelta(hours=2),
	# 'execution_timeout': timedelta(seconds=300),
	# 'on_failure_callback': some_function,
	# 'on_success_callback': some_other_function,
	# 'on_retry_callback': another_function,
	# 'sla_miss_callback': yet_another_function,
	# 'trigger_rule': 'all_success'
}


'''
You need a DAG object to nest our tasks into. Here we pass a
string that defines the dag_id, which serves as a unique identifier
for your DAG. We also pass the default argument dictionary that we 
just defined and define a schedule_interval of 1 day for the DAG.
'''
dag = DAG(
	'tutorial',
	default_args=default_args,
	description='A simple tutorial DAG',
	schedule_interval=timedelta(days=1),
)


'''
Tasks are generated when instantiating operator objects. An object
instantiated from an operator is called a constructor. The first
argument `task_id` acts as a unique identifier for the task.

* We pass a mix of operator specific arguments (bash_command) and 
an argument common to all operators (retries) inherited from the 
BaseOperator to the operator's constructor. This is simpler than 
passing every argument for every constructor call. Also, notice 
that in the second task we override the retries parameter with 3.

* The precedence rules for a task are as follows:
1) Explicitly passed arguments
2) Values that exist in the default_args dictionary
3) The operator's default value, if one exists

A task must include or inherit the arguments `task_id` and 
`owner`, otherwise Airflow will raise an exception
'''
# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
	task_id='print_date',
	bash_command='date',
	dag=dag,
)

t2 = BashOperator(
	task_id='sleep',
	depends_on_past=False,
	bash_command='sleep 5',
	retries=3,
	dag=dag,
)


'''
You can add documentation for DAG or each single task.
'''
dag.doc_md = __doc__

t1.doc_md == """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
~![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""


'''
Templating with Jinja
- Airflow leverages Jinja Templating and provides the pipeline author with a set of built-in parameters and macros. Airflow also
provides hooks for the pipeline author to define their own 
parameters, macros and templates.

- code logic is stored in {% %}
- parameters are referenced like {{ ds }}
- functions are called as {{ macros.ds_add(ds, 7)}}
- user-defined parameters are referenced as {{ params.my_param }}

* Take the time to understand how the parameter 
my_param makes it through to the template.

* Files can be passed to the `bash_command` argument,
like bash_command='templated_command.sh', where the 
file location is relative to the directory containing
the pipeline code, allowing for proper code highlighting
in files composed in different languages, and general
flexibility in structuring pipelines.
'''

templated_command = """
{% for i in range(5) %}
	echo "{{ ds }}"
	echo "{{ macros.ds_add(ds, 7)}}"
	echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
	task_id='templated',
	depends_on_past=False,
	bash_command=templated_command,
	params=('my_param': 'Parameter I passed in'),
	dag=dag,
)



'''
Setting up Dependencies
- We have tasks t1, t2 and t3 that do not depend on
each other.

- When executing your script, Airflow will raise
exceptions when it finds cycles in your DAG or when
a dependency is referred more than once.

Several ways to define dependencies between them:
'''

t1.set_downstream(t2)

# This means that t2 will depend on t1 running 
# successfully to run. It is equivalent to:

t2.set_upstream(t1)

# The bit shift operator can also be used to chain
# operations:

t1 >> t2

## Chaining multiple dependencies becomes concise
# with the bit shift operator

t1 >> t2 >> t3

# A list of tasks can also be set as dependencies. 
# These operations all have the same effect:

t1.set_downstream([t2, t3])
t1 >> [t2, t3]
[t2, t3] << t1 





















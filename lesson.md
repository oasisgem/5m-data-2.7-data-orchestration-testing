# Lesson

## Brief

### Preparation

We will be using the `elt` and `dagster` environments for this lesson. We will also be using google cloud (for which the account was created in the previous units) in this lesson.


### Lesson Overview

In this lesson, we will continue with orchestration from where we left off in the previous unit. We will use `Dagster` to orchestrate and schedule the `Meltano` and `Dbt` pipelines.

We will also dive into the concept of data testing, which is an important part of data quality management. We will use `Great Expectations`, an open-source library for data testing.

---


## Part 1 - Hands-on with Data Testing (Great Expectations)

### Background

Data testing is an important part of data quality management. It is the process of verifying that data satisfies the expected properties. It is also known as data validation, data quality assurance, data quality control, data quality assessment, etc.

Great Expectations is an open-source library for data testing. It is a Python library that helps you write, organize, evaluate, and share your data validation. It also provides a user interface for visualizing the results of the data tests. It can be used with any data source such as files, databases, data lakes, etc.

![great_expectations](assets/great_expectation.png)

### Create a Great Expectations Project (Data Context)

Please see the notebook [GX_lessons.ipynb](GX_lessons.ipynb), please attached `elt` environment to the notebook.


## Part 2 - Testing Dbt (dbt Expectation)

Back in unit 2.5, we configured some simple tests in dbt to check for _null values_, _uniqueness_ and _foreign key constraints_. We have copied the dbt project `liquor_sales` from unit 2.5 to this unit, located in the `extra` folder. You can find the tests in the `schema.yml` files in the `/models` directory.

Please update your GCP project ID in `profiles.yml`.

However, the built-in tests are limited in scope and functionality. We can expand on the tests using `dbt_utils`- a utility macros package for dbt and `dbt-expectations`- an extension package for dbt inspired by Great Expectations to write more comprehensive tests.

### Installing and Configuring `dbt_utils`

First, reactivate the conda environment.

```bash
conda activate elt
```

We will be using the same `elt` conda environment. The `liquor_sales` dbt project folder is under `extra` folder. Use the command `cd extra/liquor_sales` to navigate to the folder.

```bash
cd extra/liquor_sales
```

> To facilitate coding in class, all the following code are written but commented out. Please uncomment each section, please block them and use the key combination  (Mac:`cmd+/` or WSL:`ctrl+/`) to uncomment the configuration. 

Open the file`packages.yml`:

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.3
```

Run `dbt deps` to install the package. Refer to the [documentation](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) for supported tests.

```bash
dbt deps
```

Let's add some additional tests to the `fact_sales` model at the end of `models/schema.yml`:

```yml
- name: date
  tests:
    - dbt_utils.accepted_range:
        arguments:
            min_value: "PARSE_DATE('%F', '2012-01-01')"
            max_value: "CURRENT_DATE()"
```

Here, we are using `dbt_utils.accepted_range` to check if the `date` field is within the range of `2012-01-01` and `CURRENT_DATE()`.

We can also add the `dbt_utils.expression_is_true` test to check if the `sale_dollars` field is the product of `bottles_sold` and `state_bottle_retail` as below.

Note that the expression below involves multiple columns, and hence the test must be outside the `columns:` section so as to apply it as a model-level test (put it below `description:`)

```yml
tests:
  - dbt_utils.expression_is_true:
      arguments:
        expression: "ROUND(sale_dollars, 1) = TRUNC(bottles_sold * state_bottle_retail, 1)"
        severity: error # accepts only `error` and `warn` default is error
```

Here, we use `ROUND` to round the values to 1 decimal place and compare them.

> **Optional:**
> 
> The arguments `severity` allows you to treat a test as if it is an error or it is a warning in data drift.
> We can also add additional conditions such as `error_if` or `warn_if` for different situation.
> Please consult dbt documentation at https://docs.getdbt.com/reference/resource-configs/severity

Run the following code in order:

```bash
dbt debug
```

```bash
dbt deps
```

```bash
dbt snapshot
```

```bash
dbt run
```

```bash
dbt test
```

> The command `dbt build` combine `dbt snapshot`, `dbt run` and `dbt test`.

**Observe which tests pass and which fail.**

> 1. Run a SQL query to check which rows failed.
> 2. Run a SQL query to get the min and max values of `pack` and `bottle_volume_ml` in `liquor_sales_star.dim_item`.
> 3. Then, set the `min_value` and `max_value` in the `dbt_utils.accepted_range` test in `models/star/schema.yml` to the min and max values respectively.

### Installing and Configuring `dbt-expectations`

Add the following to `packages.yml`:

```yml
- package: metaplane/dbt_expectations
  version: 0.10.10
```

Run `dbt deps` to install the package. Refer to the [documentation](https://hub.getdbt.com/calogica/dbt_expectations/latest/) for supported tests.

```bash
dbt deps
```

Let's add some tests to check the column types in `fact_sales`:

```yml
- name: invoice_and_item_number
  tests:
    - dbt_expectations.expect_column_values_to_be_of_type:
        arguments:
            column_type: string
- name: date
  tests:
    - dbt_expectations.expect_column_values_to_be_of_type:
        arguments:
            column_type: date
```

> 1. Add type tests for all the columns in `fact_sales`, `dim_item` and `dim_store`.
> 2. Can you think of any other tests that we can add to the models?

--- 

## Extra - Hands-on with Orchestration I (Optional)
If you have not, create the conda environment based on the `dagster_environment.yml` file in the [environment](https://github.com/su-ntu-ctp/5m-data-2.1-intro-big-data-eng/tree/main/environments) folder. 

We will be using the `dagster` environment. Use the command `conda activate dagster` to activate the environment.

```bash
conda activate dagster
```

This will be covered in class, with demo on `extra/dagster_orchestration_dbt`.

This `dagster_orchestration_dbt` project demonstrates the following:
* A single Dagster project containing **three jobs**
* One of the jobs runs a **DBT pipeline** that executes: 
  * `dbt seed` 
  * `dbt run`
  * `dbt test`

```bash
cd extra/dagster_orchestration_dbt
```

Note that to run dagster successfully, you need to:
1. Create a `.env` file under the folder `extra/dagster_orchestration_dbt`. Add your Github token in the `.env` file similar to below:
    ```yaml
    GITHUB_TOKEN='github_pat_xxx'
    ```
2. In `extra/dagster_orchestration_dbt/profiles.yml`, enter your GCP project ID in `project:`
3. Since the DBT job inserts data in your BigQuery, you are reminded to do `gcloud auth application-default login` to authenticate yourself to your GCP project

```bash
gcloud auth application-default login
```

After the configuration above, we can run dagster using the command below:
```bash
dagster dev
```

Please check the following screenshot:

[Dagster Screenshot - Three Jobs](./assets/dagster1_1.png)

[Dagster Screenshot - pandas_job](./assets/dagster1_2.png)

[Dagster Screenshot - pipeline_one_job](./assets/dagster1_3.png)

[Dagster Screenshot - dbt_pipeline](./assets/dagster1_4.png)

## Extra - Hands-on with Orchestration II Using Dagster Subprocess (Optional)

In the previous unit, combining Meltano and Dbt, we have an end-to-end ELT (data ingestion and transformation) pipeline. However, we ran the pipelines manually. Now, we will use Dagster to orchestrate the pipelines and schedule them to run periodically.

### Background

We can orchestrate Meltano and Dbt pipelines using Dagster. By executing the commands from within Dagster, we get to take full advantage of its capabilities such as scheduling, dependency management, end-to-end testing, partitioning and more.

![dagster](assets/dagster_meltano.png)

### Create a Dagster Project

We will be using the meltano project we created in module 2.6. Make sure we are not in any subfolder. The pre-requisite of this exercise is that you need to complete the exercise to create a meltano project that extract data from Postgres and load into BigQuery. You also need to complete the exercise to create a HDB resale dbt project in lesson 2.6.

First, we will create a Dagster project and use it to orchestrate the Meltano pipelines.

> Warning: Don't forget we need to start a new folder under `5m-data-2.7-data-orchestration-testing`

```bash
dagster project scaffold --name meltano-orchestration
```

```bash
cd meltano-orchestration
```

### Using the Dagster Subprocess

Replace the content of `meltano-orchestration/meltano_orchestration/assets.py` with the following and also please change the path `cwd = '/path/to/your/folder/meltano_resale_in_lesson_2_6'`  and `cwd = '/path/to/your/folder/resale_flat_in_lesson_2_6'` with your respective path in lesson 2.6:

```python
# assets.py
from dagster import asset, AssetExecutionContext, PipesSubprocessClient

@asset
def pipeline_meltano(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    """
    Runs meltano using Dagster Pipes for better logging and observability.
    """
    return pipes_subprocess_client.run(
        command=["meltano", "run", "tap-postgres", "target-bigquery"],
        context=context,
        cwd = '/path/to/your/folder/meltano_resale_in_lesson_2_6'
    ).get_results()

@asset(deps=[pipeline_meltano])
def pipeline_dbt_run(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    """
    Runs dbt build using Dagster Pipes.
    """
    return pipes_subprocess_client.run(
        command=["dbt", "build"],
        context=context,
        cwd = '/path/to/your/folder/resale_flat_in_lesson_2_6'
    ).get_results()

```


Replace the content of `meltano-orchestration/meltano_orchestration/definitions.py` with the following:

```python
# definitions.py
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    PipesSubprocessClient, # 1. Import the client
)
import meltano_orchestration.assets as assets_module

all_assets = load_assets_from_modules([assets_module])

elt_job = define_asset_job(
    name="daily_elt_pipeline",
    selection="*",
    description="Meltano extraction → dbt transformation → data quality tests"
)

daily_schedule = ScheduleDefinition(
    job=elt_job,
    cron_schedule="0 2 * * *",
    name="daily_elt_schedule",
    description="Daily resale data pipeline"
)

defs = Definitions(
    assets=all_assets,
    jobs=[elt_job],
    schedules=[daily_schedule],
    # 2. Add the resource here
    resources={
        "pipes_subprocess_client": PipesSubprocessClient(),
    },
)
```

Then start the UI by running

```bash
dagster dev
```

Under `Jobs` click 'Materialized all'. We have just executed the `meltano run tap-github target-bigquery` command follow by `dbt build` from within Dagster.

![alt text](assets/dagster2_1.png)

## Extra - Hands-on with Orchestration III - Using Dagtser with dbt (Optional)

### Using Dbt with Dagster

We can also orchestrate Dbt with Dagster.

First, activate the conda environment.

```bash
conda activate dagster
```

make sure the `profiles.yml` in the `resale_flat` dbt project directory in Unit 2.6 is similar with the following format:

```yml
resale_flat:
  outputs:
    dev:
      dataset: resale_flat
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: #full-path-to-the-service-account-key-file
      location: US
      method: service-account
      priority: interactive
      project: #your-GCP-project-id
      threads: 1
      type: bigquery
  target: dev
```

Please use the above format in order to be compatible with Dagster. 

Then create a new Dagster project that points to the directory.

```bash
dagster-dbt project scaffold --project-name resale_flat_dagster --dbt-project-dir #full-path-to-the-resale-flat-dbt-project-directory
```

To run the dagster webserver:

```bash
cd resale_flat_dagster
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev
```

We can now trigger the Dbt pipeline from within Dagster by selecting the assets and clicking `Materialize all` under `Lineage`.

![dagster_lineage](assets/dagster3_2.png)

To set up the scheduler, follow the steps below.

In `resale_flat_dagster/resale_flat_dagster/schedules.py`, enter the following code and save:
```python
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import resale_flat_dbt_assets

materialize_dbt_job_schedule = build_schedule_from_dbt_selection(
    [resale_flat_dbt_assets],
    job_name="materialize_dbt_models",
    cron_schedule="0 0 * * *", # Enter your preferred cron schedule here.
    dbt_select="fqn:*",
)

# Access the job object created by the schedule
materialize_dbt_job = materialize_dbt_job_schedule.job

schedules = [materialize_dbt_job_schedule]
jobs = [materialize_dbt_job]
```

In `resale_flat_dagster/resale_flat_dagster/definitions.py`, enter the following code and save:
```python
from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import resale_flat_dbt_assets
from .project import resale_flat_project
from .schedules import schedules, jobs

defs = Definitions(
    assets=[resale_flat_dbt_assets],
    schedules=schedules,
    jobs=jobs,
    resources={
        "dbt": DbtCliResource(project_dir=resale_flat_project),
    },
)
```

Now in the Dagster UI, click `Reload definitions` and you will see the new schedule.

Toggle on the scheduler(see red box in screenshot below) and your job will run at the scheduled time.

![assets/dagster3_1.png](assets/dagster3_1.png)

# data_eng_poc
POC for data eng team

## Set up.
Run:
```
docker-compose up --build
```
that command should create all the containers that you need to eun the app.

## API doc.
When the containers are running, go to:
[Swagger doc](http://localhost:8000/docs)

## Order of information load.
1. load jobs data
2. load departments data
3. load employees data

## Employees endpoint notes:
You can use the parameter `bloks` to specify the number of records to process as kind of "batch" process, but actually, the app works using spark, so it just partition the data and paralellize it using this parameter. e.g. If you set bloks = 1000, a file of 10K records would be partitioned in 10 blocks of 1K each approx.

## Reports enpoints
In order to watch the reports with a fancy style, open them in your browser:
1. [department, job and quarter](http://localhost:8000/employees_job_2021)
2. [Employees hired by department](http://localhost:8000/hired_by_department_2021)

The reports will be available if you loaded the data first.
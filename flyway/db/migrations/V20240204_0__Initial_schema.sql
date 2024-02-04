-- Initial_schema.sql

CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    department VARCHAR(255) NOT NULL
);

CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    job VARCHAR(255) NOT NULL
);

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    department_id INTEGER,
    job_id INTEGER,
    FOREIGN KEY (department_id) REFERENCES departments (id),
    FOREIGN KEY (job_id) REFERENCES jobs (id)
);

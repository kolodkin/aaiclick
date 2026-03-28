# Examples Guidelines

## Project Structure

- Each example project should have a `report.py` file containing final report printout logic
- The `@job` function returns the report task — report is always the finalization task of the job, with all other tasks as its dependencies; `report.py` is only responsible for the printout

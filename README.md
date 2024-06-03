[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/l3elQelD)
# Spark Data Quality

Your assignment is to convert **two** of the SQL queries you wrote in Weeks 1-2 to SparkSQL *and* write unit integration tests on those queries.
- Create new PySpark jobs in `src/jobs` for these queries
- Create tests in `src/unit_tests` folder with fake input and expected output data

### Submission Guidelines

1. Write your PySpark jobs in `src/jobs/job_1.py` and `src/jobs/job_2.py` in the `src/jobs` folder. 

2. Write your tests in `src/unit_tests/test_spark_jobs.py` in the `src/unit_tests` folder. 

    >
    > **Please do not change the file paths or file names!**
    >

2. **Lint your code for readability.** This makes it easier for ChatGPT to review your work and for the TAs.

3. **Add comments to your code.** This helps ChatGPT provide a more accurate response and helps your reviewer understand your thought process.

4. Once you've completed the assignment, please review your code for errors, ensure it's well-commented, and confirm that no further (obvious) changes are needed before proceeding to the next step.

    > :warning: Once you open a PR, the assignment will be marked as submitted and your submission will be linked to **that specific PR** via GitHub classroom. The PR link will be shared with our TA team for review. 
    >
    > If you close that PR and/or open a new one, **your submission will still be linked to the original PR**. This is not something we can change, as it's an automated feature handled by GitHub classroom, and we cannot guarantee we will be able to accommodate individual cases due to the high volume of submissions we receive.
    > 

5. **Open a Pull Request (PR) to submit the assignment.** Please refrain from further work on the assignment or making additional changes to the code after the submission deadline, as this may complicate the review process.

    > :warning: Committing changes to the PR after the deadline can cause confusion about the readiness of your submission and delay the review process, so we ask that you avoid making additional changes to the code unless absolutely necessary or the reviewer requests changes.
    > 

6. Check the Github workflow for LLM-generated feedback for revision. You can update your PR as many times as you'd like before the submission deadline.

Grades are determined on a pass or fail basis. This is only used for certification purposes.

## Getting started

1. Create a virtual environment and activate it. For example:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install the dependencies into that virtual environment. For example:

```bash
python -m pip install -r requirements.txt
```

Please adjust as needed based on your computer's operating system.

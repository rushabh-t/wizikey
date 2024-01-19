## About
This is a take-home assignment for Junior Data Engineer role at Wizikey. As part of the solution there is a python file for a DAG in airflow, which fetches data from news-api and loads that data into google cloud storage.

## Packages used
I have used `newsapi-python` client for calling the API instead of the standard requests library. And for the file to be saved in the google cloud storage, I am using the `google-cloud-storage` package.

To install these packages, run this command:
```python
pip install -r requirements.txt
```

# EECS4415-Big Data Systems: Project 3 (30%)

**Title:** Real-Time Streaming Analytics with Apache Spark and Python   
**Out:** March 13, 2023, @ 9:00am EST   
**Due:** April 9, 2023, by 9:00pm EST

## Objective
This project involves designing and implementing a big data system that performs real-time streaming analytics for public repositories hosted on GitHub. The system runs a stream processing pipeline, where the live data stream to be analyzed is coming from GitHub API. An Apache Spark cluster processes the data stream. A web application receives the output from Spark and visualizes the analysis result. In this project, you will follow the requirements to write codes (Python, Bash, YAML scripts, Dockerfiles) that implement such a streaming process pipeline, which is a multi-container system based on Docker and Docker Compose.

## Implementation

* You may clone this repository and implement your solution in the `streaming` folder.
* The three lines in the *info.txt* file include information about you (first name, last name, and 9-digit student ID). Please update the *info.txt* file accordingly. For example, if your name is *Foo Bar* and your student ID is *123456789*. The *info.txt* file should be as follows:
    ```
    Foo
    Bar
    123456789
    ```
* All scripts are to be written using Python >= 3.7.0.
* You should use LF line terminators in your scripts.

**Hint:** You can find a [data streaming system](https://github.com/pacslab/big-data-systems-docker/blob/main/spark/app/nine-multiples/) with a similar architecture in Lab 7. It is highly recommended to test your solution in the VM provided in Lab 2. We will use the same environment to evaluate your submission.

## Submission
You need to zip your repository and submit as one zip file with the name of *project3.zip* on eClass by the due date. The directory structure in *project3.zip* should look like this:

```
EECS4415_Project_3/
├─ streaming/
│  ├─ docker-compose.yaml
│  ├─ spark_app.py
│  ├─ ...
├─ data.txt
├─ report.pdf
├─ info.txt
├─ QA.md
├─ README.md
├─ System_Architecture.png
├─ Webapp.png
├─ .gitignore
```
You should strictly follow the specified directory structure. Implementations that do not follow the correct directory structure will be marked as 0.

**Hint:** Please note that you should not include any output files and Spark checkpoint files in your submission.


## Requirements

GitHub is one of the most impactful git platforms, which is home to more than 73 million developers and over 200 million repositories, including at least 28 million public repositories as of November 2021. Analysis of software repositories hosted on GitHub has always played a key role in empirical software engineering. The trending analysis based on GitHub repositories and open-source codes, such as top programming languages, prevalent code smells, commit message conventions, and commit frequency distributions, can shed light on the evolution of open-source software and improve DevOps practices. 
Your task is to build a streaming pipeline that tracks specific programming languages on GitHub and reports their popularity and repository-related statistics in real-time. The following figure depicts the system architecture.

![System Architecture](System_Architecture.png)

The overall requirements are as follows:

1. Choose three programming languages (must include Python) from the list of programming languages in the appendix.

2. Develop a data source service using Python, which collects information about the most recently-pushed repositories that use any of the three programming languages as the primary coding language through GitHub API (Hint: see Appendix 2). The Python scripts should collect and push the new data to Spark at an interval of around 15 seconds, which means that every 15 seconds, the scripts feed Spark with the latest data. Also, the scripts should print the data being sent to Spark using the `print()` function.

3. Develop a Python script (streaming application) for Spark streaming (`spark_app.py` in the `streaming` folder). The application receives the streaming data, divides it into batches at an interval of 60 seconds (batch duration is 60 seconds), and performs the following four analysis tasks.
   1. Compute the total number of the collected repositories since the start of the streaming application for each of the three programming languages. Each repository should be counted only once.
   2. Compute the number of the collected repositories with changes pushed during the last 60 seconds. Each repository should be counted only once during a batch interval (60 seconds).
   3. Compute the average number of stars of all the collected repositories since the start of the streaming application for each of the three programming languages. Each repository counts towards the result only once.
   4. Find the top 10 most frequent words in the description of all the collected repositories since the start of the streaming application for each of the three programming languages. Each repository counts towards the result only once. You don't need to process the project description if it is empty (null). You should use the statement `re.sub('[^a-zA-Z ]', '', DESCRIPTION_STRING)` to strip the description before extracting words.
   5. Print the analysis results for each batch (like the streaming application presented in Lab 7).

4. Develop a web service listening on port 5000, which receives the analysis results from Spark and visualizes them in real-time (Hint: see Appendix 3). The web service runs a dashboard web application that includes:
    1. Three numbers that tell the total number of the collected repositories since the start of the streaming application for each of the three programming languages in real-time (requirement 3(i)). The numbers are updated every 60 seconds.
    2. A real-time line chart that shows the number of the recently-pushed repositories during each batch interval (60 seconds) for each of the three programming languages (requirement 3(ii)). The chart should be properly labeled, where the time is on the x-axis and the count is on the y-axis. The chart is updated every 60 seconds.
    3. A real-time bar plot that shows the average number of stars of all the collected repositories since the start of the streaming application for each of the three programming languages (requirement 3(iii)). The bar plot should be properly labeled and updated every 60 seconds.
    4. Three lists that contain the top 10 most frequent words in the description of all the collected repositories since the start of the streaming application and the number of occurrences of each word, sorted from the most frequent to the least, for each of the three programming languages in real-time (requirement 3(iv)). The lists are updated every 60 seconds.

5. Containerize all components of the data streaming pipeline (i.e., data source service, Spark cluster, and web service) with Docker and orchestrate containers using Docker Compose. The whole system should be up and running using the following commands:
    ```
    $ docker-compose up
    $ docker exec streaming_spark_1 spark-submit /streaming/spark_app.py
    ```
    The web application with the real-time charts should be listening on port 5000 of the Docker host.

Also, you should strictly follow the following technicalities/instructions:
* Implement your solution in the `streaming` folder.
* When defining the services in `docker-compose.yaml`, use the exact same service name as specified in the system architecture.
* Mount the `streaming` folder to `/streaming` for all containers using the `volumes` attribute in `docker-compose.yaml`.
* You may not change the name and the path of `spark_app.py`.
* The web application should map the port that the web app is listening on to port `5000` on the Docker host using the `ports` attribute in `docker-compose.yaml`.
* The data source service should read the GitHub personal access token (PAT) from the environment variable `TOKEN`, which is defined by the `environment` field in `docker-compose.yaml`. Do not hard-code your PAT anywhere in your solution. In your submission, you can define a dummy value in `docker-compose.yaml`, and we will replace it with our PAT when evaluating your solution.
* The architecture of your solution should be similar to the aforementioned system architecture. You can add a Redis service just like the streaming application presented in Lab 7. Thus, the whole system should comprise 4 or 5 services, which include a data source service, a Spark master service, a Spark worker service, a web application service, and an optional Redis service. You may not add any other services to the system.
* You can only use the `eecs4415/spark`, `eecs4415/hadoop`, `eecs4415/python`, and `redis` images or build your own images based on these images. The operations to pull other Docker images will be blocked. If you choose to build your own images, you need to specify the path to the build context using the `build` attribute in `docker-compose.yaml`.

## Data and Report 
You need to deploy your GitHub streaming analytics pipeline and keep it running for at least two hours on your machine. Then, you need to prepare a text file (`data.txt`), which consists of analysis results in the following format:
```
Application start timestamp in UTC:application end timestamp in UTC
Python:#collected repo (requirement 3(i)):average number of stars (requirement 3(iii))
PL2:#collected repo (requirement 3(i)):average number of stars (requirement 3(iii))
PL3:#collected repo (requirement 3(i)):average number of stars (requirement 3(iii))
Python:a comma-separated list with ten tuples, each of which contains a frequent word (top 10) and its number of occurrences (requirement 3(iv))
PL2:a comma-separated list with ten tuples, each of which contains a frequent word (top 10) and its number of occurrences (requirement 3(iv))
PL3:a comma-separated list with ten tuples, each of which contains a frequent word (top 10) and its number of occurrences (requirement 3(iv))
```
For example
```
1648771200:1648778400
Python:4096:6.4
Java:2048:12.8
Ruby:1024:1.6
Python:(You,1000),(need,999),(to,998),(deploy,997),(the,996),(streaming,995),(analysis,994),(system,993),(and,992),(keep,991)
Java:(it,1024),(running,512),(for,256),(at,128),(least,64),(two,32),(hours,16),(on,8),(your,4),(machine,2)
Ruby:(Then,100),(you,99),(need,98),(to,97),(prepare,96),(a,95),(text,94),(file,93),(which,92),(consists,91)
```
Also, you need to write a short report (1-2 pages) that briefly explains the system architecture and implementation of your solution. The report should at least cover:
* description of the system architecture
* how the services interact with each other
* how the Spark application processes the streaming data


## Evaluation
An automated judge will programmatically evaluate the majority part of your solution. TAs will assess the parts that cannot be programmatically judged. The grading criteria is as follows:

* Data Source Service (20%): the data source service has completed all the features and tasks specified in Requirement 2.
* Spark Application (45%): the Spark application has completed all the features and tasks specified in Requirement 3; in order to get these marks, your data source service must function properly.
* Webapp Service (15%): the webapp service has completed all the features and tasks specified in Requirement 4; in order to get these marks, your data source service and spark application must function properly.
* Containerization and Orchestration (10%): all services are properly containerized with Docker and orchestrated by Docker Compose following the requirements; the data streaming pipeline can run without any error using the commands in Requirement 5; in order to get these marks, your data source service, spark application, and webapp service must function properly.
* Data and Report (10%): the data submitted in `data.txt` is valid and authentic; the report correctly describes the system architecture and implementation of the solution.

## Q&As on the Project Description
Please refer to [QA.md](QA.md).


## Appendix

### 1. List of Programming Languages
```
Python
Java
JavaScript
TypeScript
CPP
CSharp
C
PHP
Go
Ruby
```

### 2. GitHub API
The official documentation for GitHub API can be accessed [here](https://docs.github.com/en/rest). This project mainly involves retrieving data through the [GitHub Search API](https://docs.github.com/en/rest/reference/search). You can use the following endpoint to find the most recently-pushed repositories that use a specific programming language as the primary coding language.
```
https://api.github.com/search/repositories?q=+language:{$Programming Language}&sort=updated&order=desc&per_page=50
```
For example, the following endpoint is for searching recently-pushed repositories that use C# as the primary programming language.
```
https://api.github.com/search/repositories?q=+language:CSharp&sort=updated&order=desc&per_page=50
```
The search API returns a JSON file that contains a list (`items`), where each element is a dictionary composed of many key-value pairs containing information about a repository. The fields `full_name` records the unique name of the repository, `pushed_at` contains the date and time of the last push to the repository, `stargazers_count` records the number of stars of the repository, and `description` contains the description of the repository. The field `description` is null when the description of the repository is empty. You can use the built-in [`json`](https://docs.python.org/3/library/json.html) package to process JSON files.

GitHub Search API has a rate limit of 30 requests per minute for authenticated requests. The rate limit allows you to make up to 10 requests per minute for unauthenticated requests. Thus, you need a GitHub personal access token (PAT) to make requests in the data source service. You can follow [these steps](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) to get a PAT.

In order to search repositories, you need to send HTTP GET requests to GitHub API. You can easily do so using the [`requests`](https://docs.python-requests.org/en/latest/) package. The following code block sends an HTTP GET request to GitHub Search API and obtains the recently-pushed repositories that use C# as the primary programming language.
```
import requests

url = 'https://api.github.com/search/repositories?q=+language:CSharp&sort=updated&order=desc&per_page=50'
res = requests.get(url, headers={"Authorization": "token ghp_REPLACE_THIS_DUMMY_VALUE_WITH_YOUR_PAT"})
result_json = res.json()
```
Please note that the scripts running in the data source service container need to get PAT from the environment variable `TOKEN`. Do not hard-code your PAT in your solution. In your submission, you can define a dummy value in `docker-compose.yaml`, and we will replace it with our PAT when evaluating your solution. The following code block shows how to get the value of an environment variable in Python.
```
import os

token = os.getenv('TOKEN')
```

### 3. Web Application
The web application visualizes the analysis results in real-time, which doesn't need to be fancy. A simple dashboard such as shown in the figure below would suffice. You can easily create a web application using web frameworks, such as [Flask](https://flask.palletsprojects.com/en/2.0.x/) and [Django](https://www.djangoproject.com/). Also, a simple [Flask-based dashboard](https://github.com/pacslab/big-data-systems-docker/blob/main/spark/app/nine-multiples/webapp/flask_app.py) is presented in Lab 7. You can modify its source code to implement the web application for this project.

<img src="Webapp.png" alt="Webapp Screenshot" width="500"/>



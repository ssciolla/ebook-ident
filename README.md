## ebook-ident

### Overview

This Python application helps to identify e-books (or any book) by taking existing bibliographic records, searching for those titles using the WorldCat Search API, and then transforming and analyzing the results to produce a set of ISBNs (and their associated formats, when possible). This work was embarked upon initially by ssciolla as part of a freelance project for Michigan Publishing Services, part of the University of Michigan Library.

### Application Workflow

The application currently gathers, analyzes, and transforms data using the following workflow:

1. Prepare input book records for use.
2. For each book,
    1. Search for title and author in WorldCat, using the Bibliographic Resource API endpoint.
    2. Parse returned MARCXML WorldCat records into a flat tabular data structure.
    3. Check whether the title and publisher of each WorldCat record matches the original book record. Strings are normalized and then analyzed for differences using the Levenshtein Distance algorithm.
    4. For matching records, determine their format and eliminate duplicate pairs of ISBNs and format types.
    5. Accumulate the resulting new records.
3. Output new ISBN/Format records for all books as a CSV.
4. Output original book records that did not match with records with ISBNs into another CSV.
5. Log summary report.

**Note**: The workflow is currently in flux, and more work needs to be done to properly generate usable output.

### Installation

#### Prerequisities

Currently, the recommended way to set up the project and install needed dependencies is by using `virtualenv`. Before following the instructions below, you may need to install some or all of the following:

* [Python 3.8](https://www.python.org/)
* [`virtualenv`](https://pypi.org/project/virtualenv/)

#### Instructions

To set up the project, perform these steps in order after using your command line tool of choice to navigate to the directory where you would like to keep all project-related files.

1. Clone the repository and navigate into it.
    ```
    git clone git@github.com:ssciolla/ebook-ident.git      # SSH
    git clone https://github.com/ssciolla/ebook-ident.git  # HTTPS
    
    cd ebook-ident
    ```

2. Create a virtual environment.
    ```
    virtualenv venv
    ```

3. Activate the virtual environment.
    ```
    source venv/bin/activate  # Mac OS
    venv\Scripts\activate     # Windows
    ```

    **Note**: to deactivate after installation and/or use, simply issue the command `deactivate`.

4. Install the dependencies in `requirements.txt`
    ```
    pip install -r requirements.txt
    ```

    **Note for Windows users:** the `python-Levenshtein` package may require additional software to build. You can satisfy these dependencies by installing [Microsoft Visual Studio](https://visualstudio.microsoft.com/visual-cpp-build-tools/), ensuring that you install the C++ language development tools. 
    
    Alternatively, you can download a pre-compiled binary from the [Unofficial Windows Binaries for Python Extension Packages](https://www.lfd.uci.edu/~gohlke/pythonlibs/#python-levenshtein). Note the maintainer warns that these binaries should only be used for testing. To install the `.whl` file, from within the activated virtual environment, run the following command, replacing `{absolute_path}` with the absolute path to the download.
    ```
    pip install {absolute_path}
    ```

### Configuration

The application running successfully depends on two input files being provided: a configuration file called `env.json` and a CSV or Excel file with an arbitrary name. For the former, a template file called `env_blank.json` has been provided within the `config` directory. To set up these files and prepare the application for execution, do the following -- using a terminal, text editor, or file utility as necessary.

1. Place the CSV or Excel file containing the book records to be searched for and analyzed within the `data` directory.

    **Note**: the application currently expects the tabular data file to have certain column names, which are then crosswalked to what is used by the algorithm; look at the `input_to_identify.json` crosswalk file in the `config` directory for details on the most current setup.

2. Copy the template file, renaming it `env.json`.
    ```
    cp config/env_blank.json config/env.json
    ```

3. Replace the default or empty values in the nested structure with a value of the same data type. The table below provides detail on the meaning of each key-value pair and its accepted values.

    **Key** | **Description**
    ----- | -----
    `LOG_LEVEL` | The minimum level for log messages that will appear in output. `INFO` or `DEBUG` is recommended for most use cases; see [Python's logging module](https://docs.python.org/3/library/logging.html).
    `WC_SEARCH_API_KEY` in the `WORLDCAT` object | The WS Key for authenticating to the WorldCat Search API; see [WorldCat Search API](https://www.oclc.org/developer/develop/web-services/worldcat-search-api.en.html).
    `BIB_RESOURCE_BASE_URL` in the `WORLDCAT` object | The base URL specifying the Bibliographic Resource endpoint of the REST API; as of March 2020, the default should be correct.
    `DB_CACHE_PATH` | An array of strings specifying each step in a path to where the database cache will be written; the default is recommended.
    `BOOKS_CSV_PATH` | An array of strings specifying each step in a path to where the input CSV or Excel file was placed in Step #1; the first string should be `"data"`, and the second should be the name of the input file.
    `ON` in the `TEST_MODE` object | A boolean (either `true` or `false`) specifying whether the application should only process a limited number of the input book records.
    `NUM_RECORDS` in the `TEST_MODE` object | An integer specifying the number of book records from the input tabular data to process if the `ON` value is `true`.

### Usage

#### Running the application

Once the steps under **Installation** and **Configuration** have been completed, the application can be used. At the project's root level and with the virtual environment activated, enter the following command:

```
python identify.py
```

**Note**: if you are making changes to the code or otherwise tuning it, make use of the `LOG_LEVEL` and `TEST_MODE` options described above to see increased output or limit the number of records processed.

#### Outputs

Currently, the project has two primary outputs. These are likely to change if work proceeds on this project in the future.

    1. `matched_manifests.csv`: A CSV containing the ISBN and Format of WorldCat records that matched with one of the input book records. The records also contain the ID and title from the corresponding original book record, as well as a `Source` column indicating these records came from WorldCat.
    2. `no_isbn_matches.csv`: a CSV containing the original records of books that the workflow did not produce any results for, likely because WorldCat returned no results for the title, no results passed the matching algorithm, or no results had ISBNs. Future work might focus on determining why these failed and deciding whether the algorithm needs to be tuned or expanded upon to collect more data.

#### Using and re-setting the cache

In order to use the WorldCat Search API responsibly, the application includes a caching implementation that stores the request URLs and corresponding XML responses (along with a timestamp) in the `request` table of an SQLite database. The database will automatically be generated when the application is initially executed. If the default configuration options are maintained, the file-based database will appear in the `data` directory with the name `db_cache.db`. 

If the application crashes for some reason during execution, when it is restarted, it will use cached data for requests that it has already made. If the data in the database becomes stale or needs to be invalidated, the database can be reset easily by issuing the following command within the activated virtual environment:
```
python db_cache.py
```

The `db_cache.db` database can be connected to using a number of free database utility applications, including the [DB Browser for SQLite](https://sqlitebrowser.org/) or [DBeaver](https://dbeaver.io/).

### Resources

The following resources were consulted while working on this project:

* WorldCat Search API and MARCXML
    * https://www.oclc.org/developer/develop/web-services/worldcat-search-api/bibliographic-resource.en.html
    * https://www.loc.gov/marc/bibliographic/
    * https://www.oclc.org/bibformats/en.html

* Fuzzy String Comparison using Levenshtein Distance
    * https://www.datacamp.com/community/tutorials/fuzzy-string-python
    * https://pypi.org/project/fuzzywuzzy/
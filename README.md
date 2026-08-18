# Cloud-Based Distributed Data Processing Service

## Architecture

```
User (browser)
   |
   v
Streamlit App (app.py) — hosted FREE on Streamlit Community Cloud
   |
   | user uploads file -> saved temporarily -> PySpark (local mode)
   | runs inside the SAME cloud container as the app
   |
   v
Descriptive stats + 4 MLlib jobs computed
   |
   v
Results displayed in-browser + downloadable as JSON
```

Separately, in **Google Colab** (a different free cloud environment), we run
`colab_scalability_experiment.py` to produce the required
1/2/4/8-"machine" timing table for the report.

Why two environments? Streamlit Cloud's free container isn't guaranteed
consistent CPU allocation between runs (bad for a *timing* experiment), while
Colab gives you a dedicated VM you fully control for the duration of the
session — better for repeatable measurements.

---

## Part A — Get the app running locally first (test before deploying)

### 1. Install Java (PySpark needs it)

Download and install **Temurin JDK 17** (free): https://adoptium.net/temurin/releases/

After installing, open a **new** Command Prompt and check:
```
java -version
```

### 2. Create a project folder and a virtual environment (use Python 3.11)

```
mkdir spark-cloud-project
cd spark-cloud-project
py -3.11 -m venv venv
venv\Scripts\activate
```

Your prompt should now start with `(venv)`.

### 3. Put these files in the folder
- `app.py`
- `spark_processing.py`
- `requirements.txt`
- `packages.txt`
- `colab_scalability_experiment.py`

### 4. Install dependencies

```
pip install -r requirements.txt
```

### 5. Run the app locally

```
streamlit run app.py
```

A browser tab should open automatically (usually `http://localhost:8501`).
Upload a CSV and click "Run Spark Job" to make sure everything works before
deploying.

---

## Part B — Deploy the app to the cloud for free (Streamlit Community Cloud)

No credit card required — just a free GitHub account.

### 1. Create a GitHub account (if you don't have one)
https://github.com/join

### 2. Create a new repository
- Go to https://github.com/new
- Name it e.g. `cloud-spark-data-processing`
- Set it to **Public** (needed for Streamlit Cloud's free tier)
- Click "Create repository"

### 3. Upload your project files to the repo
Easiest way (no git command line needed):
- On the repo page, click **"Add file" -> "Upload files"**
- Drag in: `app.py`, `spark_processing.py`, `requirements.txt`, `packages.txt`
- Commit the changes

### 4. Deploy on Streamlit Community Cloud
- Go to https://share.streamlit.io
- Sign in with your GitHub account
- Click **"New app"**
- Pick your repository, branch (`main`), and main file (`app.py`)
- Click **"Deploy"**

Wait a few minutes for the build. You'll get a public URL like:
```
https://your-app-name.streamlit.app
```

**This is the link you put in the report** for "the program on the cloud."

---

## Part C — Run the scalability experiment (1/2/4/8) in Google Colab

### 1. Get a large dataset
Go to https://archive.ics.uci.edu/ and pick a dataset with a good number of
rows (tens of thousands+) so the timing differences are meaningful. Download
it as CSV.

### 2. Open Google Colab
https://colab.research.google.com -> "New notebook" (just needs a free
Google/Gmail account, no card).

### 3. In the first cell, install PySpark
```python
!pip install pyspark -q
```

### 4. Upload your files
On the left sidebar, click the folder icon -> upload icon, and upload:
- your dataset CSV
- `spark_processing.py`

### 5. Paste and run `colab_scalability_experiment.py`
Edit `INPUT_PATH` at the top to match your uploaded file's name (e.g.
`/content/my_dataset.csv`), then run the cell.

### 6. Copy the printed table into your report
It will print something like:

```
Machines  Time (s)   Speedup   Efficiency
1         12.4       1.0       1.0
2         7.1        1.75      0.87
4         4.3        2.88      0.72
8         3.9        3.18      0.40
```

Fill these into the table already in the report template (Experiments and
Evaluation section), and use the note at the bottom of
`colab_scalability_experiment.py` to help write the scalability discussion.

---

## Checklist against the assignment requirements

- [x] User uploads dataset via UI, options selected, results viewed/downloaded
- [x] Upload validated, processed with Spark, results returned
- [x] >=4 descriptive statistics (rows, columns, dtypes, min/max/mean, unique counts, null %)
- [x] >=4 MLlib jobs: Linear Regression, KMeans, FPGrowth, time-series aggregation
- [x] Results displayed on screen and written to storage (JSON download)
- [ ] YOU DO: run `colab_scalability_experiment.py` on a large UCI dataset, fill in the table
- [ ] YOU DO: push code to a public GitHub repo
- [ ] YOU DO: deploy on Streamlit Community Cloud, get the public URL
- [ ] YOU DO: record a 5-7 min demo video of the deployed app
- [ ] YOU DO: fill in the report template with links + scalability discussion

# CP422 Study Lab

A study app for CP422 (Programming for Big Data). It started as midterm prep and now targets the final quiz (15%) on Friday, November 27, 2026, which covers every lecture from Sept 11 to Nov 25: Hadoop, MapReduce, Hadoop Streaming, Spark, Spark SQL, Spark Streaming, Machine Learning with Spark MLlib, and Pig Latin. It's built from the lecture slides, class notes, both posted review sheets and the lab notebooks.

## Open it

Open `index.html` in a browser. No install is needed. Your progress is saved in that browser.

## What's inside

| Section | What it does |
| --- | --- |
| Plan | Countdown, quiz blueprint (10 MCQ 20%, 10 T/F 20%, 2 coding 40%, 1 short answer 20%), what the prof flagged in class, a two-day plan, and progress by topic |
| Notes | Cheat sheet per topic, compare tables (MapReduce vs Spark, YARN daemons, HDFS daemons…), and the slide patterns to reuse in code answers. Searchable. |
| Quiz | 171 multiple-choice and 110 true/false questions across 9 topics, with explanations and slide references. Includes all MCQs from both review sheets. Missed questions go to a redo pile. |
| Code Lab | 83 exercises in 7 tracks (Hadoop Streaming, Java MapReduce, PySpark, Spark SQL & DataFrames, Pig Latin, Spark MLlib, Spark Streaming), each in 5 levels: read → fill in → arrange lines → write with hints → exam-style. Exam-style questions include a data-flow trace and a marking rubric. |
| Short answer | 29 compare / explain-why questions with key points and model answers, including all of review sheet 2 |
| Mock exam | A random 50-minute paper in the real format, with auto-marked MCQ/T/F and rubric marking for code and short answer |

All model answers follow the patterns from the lecture slides and labs, because the prof said he only marks his way as correct.

When the app runs as a claude.ai artifact, Claude can grade written code and short answers against the rubric and answer questions about any item. Opened as a local file, it uses self-marking with the rubrics instead.

## Editing content

- `src/content-theory.js`: notes, compare tables, MCQ, true/false, short answer
- `src/content-code.js`: Code Lab exercises (fill-in blanks are written `⟦answer|alternative⟧`)
- `src/content-final.js`: everything added for the final quiz (Spark Streaming, MLlib, Pig Latin, review sheet 2, new pre-midterm questions)
- `src/app.js`, `src/page.html`: the app itself

After editing, run `python3 build.py`. It rebuilds `index.html`, plus `dist/artifact.html` for publishing.

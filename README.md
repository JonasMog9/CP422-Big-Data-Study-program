# CP422 Midterm Lab

A study app for the CP422 (Programming for Big Data) midterm on Friday, October 23, 2026. It's built from the lecture slides, class notes, posted review questions and lab notebooks, and it covers everything up to the start of Spark Streaming (Spark I&II deck, slide 24).

## Open it

Open `index.html` in a browser. No install is needed. Your progress is saved in that browser.

## What's inside

| Section | What it does |
| --- | --- |
| Plan | Countdown, exam blueprint (10 MCQ 20%, 10 T/F 20%, 2 coding 40%, 1 short answer 20%), what the prof flagged in class, a two-day plan, and progress by topic |
| Notes | Cheat sheet per topic, compare tables (MapReduce vs Spark, YARN daemons, HDFS daemons…), and the slide patterns to reuse in code answers. Searchable. |
| Quiz | 90 multiple-choice and 63 true/false questions with explanations and slide references. Includes the prof's 10 review MCQs. Missed questions go to a redo pile. |
| Code Lab | 50 exercises in 4 tracks (Hadoop Streaming, Java MapReduce, PySpark, Spark SQL & DataFrames), each in 5 levels: read → fill in → arrange lines → write with hints → exam-style. Exam-style questions include a data-flow trace and a marking rubric. |
| Short answer | 16 compare / explain-why questions with key points and model answers |
| Mock exam | A random 50-minute paper in the real format, with auto-marked MCQ/T/F and rubric marking for code and short answer |

All model answers follow the patterns from the lecture slides and labs, because the prof said he only marks his way as correct.

When the app runs as a claude.ai artifact, Claude can grade written code and short answers against the rubric and answer questions about any item. Opened as a local file, it uses self-marking with the rubrics instead.

## Editing content

- `src/content-theory.js`: notes, compare tables, MCQ, true/false, short answer
- `src/content-code.js`: Code Lab exercises (fill-in blanks are written `⟦answer|alternative⟧`)
- `src/app.js`, `src/page.html`: the app itself

After editing, run `python3 build.py`. It rebuilds `index.html`, plus `dist/artifact.html` for publishing.

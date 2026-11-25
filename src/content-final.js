/* CP422 Study Lab: material added for the Nov 27 quiz ("the final", 15%).
 * Covers every lecture from Sept 11 to Nov 25: everything in content-theory.js and
 * content-code.js, plus Spark Streaming (Spark deck slides 24-45), Machine Learning /
 * Distributed Data Science (ML deck) and Pig Latin (Pig deck). Also adds review sheet 2
 * and a new batch of questions on the pre-midterm material.
 * Sources: ML = "Machine Learning - Distributed Data Science" deck, Pig = "Pig Latin I&II"
 * deck, Spark = "Spark I&II" deck, Review 2 = second review sheet, Lab = ML-Spark notebook. */
(function () {
const R = String.raw;

CP.exam = {
  name: "CP422 Quiz",
  title: "CP422 Final Quiz",
  start: "2026-11-27T10:30:00-05:00",
  end: "2026-11-27T11:20:00-05:00",
  dateLabel: "Fri Nov 27 · 10:30-11:20",
  where: "In person, written on MyLS",
  minutes: 50,
  weight: "15% of your grade",
  parts: CP.exam.parts,
  scope: "Every lecture from Sept 11 up to Nov 25: everything on the midterm, plus Spark Streaming, Machine Learning with Spark and Pig Latin."
};

CP.topics.push(
  { id: "sparkstream", name: "Spark Streaming", deck: "Spark deck slides 24-45", isNew: true },
  { id: "ml", name: "Machine Learning (MLlib)", deck: "ML / Distributed Data Science deck", isNew: true },
  { id: "pig", name: "Pig Latin", deck: "Pig Latin I&II deck", isNew: true }
);

CP.flags = [
  { when: "Oct 28", text: "His email: \"These slides are a rich source for your quiz, especially the Spark Streaming architecture part.\"", go: { view: "notes", topic: "sparkstream" } },
  { when: "Oct 30", text: "Slide 38 (stateless vs stateful transformations) is probably a question on the quiz.", go: { view: "short", id: "sa2-stateless-stateful" } },
  { when: "Nov 6", text: "ML slide 8 (transformer, estimator, pipeline) will be a true/false question.", go: { view: "quiz", topic: "ml" } },
  { when: "Nov 11", text: "ML slide 16 (one-hot encoding pipeline) might come up as a question.", go: { view: "code", id: "ml-ohe-fill" } },
  { when: "Nov 18", text: "ML slide 44 code (the dating recommender with ALS) is on the quiz.", go: { view: "code", id: "ml-ex-als" } },
  { when: "Nov 18", text: "Your notes say \"quiz is up to slide 17\". If that's the Pig deck, slide 17 is DESCRIBE / EXPLAIN / ILLUSTRATE, but review sheet 2 also asks about JOIN (slide 21), so the app covers the whole Pig deck.", go: { view: "notes", topic: "pig" } },
  ...CP.flags.filter(f => f.when !== "Oct 9")
];

CP.plan = [
  { id: "f1", day: "Wed night", text: "Read Notes for Spark Streaming, ML and Pig Latin. Then quiz yourself on just those three topics." },
  { id: "f2", day: "Wed night", text: "Review sheet 2: all 15 MCQs, then write 3 of its short answers." },
  { id: "f3", day: "Thu morning", text: "Code Lab: MLlib levels 1-3 (slides 8, 16, 34-35) and Pig Latin levels 1-3." },
  { id: "f4", day: "Thu morning", text: "Code Lab: Spark Streaming levels 1-4, especially windows and updateStateByKey." },
  { id: "f5", day: "Thu afternoon", text: "Exam-style code: the ALS recommender (slide 44), the ML pipeline, a Pig script, plus one Java MapReduce from the midterm set." },
  { id: "f6", day: "Thu afternoon", text: "One quiz of 40 mixed questions across all 9 topics, including the new pre-midterm ones." },
  { id: "f7", day: "Thu evening", text: "Full 50-minute mock exam. Then redo everything in the mistakes pile." },
  { id: "f8", day: "Fri 8-10am", text: "Compare tables, the prof's flags list and your mistakes pile. No new material." }
];

/* ---------------- Notes ---------------- */
CP.notes.sparkstream = [
  { h: "What Spark Streaming is", items: [
    "An extension of Spark for **real-time** processing of unbounded data streams. Sources: **Kafka, Flume, HDFS, TCP sockets**.",
    "Inherits fault tolerance from Spark's RDD model. Use cases: real-time analytics (clickstream), fraud / anomaly detection, dynamic pricing, real-time recommendations, log monitoring, social media trends.",
    "Not the same as Hadoop Streaming, which is batch MapReduce over Unix stdin/stdout."
  ]},
  { h: "Architecture (prof: rich source for the quiz)", items: [
    "**Micro-batch** architecture: the stream is treated as a continuous series of small batch computations.",
    "Data arriving during each **batch interval** is grouped into a batch; a new batch starts at the beginning of each interval. The batch interval is typically **500 ms to several seconds**, set by the developer.",
    "Each batch becomes an **RDD** and is processed with normal Spark jobs; results are pushed to external systems in batches.",
    "The abstraction is the **DStream** (discretized stream): a **sequence of RDDs**, one per time slice. A transformation on a DStream is applied to every RDD in it.",
    "The **StreamingContext** in the driver is the entry point; it also creates an underlying SparkContext. It launches **receivers** (tasks inside executors) for each input source.",
    "Receivers store incoming data in executor memory like cached RDDs and **replicate it to another executor by default** (two nodes), so a single worker failure is tolerated.",
    "The StreamingContext periodically runs Spark jobs to process the data and combine it with RDDs from earlier time steps.",
    "Recomputing from lineage alone could take a long time, so Spark Streaming also **checkpoints** state to reliable storage (HDFS, S3), typically every **5-10 batches**. Recovery only goes back to the last checkpoint."
  ]},
  { h: "Writing a streaming job (slide 26)", items: [
    "Create a StreamingContext with a batch interval: Scala `val ssc = new StreamingContext(conf, Seconds(1))` (PySpark: `ssc = StreamingContext(sc, 1)`).",
    "Create a DStream: `ssc.socketTextStream(\"localhost\", 7777)`. Transform it: `lines.filter(_.contains(\"error\"))`. Output: `errorLines.print()`.",
    "This only **sets up** the computation. Nothing runs until `ssc.start()`. Processing happens in a separate thread, so call `ssc.awaitTermination()` to keep the application from exiting."
  ]},
  { h: "Stateless vs stateful (slide 38: likely a question)", items: [
    "**Stateless**: each batch is processed independently of earlier batches. The usual RDD transformations: `map()`, `filter()`, `reduceByKey()`. `reduceByKey` on a DStream reduces **within each time step, not across** time steps.",
    "Stateless transformations can combine several DStreams, again within each time step: `join()`, `cogroup()`, `leftOuterJoin()`.",
    "Slide 33 example: parse Apache log lines → map each IP to (ip, 1) → reduceByKey to count each IP **per batch**.",
    "**Stateful**: uses data or intermediate results from previous batches. Two kinds: **windowed operations** and **updateStateByKey()** (or mapWithState).",
    "Stateful transformations **require checkpointing**: `ssc.checkpoint(\"hdfs://...\")`."
  ]},
  { h: "Windowed transformations (slides 35-36)", items: [
    "Compute results over a longer period than the batch interval by combining several batches.",
    "Two parameters: **window duration** and **sliding duration**. Both must be **multiples of the batch interval**.",
    "Window duration controls how many batches are considered: windowDuration / batchInterval. Batch 10 s, window 30 s → the last **3 batches**.",
    "Sliding duration (defaults to the batch interval) controls **how often** the result is computed. Batch 10 s, slide 20 s → compute on every second batch.",
    "Simplest operation: `window()`. Others: `reduceByWindow()`, `reduceByKeyAndWindow()`, `countByWindow()`."
  ]},
  { h: "updateStateByKey (slide 37)", items: [
    "Keeps state for each key across batches, e.g. the last 10 pages each user visited, or a running count.",
    "Given a DStream of (key, event) pairs it builds a DStream of (key, state) pairs, using `update(events, oldState) → newState`.",
    "`events`: events that arrived for this key in the current batch (may be empty). `oldState`: the previous state, in an Option (may be missing). Return an empty Option to **delete** the state.",
    "Review sheet 2: its main advantage is **stateful computations across multiple time windows**. It needs checkpointing."
  ]},
  { h: "24/7 operation and checkpointing (slides 39-40)", items: [
    "Exactly-once semantics: as long as the input is stored reliably, the result is as if no node had failed, even if workers or the driver fail.",
    "Checkpointing is the main fault-tolerance mechanism. Two purposes: (1) **limit the state that must be recomputed** on failure (how far back lineage must go); (2) **fault tolerance for the driver**: a restarted driver recovers from the checkpoint and carries on.",
    "Review sheet 2: **metadata checkpointing** saves the streaming context and configuration; **data checkpointing** saves the RDDs holding intermediate state.",
    "Driver fault tolerance: create the context with **`StreamingContext.getOrCreate(checkpointDir, createFunc)`** instead of `new StreamingContext`."
  ]}
];
CP.notes.ml = [
  { h: "What machine learning is", items: [
    "Extracting patterns from data using statistics, linear algebra and numerical optimization. Types: supervised, semi-supervised, unsupervised, reinforcement.",
    "**Supervised**: records have labels; predict the label of new input. Discrete labels = **classification** (fraud or not), continuous = **regression** (price).",
    "**Unsupervised**: no labels; find structure, e.g. **clustering**. Also used for outlier detection, or as preprocessing (dimensionality reduction) before supervised learning.",
    "Review sheet 2 examples: supervised = fraud detection, house prices (RandomForestClassifier); unsupervised = customer segmentation (KMeans)."
  ]},
  { h: "Why Spark for ML", items: [
    "One unified engine for ingestion, feature engineering, training and deployment, instead of many tools that don't scale.",
    "Two packages: **spark.mllib** (original, **RDD**-based, maintenance mode since Spark 2.0) and **spark.ml** (newer, **DataFrame**-based). \"MLlib\" is the umbrella term for both.",
    "spark.ml is built for O(n) scale-out: the model scales linearly with the number of data points."
  ]},
  { h: "Transformers, estimators, pipelines (slide 8: a T/F is coming)", items: [
    "**Transformer**: takes a DataFrame, returns a new DataFrame with column(s) appended. **Learns nothing** from the data; rule-based. Method: **`.transform()`**. Example: VectorAssembler.",
    "**Estimator**: **learns (fits)** parameters from a DataFrame with **`.fit()`** and returns a **Model, which is a transformer**. Example: LinearRegression, StringIndexer, DecisionTreeClassifier, RandomForestRegressor, Imputer.",
    "Estimators are **eagerly** evaluated (`.fit()` kicks off Spark jobs); transformers are lazily evaluated.",
    "**Pipeline**: organizes transformers and estimators into one workflow. A Pipeline **is an estimator**; `pipeline.fit()` returns a **PipelineModel**, which **is a transformer**.",
    "Pipelines give code reusability and organization, and work out which stages need fit vs transform for you."
  ]},
  { h: "The linear regression pipeline (slides 9-14)", items: [
    "Goal: predict Airbnb price per night. `airbnbDF = spark.read.parquet(filePath)`.",
    "Split once: `trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)`. 80/20 is standard. Why not train on everything? The model could **memorize / overfit**, and you'd have no unseen data to measure generalization. Test performance is a proxy for production performance.",
    "Spark ML models need **all features in one vector column**: `vecAssembler = VectorAssembler(inputCols=[\"bedrooms\"], outputCol=\"features\")`, `vecTrainDF = vecAssembler.transform(trainDF)`. VectorAssembler only combines columns; modify columns first (e.g. `withColumn` + `log`).",
    "`lr = LinearRegression(featuresCol=\"features\", labelCol=\"price\")`, `lrModel = lr.fit(vecTrainDF)`; `lrModel.coefficients[0]`, `lrModel.intercept` → price = 123.68*bedrooms + 47.51.",
    "`pipeline = Pipeline(stages=[vecAssembler, lr])`, `pipelineModel = pipeline.fit(trainDF)`, `predDF = pipelineModel.transform(testDF)` adds a `prediction` column.",
    "Linear regression fits y ≈ mx + b + ε (multiple features: w0 + w1x1 + …); it minimizes the **sum of squared residuals**. Estimating the weights = learning / fitting the parameters."
  ]},
  { h: "Categorical features (slides 15-17, prof flagged 16)", items: [
    "Models need numbers. Mapping Dog=1, Cat=2, Fish=3 invents an order, so use **one-hot encoding**: one column per distinct value (Dog = [1,0,0]).",
    "OHE doesn't blow up memory in Spark: it uses a **SparseVector** when most entries are 0. `SparseVector(10, [3, 5], [7, 2])` = `DenseVector(0,0,0,7,0,2,0,0,0,0)` (size, indices, values).",
    "Slide 16: `categoricalCols` = string columns; `indexOutputCols` = name + \"Index\"; `oheOutputCols` = name + \"OHE\"; `StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid=\"skip\")` maps each string to an integer index; `OneHotEncoder(inputCols=indexOutputCols, outputCols=oheOutputCols)`; `numericCols` = double columns except price; `VectorAssembler(inputCols=oheOutputCols + numericCols, outputCol=\"features\")`.",
    "Slide 17: `Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])`, then fit on trainDF and transform testDF.",
    "`handleInvalid` for categories seen only in test data: **skip** (filter out rows), **error** (throw), **keep** (special extra bucket).",
    "**VectorIndexer** can detect categorical columns automatically, but it's expensive (checks every column against `maxCategories`)."
  ]},
  { h: "Evaluating regression (slides 18-20)", items: [
    "**RMSE**: error → square → sum (SSE) → divide by n (MSE) → square root (back to original units). Range 0 to ∞; **closer to 0 is better**.",
    "Judge RMSE against a **baseline**: always predict the training mean ȳ and compute its RMSE.",
    "**R²** = 1 − SSres/SStot. Perfect model → 1. Same as predicting ȳ → 0. Worse than predicting ȳ → **negative**. No baseline needed.",
    "`RegressionEvaluator(labelCol=\"price\", predictionCol=\"prediction\", metricName=\"rmse\")` (or `\"r2\"`), then `.evaluate(predDF)`."
  ]},
  { h: "Saving models (slide 21)", items: [
    "`pipelineModel.write().overwrite().save(pipelinePath)`.",
    "Loading needs the model type, so always wrap stages in a Pipeline and load with `PipelineModel.load(pipelinePath)`."
  ]},
  { h: "Decision trees and random forests (slides 22-30)", items: [
    "Decision tree = a series of **if-then-else rules** learned from data, for classification or regression. Fast, interpretable, **scale-invariant** (no need to standardize features). Handles categorical features via StringIndexer (no OHE needed).",
    "**Depth** = longest root-to-leaf path. Too deep → overfits; too shallow → underfits.",
    "Distributed: every worker computes summary statistics for every feature and split point, aggregated across workers; the best split is broadcast back and each worker partitions its local data.",
    "**maxBins** (default 32) controls discretization of continuous features and must be ≥ the number of categories in any categorical column (36 categories → error until maxBins ≥ 36; slides use 40). More bins = more splits = more computation.",
    "`dtModel = pipelineModel.stages[-1]`; `print(dtModel.toDebugString)` shows the learned rules; `featureImportances` scores features.",
    "A single tree here had a negative R² on test data, so use an ensemble: **random forest** = many trees with (1) **bootstrapping rows** (sampling with replacement, \"bagging\", `subsamplingRate`) and (2) **random feature subsets per split** (1/3 of features for regression, √features for classification). Predictions: average (regression) or **majority vote** (classification).",
    "`RandomForestRegressor(labelCol=\"price\", maxBins=40, seed=42)`."
  ]},
  { h: "Hyperparameter tuning (slides 30-38)", items: [
    "**Hyperparameter**: set before training, not learned (number of trees, max depth). **Parameter**: learned during training (weights).",
    "Tune on the training set → overfit; on the test set → no longer unseen. So use a **validation set** (60/20/20), which wastes 25% of training data → **k-fold cross-validation**.",
    "k-fold: split training data into k folds; for each configuration train on k−1 folds and validate on the remaining one, k times; average the k metrics. **Every point is used for validation exactly once.** Random search often beats grid search (Hyperopt).",
    "Steps in Spark: (1) define the estimator (pipeline); (2) **ParamGridBuilder** for the values to try; (3) an **evaluator** (RegressionEvaluator, RMSE); (4) **CrossValidator** to evaluate every model.",
    "`ParamGridBuilder().addGrid(rf.maxDepth, [2, 4, 6]).addGrid(rf.numTrees, [10, 100]).build()` → 3 × 2 = **6 configurations**; with numFolds=3 that's 18 fits.",
    "`CrossValidator(estimator=pipeline, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3, seed=42)`, `cvModel = cv.fit(trainDF)`, inspect `cvModel.avgMetrics`. Best here: maxDepth=6, numTrees=100 (lowest RMSE).",
    "Speed-ups: `parallelism` (Spark 2.3+, models trained in parallel; up to ~10 is enough), and putting the **CrossValidator inside the Pipeline** (`Pipeline(stages=[stringIndexer, vecAssembler, cv])`) so unchanged stages like StringIndexer aren't re-fit for every model."
  ]},
  { h: "Scalable ML: recommenders and clustering (slides 39-51)", items: [
    "**Collaborative filtering** (recommenders, from past behaviour), **classification** (supervised), **clustering** (unsupervised).",
    "User-based recommenders find similar users; item-based recommenders find similar items. The goal is to fill in the missing ratings of a user-item matrix.",
    "Slide 44 (on the quiz): `parse_rating` returns `(random.randint(1, 10), (user_id, profile_id, rating))`; `ratings = sc.textFile(...).map(parse_rating)`; training = `ratings.filter(lambda x: x[0] < 6).values().repartition(4).cache()` (60%), validation = `x[0] >= 6` (40%). `users = dict(sc.textFile(gender.dat).map(parse_user).collect())`. `matchseeker = int(sys.argv[1])`, `gender_filter = sys.argv[2]`.",
    "**ALS** (alternating least squares): `model = ALS.train(training, rank, num_iterations, lambda)`. Params: **rank** (number of latent factors, default 10), **iterations** (default 10), **lambda** (regularization, default 0.01), **alpha** (implicit confidence, default 1.0).",
    "RMSE: `model.predictAll(data.map(lambda x: (x[0], x[1])))` → map to `((user, item), pred)` → `join` with actual ratings → `.values()` → `sqrt(map((p - a) ** 2).reduce(add) / float(n))`.",
    "Recommend: `partners = sc.parallelize([u[0] for u in filter(lambda u: u[1] == gender_filter, users.items())])`; `model.predictAll(partners.map(lambda x: (matchseeker, x))).collect()`; `sorted(predictions, key=lambda x: x[2], reverse=True)[:10]`.",
    "**k-means** (slide 51): `model = KMeans.train(training, k)` on (lat, long) vectors; `model.clusterCenters`; quality = WSSSE (within set sum of squared errors) via `model.predict(point)`."
  ]},
  { h: "Distributed data science (slides 52-53)", items: [
    "Data products need an active data engineering pipeline: ingestion, wrangling, warehousing, computation, exploratory analysis = a **data workflow management system**.",
    "Goal: build and **operationalize fitted models**. At its heart are **ETL** processes: extract from the application, load into Hadoop, process, ETL back to the application.",
    "The ML lifecycle has a **build phase** (feature analysis, exploration) and an **operational phase** (users interact with the product and generate data to adapt the models). Production = steered analyses (data scientists drive) + automatic workflows, managed by the Hadoop ecosystem."
  ]}
];
CP.notes.pig = [
  { h: "What Pig is", items: [
    "A platform for analyzing large data sets: a high-level language (**Pig Latin**) plus the infrastructure to run it. An **abstraction on top of Hadoop**: scripts are **converted into MapReduce jobs**.",
    "For analysts, data scientists and statisticians who don't want to think in map and reduce functions or write Java.",
    "Why: quick (parallel), easy (small learning curve, common tasks built in), flexible. Parallel databases are expensive and need declarative SQL; MapReduce needs custom code even for common operations. Pig combines high-level declarative querying with low-level procedural programming.",
    "Multi-query approach: ~200 lines of Java ≈ 10 lines of Pig; development time cut ~16×. SQL-like. Built-in operators for joins, filters, ordering; nested types (tuple, bag, map) that MapReduce lacks.",
    "Features: rich operators, ease of programming, automatic optimization, extensibility, **UDFs**, handles structured and unstructured data, stores results in HDFS, ETL operators."
  ]},
  { h: "Pig vs MapReduce vs SQL (slide 8)", items: [
    "Pig is a **data flow language**, high level; joins are easy; novices with SQL can use it; multi-query approach; no compilation (each operator becomes a MapReduce job at execution).",
    "MapReduce is a **data processing paradigm**, low level and rigid; joins are hard; needs Java; ~20× more lines; long compilation.",
    "Pig Latin is **procedural**; SQL is **declarative**. In Pig the **schema is optional** (fields are $0, $1…); in SQL it's mandatory. Pig's data model is **nested relational**; SQL's is **flat relational**. SQL has more opportunity for query optimization."
  ]},
  { h: "Architecture (slides 9-10)", items: [
    "Run scripts through the Grunt shell, Pig Server, UDFs or embedded. Pig turns them into MapReduce jobs that run on Hadoop over HDFS.",
    "**Parser**: checks syntax, does type checking; outputs a **DAG** (logical plan): logical operators are nodes, data flows are edges.",
    "**Optimizer**: logical optimizations such as **projection and pushdown**.",
    "**Compiler**: compiles the optimized logical plan into **a series of MapReduce jobs**.",
    "**Execution engine**: submits the MapReduce jobs to Hadoop in sorted order and runs them."
  ]},
  { h: "Data model (slides 11-14)", items: [
    "Fully nested; no schema required. **Atom** (field): a single value, e.g. 'raja' or 30. **Tuple**: ordered set of fields, like a row: (Raja, 30). **Bag**: unordered collection of tuples `{(Raja, 30), (Mohammad, 45)}`; tuples may have different numbers of fields; a bag inside a relation is an **inner bag**. **Relation**: a bag of tuples, unordered. **Map**: key-value pairs, key must be a chararray and unique: `[name#Raja, age#30]`.",
    "Bag ≈ table, tuple ≈ row, but a bag doesn't need every tuple to have the same fields.",
    "Simple types: int (32-bit), long (64-bit, 10L), float (10.5F), double (10.5, 10.5e2). Arrays: chararray (UTF-8 string), bytearray (blob). Complex: tuple (19,2), bag {(19,2),(18,1)}, map [open#apache].",
    "UDFs: custom processing, can use non-atomic values (slide 11 says Java; slide 23 lists Java, Python, JavaScript)."
  ]},
  { h: "Running Pig and core commands (slides 15-20)", items: [
    "**Script**: `pig scriptFile.pig`. **Grunt**: interactive shell, started when no script is given (can `run`/`exec` scripts). **Embedded**: PigServer class, like JDBC for SQL.",
    "`LOAD 'data' [USING function] [AS schema];` path in **single quotes**; default load function **PigStorage**, default delimiter **tab ('\\t')**; `AS` names and types the fields. Example: `records = LOAD '/training/playArea/pig/excite-small.log' USING PigStorage() AS (userId:chararray, timestamp:long, query:chararray);`",
    "Diagnostics: **DESCRIBE** shows the schema of a relation/bag; **EXPLAIN** shows the execution plan (logical and MapReduce plans); **ILLUSTRATE** shows step by step how Pig transforms the data.",
    "`charGroup = GROUP chars BY c;` → each tuple has a field named **group** (the unique key) and a bag named after the original relation (**chars**) with all matching tuples: `(a,{(a),(a),(a)})`.",
    "`TOKENIZE(line)` splits a string into a **bag of tokens** (separators: space, double quote, comma, parentheses, star): `tokenBag = FOREACH linesOfText GENERATE TOKENIZE(line);`",
    "**Lazy**: nothing executes until **DUMP** (print to screen) or **STORE** (save, usually to HDFS/HBase). Pig parses and validates before that. `toPrint = LIMIT records 5; DUMP toPrint;`"
  ]},
  { h: "Joins, UDFs, choosing a tool (slides 21-26)", items: [
    "Pig supports inner, outer and full joins. Steps: load input 1, load input 2, join by key. **Default is an inner join**: only rows whose keys match. `userInfo = JOIN posts BY user, likes BY user;`",
    "UDF kinds: filter (`FILTER bag BY udfFilter(post)`), load (`LOAD 'file.txt' USING udfLoad()`), eval (`FOREACH bag GENERATE udfEval($1)`).",
    "MapReduce: flexibility, control, performance, but slower to write and error-prone. Pig and Hive: faster to write, test, deploy; best for most analysis.",
    "Use Pig for script-experienced developers, complex processing flows, unstructured/semi-structured data. Use Hive for very complex long-running queries.",
    "Pig/Hive are not RDBMS replacements. RDBMS: small-medium data, immediate results, in-place updates, interactive. Pig/Hive: large **read-only** data, cheap scalability, **batch** processing."
  ]},
  { h: "Yellow taxi example (slides 27-31)", items: [
    "`data = LOAD 'yellow_tripdata_2015-01.csv' USING PigStorage(',') AS (VendorID:chararray, pickup_datetime:chararray, …, passenger_count:int, trip_distance:float, …, fare_amount:float, tip_amount:float, total_amount:float);`",
    "`filtered_data = FILTER data BY (passenger_count > 0) AND (trip_distance > 0) AND (fare_amount > 0);`",
    "`group_by_passengers = GROUP filtered_data BY passenger_count;` then `avg_metrics = FOREACH group_by_passengers GENERATE group AS passenger_count, AVG(filtered_data.fare_amount) AS avg_fare, AVG(filtered_data.trip_distance) AS avg_distance, AVG(filtered_data.tip_amount) AS avg_tip;`",
    "UDF in Python (Jython): `@outputSchema(\"generous_tip:int\")` on `is_generous_tip(tip_amount, total_amount, threshold=20.0)`; `REGISTER 'TipPercentage.py' USING jython AS myudfs;` then `FOREACH filtered_data GENERATE …, myudfs.is_generous_tip(tip_amount, total_amount) AS generous_tip_label;`",
    "Aggregate with `SUM(labeled_data.generous_tip_label)`, `COUNT(labeled_data)`; `STORE features INTO 'trip_features_with_generous_tip_label' USING PigStorage(',');`"
  ]}
];

CP.compare.push(
  { title: "Stateless vs stateful (Spark Streaming)", topic: "sparkstream", cols: ["", "Stateless", "Stateful"], rows: [
    ["Uses earlier batches?", "No: each batch is independent", "Yes: data or results from previous batches"],
    ["Examples", "map, filter, reduceByKey (within one batch), join, cogroup", "Windowed operations, updateStateByKey / mapWithState"],
    ["Checkpointing", "Not required", "Required (`ssc.checkpoint(\"hdfs://...\")`)"],
    ["Trade-off", "Simple, efficient, no memory of the past", "Running totals, sessions, trends over time"]
  ]},
  { title: "Spark Streaming vs Hadoop Streaming", topic: "sparkstream", cols: ["", "Spark Streaming", "Hadoop Streaming"], rows: [
    ["What \"streaming\" means", "Unbounded real-time data streams", "Unix stdin / stdout"],
    ["Processing", "Micro-batches every batch interval", "Batch MapReduce job"],
    ["Abstraction", "DStream (sequence of RDDs)", "Lines of key<TAB>value text"]
  ]},
  { title: "Transformer vs estimator", topic: "ml", cols: ["", "Transformer", "Estimator"], rows: [
    ["Learns from data?", "No, rule-based", "Yes, fits parameters"],
    ["Method", "`.transform(df)` → DataFrame with new column(s)", "`.fit(df)` → a Model (which is a transformer)"],
    ["Evaluation", "Lazy", "Eager (kicks off Spark jobs)"],
    ["Examples", "VectorAssembler, fitted models, PipelineModel", "LinearRegression, StringIndexer, RandomForestRegressor, Pipeline"]
  ]},
  { title: "Supervised vs unsupervised", topic: "ml", cols: ["", "Supervised", "Unsupervised"], rows: [
    ["Data", "Labelled", "Unlabelled"],
    ["Goal", "Predict labels for new data", "Find structure / groups"],
    ["Kinds", "Classification (discrete), regression (continuous)", "Clustering, dimensionality reduction, outlier detection"],
    ["Spark examples", "LinearRegression, RandomForestClassifier", "KMeans"]
  ]},
  { title: "Decision tree vs random forest", topic: "ml", cols: ["", "Decision tree", "Random forest"], rows: [
    ["Model", "One tree of if-then-else rules", "Ensemble of many trees"],
    ["Training data", "All rows, all features", "Bootstrapped rows + random feature subset at each split"],
    ["Prediction", "Follow one path", "Average (regression) or majority vote (classification)"],
    ["Weakness", "Overfits when deep; was negative R² on the slides", "Slower; less interpretable"]
  ]},
  { title: "Train / validation / test and k-fold", topic: "ml", cols: ["", "Train/test (80/20)", "Train/validation/test (60/20/20)", "k-fold cross-validation"], rows: [
    ["Used for", "Fit, then estimate performance on unseen data", "Pick hyperparameters on validation", "Pick hyperparameters without losing training data"],
    ["Downside", "No safe way to tune hyperparameters", "Loses 25% of the training data", "Trains k models per configuration (slower)"]
  ]},
  { title: "Pig vs MapReduce vs SQL", topic: "pig", cols: ["", "Pig Latin", "MapReduce", "SQL"], rows: [
    ["Style", "Procedural data flow", "Low-level processing paradigm", "Declarative"],
    ["Schema", "Optional ($0, $1…)", "None built in", "Mandatory"],
    ["Data model", "Nested relational", "Key-value pairs", "Flat relational"],
    ["Joins", "Easy", "Hard", "Easy"],
    ["Code size", "~10 lines", "~200 lines (about 20× more)", "Short"]
  ]},
  { title: "Pig / Hive vs RDBMS", topic: "pig", cols: ["", "Pig / Hive", "RDBMS"], rows: [
    ["Data size", "Large", "Small to medium"],
    ["Updates", "Read-only data", "In-place modification"],
    ["Use", "Batch processing", "Interactive, immediate results"],
    ["Scaling", "Extensive, low cost", "Expensive"]
  ]}
);

CP.slideStyle.push(
  { track: "sparkstream", name: "Spark Streaming", items: [
    "Slide 26 (Scala): `val ssc = new StreamingContext(conf, Seconds(1))`, `val lines = ssc.socketTextStream(\"localhost\", 7777)`, `val errorLines = lines.filter(_.contains(\"error\"))`, `errorLines.print()`, `ssc.start()`, `ssc.awaitTermination()`.",
    "PySpark equivalent: `ssc = StreamingContext(sc, 1)`, `lines.filter(lambda line: \"error\" in line)`, `.pprint()`.",
    "Stateless counts per batch: `map(lambda x: (x, 1)).reduceByKey(add)`. Stateful: `ssc.checkpoint(\"hdfs://...\")` + `updateStateByKey(update)` or `reduceByKeyAndWindow(func, None, windowDuration, slideDuration)`.",
    "Driver fault tolerance: `StreamingContext.getOrCreate(checkpointDir, createContext)`."
  ]},
  { track: "ml", name: "Spark MLlib", items: [
    "`trainDF, testDF = df.randomSplit([.8, .2], seed=42)`; `VectorAssembler(inputCols=[...], outputCol=\"features\")`; `LinearRegression(featuresCol=\"features\", labelCol=\"price\")`.",
    "`StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid=\"skip\")`, `OneHotEncoder(inputCols=indexOutputCols, outputCols=oheOutputCols)`.",
    "`pipeline = Pipeline(stages=[...])`, `pipelineModel = pipeline.fit(trainDF)`, `predDF = pipelineModel.transform(testDF)`; `RegressionEvaluator(labelCol=\"price\", predictionCol=\"prediction\", metricName=\"rmse\").evaluate(predDF)`.",
    "`ParamGridBuilder().addGrid(rf.maxDepth, [2, 4, 6]).addGrid(rf.numTrees, [10, 100]).build()`; `CrossValidator(estimator=pipeline, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3, seed=42)`.",
    "RDD MLlib (slides 44-51): `ALS.train(training, rank, num_iterations, lambda)`, `model.predictAll(...)`, `KMeans.train(training, k)`."
  ]},
  { track: "pig", name: "Pig Latin", items: [
    "`data = LOAD 'file.csv' USING PigStorage(',') AS (name:chararray, n:int, x:float);`",
    "`filtered = FILTER data BY (n > 0) AND (x > 0);` · `grouped = GROUP filtered BY name;`",
    "`out = FOREACH grouped GENERATE group AS name, AVG(filtered.x) AS avg_x, SUM(filtered.x) AS total_x, COUNT(filtered) AS n;`",
    "`joined = JOIN a BY key, b BY key;` (inner by default) · `DUMP out;` or `STORE out INTO 'path' USING PigStorage(',');` · `DESCRIBE`, `EXPLAIN`, `ILLUSTRATE`, `LIMIT rel 5`."
  ]}
);

CP.tracks.push(
  { id: "pig", name: "Pig Latin", lang: "pig", blurb: "LOAD … USING PigStorage, FILTER, GROUP, FOREACH … GENERATE, JOIN, DUMP / STORE.", isNew: true },
  { id: "ml", name: "Spark MLlib", lang: "python", blurb: "VectorAssembler, StringIndexer, Pipeline, fit / transform, RegressionEvaluator, CrossValidator, ALS.", isNew: true },
  { id: "sparkstream", name: "Spark Streaming", lang: "python", blurb: "StreamingContext, DStreams, stateless vs stateful, windows, updateStateByKey, checkpointing.", isNew: true }
);

/* ---------------- Multiple choice ---------------- */
CP.mcq.push(
  // review sheet 2 (answers posted by the prof)
  { id: "r2m1", topic: "spark", review: 2, q: "What is the primary reason Apache Spark is considered faster than traditional MapReduce frameworks?", options: ["It uses more efficient hardware resources", "It minimizes data processing overhead by using directed acyclic graphs", "It avoids disk I/O by caching intermediate data in memory", "It integrates tightly with Hadoop's YARN"], answer: 2, explain: "Prof's answer: C. The DAG (B) helps too, which is why it's a \"best answer\" question: in-memory caching is the primary reason.", src: "Review 2 MCQ 1" },
  { id: "r2m2", topic: "sparksql", review: 2, q: "Which of the following best describes the relationship between RDDs and DataFrames in Spark?", options: ["RDDs are optimized for relational operations, while DataFrames are not", "DataFrames provide a higher-level abstraction over RDDs with schema information", "RDDs cannot be parallelized, but DataFrames can", "DataFrames are less efficient than RDDs for iterative algorithms"], answer: 1, explain: "A DataFrame wraps an RDD of Row objects and adds a schema.", src: "Review 2 MCQ 2" },
  { id: "r2m3", topic: "sparkstream", review: 2, q: "In the context of Spark Streaming, what is the purpose of a Discretized Stream (DStream)?", options: ["It continuously computes results for an infinite stream of data", "It breaks a stream of data into small batches for processing as RDDs", "It directly supports relational queries on streaming data", "It bypasses the use of RDDs for faster processing"], answer: 1, explain: "A DStream is a sequence of RDDs, one per batch interval (micro-batch architecture).", src: "Review 2 MCQ 3, Spark slides 27-29" },
  { id: "r2m4", topic: "ml", review: 2, q: "Which feature of Spark MLlib pipelines ensures that data preparation steps and model training can be reused and organized efficiently?", options: ["Transformers only", "Estimators only", "PipelineModel", "ParameterGridBuilder"], answer: 2, explain: "Prof's answer: PipelineModel, the fitted pipeline that applies every stage to new data in one transform().", src: "Review 2 MCQ 4, ML slide 14" },
  { id: "r2m5", topic: "pig", review: 2, q: "What advantage does Apache Pig provide over raw MapReduce programming?", options: ["Pig scripts are dynamically typed, eliminating the need for schema definitions", "Pig abstracts complex data operations into simpler SQL-like queries", "Pig automatically optimizes MapReduce jobs at runtime without programmer intervention", "Pig enables low-level control over Hadoop jobs"], answer: 1, explain: "Pig Latin is high level and SQL-like; low-level control (D) is MapReduce's strength.", src: "Review 2 MCQ 5, Pig slides 3-5" },
  { id: "r2m6", topic: "spark", review: 2, q: "How does Spark's lazy evaluation improve the efficiency of data processing pipelines?", options: ["It prevents unnecessary computations by combining transformations", "It minimizes memory usage by discarding intermediate data", "It allows actions to execute immediately without overhead", "It avoids serialization of objects across nodes"], answer: 0, explain: "Spark sees the whole chain before running it, so it can combine steps and skip what isn't needed.", src: "Review 2 MCQ 6" },
  { id: "r2m7", topic: "pig", review: 2, q: "Which component of Apache Pig converts Pig Latin scripts into MapReduce jobs for execution?", options: ["Parser", "Optimizer", "Compiler", "Execution Engine"], answer: 2, explain: "Parser → DAG; Optimizer → projection/pushdown; Compiler → MapReduce jobs; Execution engine → submits them to Hadoop.", src: "Review 2 MCQ 7, Pig slide 10" },
  { id: "r2m8", topic: "ml", review: 2, q: "In machine learning with Spark, which step typically involves creating a VectorAssembler?", options: ["Model evaluation", "Data transformation", "Hyperparameter tuning", "Feature engineering"], answer: 3, explain: "Prof's answer: feature engineering. \"Data transformation\" is partly right (it's a transformer), a classic best-answer distractor.", src: "Review 2 MCQ 8, ML slide 11" },
  { id: "r2m9", topic: "spark", review: 2, q: "What is a key difference between yarn-client and yarn-cluster deployment modes in Spark?", options: ["Only yarn-client supports streaming applications", "In yarn-client, the driver runs locally, while in yarn-cluster, it runs on the cluster", "yarn-client provides better fault tolerance than yarn-cluster", "In yarn-cluster, the driver is not required for job submission"], answer: 1, explain: "yarn-cluster runs the driver inside the ApplicationMaster.", src: "Review 2 MCQ 9, Spark slide 10" },
  { id: "r2m10", topic: "sparksql", review: 2, q: "Which of the following best describes Spark SQL's role in data processing?", options: ["It provides a replacement for Spark Core", "It allows querying structured data using a SQL-like interface", "It is an alternative to RDD transformations for real-time analytics", "It improves the performance of iterative machine learning tasks"], answer: 1, explain: "Spark SQL runs as a library on top of Spark Core.", src: "Review 2 MCQ 10" },
  { id: "r2m11", topic: "sparkstream", review: 2, q: "What is the main advantage of Spark's updateStateByKey transformation in streaming applications?", options: ["It performs batch processing on unbounded datasets", "It allows stateful computations across multiple time windows", "It optimizes memory usage for large datasets", "It eliminates the need for checkpointing"], answer: 1, explain: "It keeps per-key state across batches. It actually REQUIRES checkpointing (D is the trap).", src: "Review 2 MCQ 11, Spark slide 37" },
  { id: "r2m12", topic: "pig", review: 2, q: "In Pig Latin, what is the purpose of the DESCRIBE command?", options: ["It executes the script and outputs the results", "It displays the schema of a relation or bag", "It compiles the logical plan for debugging", "It loads data from an external source"], answer: 1, explain: "DESCRIBE = schema. EXPLAIN = execution plan. ILLUSTRATE = step-by-step transformation. DUMP = run and print.", src: "Review 2 MCQ 12, Pig slide 17" },
  { id: "r2m13", topic: "ml", review: 2, q: "What is the benefit of using k-fold cross-validation for hyperparameter tuning?", options: ["It reduces computation time by using fewer data points", "It provides a consistent training and test split for every iteration", "It ensures every data point is used for both training and validation exactly once", "It eliminates the need for separate validation and test datasets"], answer: 2, explain: "Every point is in the validation fold exactly once. It costs MORE computation (A is wrong), and you still keep a test set.", src: "Review 2 MCQ 13, ML slides 32-33" },
  { id: "r2m14", topic: "sparkstream", review: 2, q: "In Spark Streaming, what role does checkpointing play in ensuring fault tolerance?", options: ["It caches data in memory to improve processing speed", "It stores intermediate data to reduce recomputation", "It saves metadata and state to reliable storage for recovery", "It eliminates the need for data replication across nodes"], answer: 2, explain: "Prof's answer: C. B is partly true (it limits recomputation), but C is the best answer.", src: "Review 2 MCQ 14, Spark slide 39" },
  { id: "r2m15", topic: "pig", review: 2, q: "Which transformation in Apache Pig is used to combine datasets based on a common key, and what is its default behavior?", options: ["FILTER; removes rows with mismatched keys", "JOIN; performs an inner join on matching keys", "COGROUP; creates a bag for each key with all matching records", "UNION; concatenates datasets without removing duplicates"], answer: 1, explain: "JOIN defaults to an inner join: rows without a match are dropped.", src: "Review 2 MCQ 15, Pig slide 21" },

  // Spark Streaming
  { id: "ssm1", topic: "sparkstream", q: "Which BEST describes Spark Streaming's architecture?", options: ["Each record is processed individually the instant it arrives", "Micro-batch: the stream is grouped into small batches at regular intervals, and each batch is processed as an RDD", "Data is written to HDFS and processed by a nightly MapReduce job", "It uses stdin and stdout like Hadoop Streaming"], answer: 1, explain: "Micro-batch architecture: a continuous series of small batch computations.", src: "Spark slide 27" },
  { id: "ssm2", topic: "sparkstream", q: "What is a typical batch interval in Spark Streaming?", options: ["1 to 10 microseconds", "500 milliseconds to several seconds", "10 to 60 minutes", "One day"], answer: 1, explain: "Set by the developer when creating the StreamingContext.", src: "Spark slide 27" },
  { id: "ssm3", topic: "sparkstream", q: "In `ssc = StreamingContext(conf, Seconds(1))` … `ssc.socketTextStream(\"localhost\", 7777)` … `errorLines.print()`, when does data start being processed?", options: ["As soon as socketTextStream is called", "When filter() is applied", "Only after ssc.start() is called", "When print() is called"], answer: 2, explain: "The earlier lines only set up the computation. start() begins receiving data; awaitTermination() keeps the app alive.", src: "Spark slide 26" },
  { id: "ssm4", topic: "sparkstream", q: "Why does the streaming example call ssc.awaitTermination() after ssc.start()?", options: ["To checkpoint the data", "Because streaming runs in a separate thread, so the application would otherwise exit", "To stop the receivers", "To compile the DStream"], answer: 1, explain: "start() schedules jobs in a separate thread; awaitTermination() waits for the streaming computation to finish.", src: "Spark slide 26" },
  { id: "ssm5", topic: "sparkstream", q: "By default, what do receivers do with the data they receive?", options: ["Write it straight to HDFS", "Store it in executor memory and replicate it to another executor for fault tolerance", "Send it to the driver", "Discard it after each batch"], answer: 1, explain: "Data is replicated to a second node so a single worker failure is tolerated.", src: "Spark slide 31" },
  { id: "ssm6", topic: "sparkstream", q: "A DStream of (ip, 1) pairs is reduced with reduceByKey. What does the result count?", options: ["The total for each IP since the program started", "The count for each IP within each batch only", "The count over the last 3 batches", "Nothing: reduceByKey isn't allowed on DStreams"], answer: 1, explain: "Stateless: reduceByKey reduces within each time step, not across time steps.", src: "Spark slide 33" },
  { id: "ssm7", topic: "sparkstream", q: "The batch interval is 10 seconds and you want results over the last 30 seconds, computed every 20 seconds. Which settings do you use?", options: ["window duration 30 s, sliding duration 20 s", "window duration 20 s, sliding duration 30 s", "window duration 3, sliding duration 2", "batch interval 30 s"], answer: 0, explain: "Window = how far back (30 s = 3 batches); slide = how often (20 s = every second batch). Both are multiples of the batch interval.", src: "Spark slides 35-36" },
  { id: "ssm8", topic: "sparkstream", q: "Which of these is a stateful transformation?", options: ["filter()", "map()", "reduceByKey()", "updateStateByKey()"], answer: 3, explain: "Stateful = windowed operations and updateStateByKey. The others are stateless.", src: "Spark slides 32-34" },
  { id: "ssm9", topic: "sparkstream", q: "What must you set up before using updateStateByKey or windowed operations?", options: ["A second StreamingContext", "Checkpointing: ssc.checkpoint(\"hdfs://...\")", "A combiner", "yarn-cluster mode"], answer: 1, explain: "Stateful transformations require checkpointing in the StreamingContext.", src: "Spark slide 34" },
  { id: "ssm10", topic: "sparkstream", q: "In `update(events, oldState)` for updateStateByKey, how do you delete the state for a key?", options: ["Return 0", "Return an empty Option (None)", "Call ssc.stop()", "Throw an exception"], answer: 1, explain: "newState is an Option; an empty one removes the key's state.", src: "Spark slide 37" },
  { id: "ssm11", topic: "sparkstream", q: "Which pair lists the TWO purposes of checkpointing in Spark Streaming?", options: ["Speed up reads and compress data", "Limit the state that must be recomputed on failure, and give the driver fault tolerance", "Replace receivers and remove the need for HDFS", "Encrypt data and balance load"], answer: 1, explain: "Lineage recomputation could reach back to the start; checkpoints bound it. A restarted driver recovers from the checkpoint.", src: "Spark slide 39" },
  { id: "ssm12", topic: "sparkstream", q: "How do you make a streaming driver recoverable after it crashes?", options: ["Create the context with StreamingContext.getOrCreate(checkpointDir, createFunc)", "Run two drivers at once", "Use a larger batch interval", "Call awaitTermination twice"], answer: 0, explain: "getOrCreate rebuilds the context from the checkpoint directory if one exists.", src: "Spark slide 40" },
  { id: "ssm13", topic: "sparkstream", q: "How often does the slide suggest checkpointing a streaming application?", options: ["Every record", "Every 5-10 batches", "Once per day", "Never: lineage is enough"], answer: 1, explain: "Recovery then only has to go back to the last checkpoint.", src: "Spark slide 31" },
  { id: "ssm14", topic: "sparkstream", q: "Spark Streaming offers \"exactly once\" semantics. What does that mean?", options: ["Each record is read from the source once and then deleted", "As long as the input is stored reliably, results are as if all data were processed without any node failing", "Each batch runs on exactly one executor", "Each key appears once in the output"], answer: 1, explain: "Even if workers or the driver fail.", src: "Spark slide 39" },

  // ML
  { id: "mlm1", topic: "ml", q: "Which is an estimator?", options: ["VectorAssembler", "LinearRegression", "A fitted PipelineModel", "A trained LinearRegressionModel"], answer: 1, explain: "Estimators learn with .fit(). VectorAssembler and fitted models are transformers.", src: "ML slides 8, 13" },
  { id: "mlm2", topic: "ml", q: "What does `pipeline.fit(trainDF)` return?", options: ["A DataFrame of predictions", "A PipelineModel, which is a transformer", "A Pipeline, which is an estimator", "The RMSE"], answer: 1, explain: "Pipelines are estimators; fitted pipelines (PipelineModels) are transformers you call .transform() on.", src: "ML slides 8, 14" },
  { id: "mlm3", topic: "ml", q: "Why do we hold out a test set instead of training on the entire data set?", options: ["Spark can't train on more than 80% of a DataFrame", "The model might memorize (overfit) the training data, and we'd have no unseen data to measure how well it generalizes", "To make training faster", "Because the test set has no labels"], answer: 1, explain: "Test performance is a proxy for performance on unseen (production) data.", src: "ML slide 10" },
  { id: "mlm4", topic: "ml", q: "Why is VectorAssembler needed before LinearRegression?", options: ["It scales features to [0, 1]", "Spark ML models require all input features in a single vector column", "It removes nulls", "It one-hot encodes strings"], answer: 1, explain: "It only combines columns into one vector; it doesn't transform values.", src: "ML slide 11" },
  { id: "mlm5", topic: "ml", q: "Why use one-hot encoding instead of mapping Dog=1, Cat=2, Fish=3?", options: ["Numbers use too much memory", "The numeric mapping would introduce a fake ordering/relationship between categories", "MLlib can't read integers", "One-hot encoding is faster to train"], answer: 1, explain: "OHE makes a separate column per distinct value.", src: "ML slide 15" },
  { id: "mlm6", topic: "ml", q: "What does `SparseVector(10, [3, 5], [7, 2])` represent?", options: ["(3, 5, 7, 2)", "A 10-element vector that is 0 everywhere except 7 at index 3 and 2 at index 5", "A vector of 10 threes and fives", "A 4-element vector"], answer: 1, explain: "Size 10, indices [3, 5], values [7, 2] = DenseVector(0,0,0,7,0,2,0,0,0,0). That's why OHE doesn't waste memory in Spark.", src: "ML slide 15" },
  { id: "mlm7", topic: "ml", q: "In slide 16, what does StringIndexer do?", options: ["Splits strings into tokens", "Maps each unique string in a categorical column to a numeric index", "Creates binary vectors for each category", "Sorts the strings"], answer: 1, explain: "StringIndexer → integer index; OneHotEncoder then turns indices into binary vectors.", src: "ML slide 16" },
  { id: "mlm8", topic: "ml", q: "A category appears in the test set but never in training. With `handleInvalid=\"skip\"`, what happens?", options: ["The pipeline throws an error", "Rows with that unseen category are filtered out", "They go into a special extra bucket", "They are assigned index 0"], answer: 1, explain: "skip = filter out; error = throw; keep = special extra bucket.", src: "ML slide 17" },
  { id: "mlm9", topic: "ml", q: "Your model's R² on the test set is negative. What does it mean?", options: ["Perfect predictions", "Same as always predicting the mean", "Worse than always predicting the mean ȳ, so re-evaluate the modeling process", "The RMSE is zero"], answer: 2, explain: "R² = 1 perfect, 0 = predicting ȳ, negative = worse than ȳ.", src: "ML slide 20" },
  { id: "mlm10", topic: "ml", q: "How do you judge whether an RMSE of 220.6 is good?", options: ["Any RMSE under 1000 is good", "Compare it with a baseline model's RMSE, e.g. always predicting the training mean", "Compare it with R²", "Divide it by the number of rows"], answer: 1, explain: "RMSE is on the scale of the label, so compare against a simple baseline.", src: "ML slide 19" },
  { id: "mlm11", topic: "ml", q: "Why does the slide recommend always putting stages in a Pipeline before saving?", options: ["Pipelines save faster", "Loading needs the model type; with a Pipeline you always load a PipelineModel and only change the path", "Only pipelines can be saved", "It compresses the model"], answer: 1, explain: "`PipelineModel.load(pipelinePath)` works for every model.", src: "ML slide 21" },
  { id: "mlm12", topic: "ml", q: "A decision tree fails because a categorical column has 36 distinct values. What do you change?", options: ["Increase maxDepth", "Set maxBins to at least 36 (the slides use 40)", "Remove StringIndexer", "Lower numTrees"], answer: 1, explain: "maxBins (default 32) must handle the discretization of categorical columns.", src: "ML slide 24" },
  { id: "mlm13", topic: "ml", q: "Which two tweaks turn a set of decision trees into a random forest?", options: ["Deeper trees and more bins", "Bootstrapping rows (bagging) and a random subset of features at each split", "One-hot encoding and scaling", "Cross-validation and a test set"], answer: 1, explain: "Random feature selection keeps the trees from being highly correlated.", src: "ML slide 28" },
  { id: "mlm14", topic: "ml", q: "Which is a hyperparameter?", options: ["A linear regression coefficient", "The intercept", "The number of trees in a random forest", "The predicted price"], answer: 2, explain: "Hyperparameters are set before training (numTrees, maxDepth). Coefficients and intercepts are learned parameters.", src: "ML slides 30-31" },
  { id: "mlm15", topic: "ml", q: "ParamGridBuilder varies maxDepth over [2, 4, 6] and numTrees over [10, 100]. How many configurations does it build?", options: ["5", "6", "12", "18"], answer: 1, explain: "3 × 2 = 6. With numFolds=3, CrossValidator trains 6 × 3 = 18 models.", src: "ML slide 34" },
  { id: "mlm16", topic: "ml", q: "What does CrossValidator need to be given?", options: ["Only the training data", "An estimator, an evaluator and estimatorParamMaps (plus numFolds, seed)", "A test set and a SparseVector", "A StringIndexer"], answer: 1, explain: "Which model, how to score it, and which hyperparameters to try.", src: "ML slide 35" },
  { id: "mlm17", topic: "ml", q: "Why put the CrossValidator inside the Pipeline instead of the Pipeline inside the CrossValidator?", options: ["It improves accuracy", "So unchanged stages like StringIndexer aren't re-fit for every model, which speeds up training", "It's required by Spark 2.3", "It lets you skip the evaluator"], answer: 1, explain: "Otherwise the same StringIndexer mapping is learned over and over.", src: "ML slide 38" },
  { id: "mlm18", topic: "ml", q: "In the ALS recommender, what does the rank parameter control?", options: ["The number of iterations", "The number of latent factors (size of the feature vectors)", "The regularization strength", "The number of users"], answer: 1, explain: "rank (default 10): larger can be better but costs more. iterations default 10, lambda default 0.01, alpha default 1.0.", src: "ML slide 45" },
  { id: "mlm19", topic: "ml", q: "In slide 44, how is the 60/40 training/validation split made?", options: ["randomSplit([.6, .4])", "Each rating gets a random integer 1-10; training keeps keys < 6, validation keys ≥ 6", "The first 60% of lines are training", "Users with even IDs are training"], answer: 1, explain: "parse_rating returns (random.randint(1, 10), (user_id, profile_id, rating)), then filter on x[0].", src: "ML slide 44" },
  { id: "mlm20", topic: "ml", q: "Which type of learning does k-means clustering use?", options: ["Supervised", "Unsupervised", "Reinforcement", "Collaborative filtering"], answer: 1, explain: "No labels; it groups similar items (e.g. earthquake locations into 6 clusters).", src: "ML slides 50-51" },
  { id: "mlm21", topic: "ml", q: "spark.mllib vs spark.ml: which statement is correct?", options: ["spark.ml is RDD-based; spark.mllib is DataFrame-based", "spark.mllib is the original RDD-based API (maintenance mode); spark.ml is the newer DataFrame-based API", "They are identical", "spark.ml only works in Scala"], answer: 1, explain: "The slides focus on spark.ml for pipelines; ALS and KMeans examples use the RDD API.", src: "ML slide 7" },
  { id: "mlm22", topic: "ml", q: "At the heart of the distributed data science workflow are which processes?", options: ["Map and reduce", "ETL: extract from the application, load into Hadoop, process, and load back", "Bootstrapping", "Checkpointing"], answer: 1, explain: "The goal is to build and operationalize fitted models.", src: "ML slide 52" },

  // Pig
  { id: "pgm1", topic: "pig", q: "Which BEST describes Apache Pig?", options: ["A NoSQL database", "A high-level data flow platform whose Pig Latin scripts are converted into MapReduce jobs on Hadoop", "A replacement for HDFS", "A real-time stream processor"], answer: 1, explain: "Pig is an abstraction on top of Hadoop.", src: "Pig slide 3" },
  { id: "pgm2", topic: "pig", q: "Pig Latin vs SQL: which is correct?", options: ["Pig Latin is declarative; SQL is procedural", "Pig Latin is procedural with an optional schema and a nested data model; SQL is declarative with a mandatory schema and a flat model", "Both require a schema", "SQL has a nested data model"], answer: 1, explain: "Slide 8's Pig vs SQL table.", src: "Pig slide 8" },
  { id: "pgm3", topic: "pig", q: "What is the output of the Pig parser?", options: ["MapReduce jobs", "A DAG (logical plan) with operators as nodes and data flows as edges", "Optimized Java bytecode", "Results on screen"], answer: 1, explain: "The optimizer then applies projection and pushdown; the compiler makes MapReduce jobs.", src: "Pig slide 10" },
  { id: "pgm4", topic: "pig", q: "Which optimizations does the Pig optimizer apply to the logical plan?", options: ["Combiner and partitioner", "Projection and pushdown", "Caching and lineage", "Bagging and boosting"], answer: 1, explain: "From the architecture slide.", src: "Pig slide 10" },
  { id: "pgm5", topic: "pig", q: "In Pig's data model, what is a bag?", options: ["An ordered set of fields", "An unordered collection of tuples, written with {}", "A key-value pair written with []", "A single atomic value"], answer: 1, explain: "Tuple = ordered fields (); bag = unordered tuples {}; map = key#value []. A bag's tuples can have different numbers of fields.", src: "Pig slides 12-14" },
  { id: "pgm6", topic: "pig", q: "What is the default load function and delimiter for LOAD?", options: ["CSVLoader with a comma", "PigStorage with a tab", "TextLoader with a space", "JsonLoader"], answer: 1, explain: "Use `USING PigStorage(',')` for CSV files.", src: "Pig slide 16" },
  { id: "pgm7", topic: "pig", q: "You run LOAD, FILTER and GROUP in Grunt. When does Pig actually execute them?", options: ["Immediately after each statement", "Only when a DUMP or STORE is encountered", "When DESCRIBE is called", "When the Grunt shell exits"], answer: 1, explain: "Pig parses and validates but doesn't run anything until DUMP or STORE.", src: "Pig slide 20" },
  { id: "pgm8", topic: "pig", q: "After `charGroup = GROUP chars BY c;`, what does each tuple contain?", options: ["Only the distinct values of c", "A field named group (the key) and a bag named chars with all matching tuples", "A count of each value", "The original tuples in order"], answer: 1, explain: "describe charGroup: {group: chararray, chars: {(c: chararray)}}. Dump: (a,{(a),(a),(a)}).", src: "Pig slide 18" },
  { id: "pgm9", topic: "pig", q: "What does `FOREACH linesOfText GENERATE TOKENIZE(line);` produce for each line?", options: ["A single string", "A bag of tokens (words)", "A count of words", "A map of words to counts"], answer: 1, explain: "({(this),(is),(a),(line),(of),(text)}).", src: "Pig slide 19" },
  { id: "pgm10", topic: "pig", q: "Which command shows how the Pig engine transforms the data, step by step?", options: ["DESCRIBE", "EXPLAIN", "ILLUSTRATE", "DUMP"], answer: 2, explain: "EXPLAIN shows the logical and MapReduce plans; ILLUSTRATE shows sample data at each step.", src: "Pig slide 17" },
  { id: "pgm11", topic: "pig", q: "When should you choose MapReduce over Pig or Hive?", options: ["When you need to write quickly", "When control and performance matter most", "For simple aggregations", "When analysts write the code"], answer: 1, explain: "MapReduce is flexible and fast but time-consuming and error-prone to write.", src: "Pig slide 24" },
  { id: "pgm12", topic: "pig", q: "Pig and Hive compared with an RDBMS: which is correct?", options: ["Pig/Hive are for small data with immediate results", "Pig/Hive are optimized for large read-only data and batch processing; RDBMSs suit interactive use and in-place updates", "Pig/Hive replace RDBMSs", "RDBMSs scale more cheaply"], answer: 1, explain: "They are not intended to replace an RDBMS.", src: "Pig slide 26" },
  { id: "pgm13", topic: "pig", q: "How is a Python UDF made available in a Pig script on the slides?", options: ["IMPORT 'TipPercentage.py';", "REGISTER 'TipPercentage.py' USING jython AS myudfs;", "LOAD 'TipPercentage.py';", "DEFINE python TipPercentage;"], answer: 1, explain: "Then call `myudfs.is_generous_tip(tip_amount, total_amount)` inside FOREACH … GENERATE.", src: "Pig slide 30" },
  { id: "pgm14", topic: "pig", q: "Which is NOT one of the ways to run Pig?", options: ["Script file (pig scriptFile.pig)", "Grunt interactive shell", "Embedded via PigServer", "Through the YARN NodeManager UI"], answer: 3, explain: "Script, Grunt, Embedded.", src: "Pig slide 15" },

  // new pre-midterm questions
  { id: "pm1", topic: "bigdata", q: "A hospital stores call logs with drop-down fields (date, problem type) plus free-text notes from the support agent. Which is the BEST description?", options: ["Purely structured data", "Purely unstructured data", "A mix: structured fields plus unstructured text, which is common in practice", "Quasi-structured only"], answer: 2, explain: "The slide notes say the four types are often mixed, e.g. an RDBMS of call logs with free-form text.", src: "Intro slide 11 notes" },
  { id: "pm2", topic: "bigdata", q: "\"Do I have enough information to draft an analytic plan and share it for peer review?\" belongs to which phase?", options: ["Discovery", "Data preparation", "Model building", "Operationalize"], answer: 0, explain: "Discovery comes first in the lifecycle.", src: "Hadoop slide 5" },
  { id: "pm3", topic: "bigdata", q: "Which tool is listed under \"distributed coordination\" in the big data sandbox?", options: ["YARN", "ZooKeeper", "HBase", "Kafka"], answer: 1, explain: "YARN = cluster management, HBase = data management, Kafka = streaming.", src: "Intro slide 24" },
  { id: "pm4", topic: "hadoop", q: "Which is the BEST reason Hadoop moves code to the data?", options: ["Code files are smaller than data, so moving code saves network bandwidth, the most precious resource", "Java can't read remote files", "The NameNode requires it", "Data can't be copied in HDFS"], answer: 0, explain: "Data locality conserves network bandwidth.", src: "Hadoop slides 8-9" },
  { id: "pm5", topic: "hadoop", q: "Which daemons run on a typical worker node?", options: ["NameNode and ResourceManager", "DataNode and NodeManager", "Secondary NameNode and ApplicationMaster only", "ZooKeeper and Hive"], answer: 1, explain: "HDFS worker (DataNode) + YARN worker (NodeManager).", src: "Hadoop slides 16-18" },
  { id: "pm6", topic: "hadoop", q: "Which component tracks the status of the NODES, and which tracks the status of a JOB's tasks?", options: ["NodeManager tracks nodes; ResourceManager tracks the job", "ResourceManager tracks nodes; ApplicationMaster tracks the job", "NameNode tracks both", "DataNode tracks nodes; NodeManager tracks the job"], answer: 1, explain: "Straight from slide 18's workflow.", src: "Hadoop slide 18" },
  { id: "pm7", topic: "mapreduce", q: "A job has 5 input splits and is configured with 2 reducers. How many output files does it write?", options: ["5", "2", "7", "1"], answer: 1, explain: "One output part file per reducer (part-0, part-1).", src: "MR slide 12" },
  { id: "pm8", topic: "mapreduce", q: "Which job can safely use its reducer as a combiner?", options: ["Average delay per airline", "Median price per route", "Total distance flown per airline (sum)", "Percentage of delayed flights"], answer: 2, explain: "Sums of partial sums give the right total. Averages, medians and percentages don't combine directly.", src: "MR slide 13" },
  { id: "pm9", topic: "mapreduce", q: "When the ApplicationMaster's job is small enough, what happens?", options: ["It runs as an uber task in the AM's JVM", "It's sent to the NameNode", "It's skipped", "It runs on every node"], answer: 0, explain: "No separate containers are started.", src: "MR slide 16" },
  { id: "pm10", topic: "mapreduce", q: "The client polls the ApplicationMaster for progress and gets a timeout. What does it do?", options: ["Fails the job", "Asks the ResourceManager for the new ApplicationMaster's address", "Restarts the NameNode", "Resubmits the job from scratch"], answer: 1, explain: "Transparent recovery after an AM failure.", src: "MR slide 19" },
  { id: "pm11", topic: "streaming", q: "Which input line does the streaming word-count reducer receive after `sort`?", options: ["`big\\t[1, 1]`", "`big\\t1` (one line per occurrence, sorted)", "`(big, 2)`", "`big 1 1`"], answer: 1, explain: "Streaming reducers get individual sorted lines, not lists.", src: "MR slide 25" },
  { id: "pm12", topic: "streaming", q: "Why does slide 27's reducer not need a final write after its loop, unlike slide 25's?", options: ["It doesn't need to emit anything", "groupby yields the last group too, so every key is emitted inside the loop", "It uses a combiner", "Python flushes automatically"], answer: 1, explain: "The slide 25 curkey loop must write the last key after the loop; groupby handles it.", src: "MR slides 25, 27" },
  { id: "pm13", topic: "spark", q: "Which RDD operation needs data from many partitions to be brought together by key?", options: ["map", "filter", "reduceByKey", "flatMap"], answer: 2, explain: "It requires a shuffle; map, filter and flatMap work partition by partition.", src: "Spark slide 7" },
  { id: "pm14", topic: "spark", q: "What does `sc.textFile(\"x.txt\").map(len).filter(lambda n: n > 0)` do when run alone?", options: ["Reads the file and filters it", "Builds a lineage but runs nothing, since there is no action", "Throws an error", "Prints the lengths"], answer: 1, explain: "Lazy evaluation: an action like count() or collect() would trigger it.", src: "Spark slide 7" },
  { id: "pm15", topic: "sparksql", q: "In Spark SQL, which clause would you use to keep only airlines with more than 100 flights?", options: ["WHERE count(*) > 100", "HAVING count(*) > 100", "ORDER BY count(*) > 100", "LIMIT 100"], answer: 1, explain: "A condition on an aggregate goes in HAVING.", src: "Spark slide 21" },
  { id: "pm16", topic: "sparksql", q: "Which DataFrame method from the lab drops rows with nulls in chosen columns?", options: ["df.drop(\"AverageTemperature\")", "df.na.drop(subset=[\"AverageTemperature\"])", "df.filter(\"null\")", "df.distinct()"], answer: 1, explain: "df.drop removes a column; na.drop removes rows with nulls.", src: "Lab" }
);

/* ---------------- True / False ---------------- */
CP.tf.push(
  // Spark Streaming
  { id: "tss1", topic: "sparkstream", q: "A DStream is a continuous sequence of RDDs, each holding the data from one batch interval.", answer: true, explain: "Any transformation on a DStream is applied to every RDD in it.", src: "Spark slides 27, 29" },
  { id: "tss2", topic: "sparkstream", q: "Spark Streaming processes each record individually the moment it arrives.", answer: false, explain: "It uses a micro-batch architecture.", src: "Spark slide 27" },
  { id: "tss3", topic: "sparkstream", q: "reduceByKey on a DStream combines values across all batches since the program started.", answer: false, explain: "It's stateless: within each time step only.", src: "Spark slide 33" },
  { id: "tss4", topic: "sparkstream", q: "Window duration and sliding duration must both be multiples of the batch interval.", answer: true, explain: "Windows are built from whole batches.", src: "Spark slide 35" },
  { id: "tss5", topic: "sparkstream", q: "If you don't set a sliding duration, it defaults to the batch interval.", answer: true, explain: "So a result is produced every batch.", src: "Spark slide 35" },
  { id: "tss6", topic: "sparkstream", q: "updateStateByKey eliminates the need for checkpointing.", answer: false, explain: "Stateful transformations REQUIRE checkpointing.", src: "Spark slide 34, Review 2" },
  { id: "tss7", topic: "sparkstream", q: "Calling socketTextStream() starts receiving data immediately.", answer: false, explain: "Nothing runs until ssc.start().", src: "Spark slide 26" },
  { id: "tss8", topic: "sparkstream", q: "By default, received data is replicated to two nodes so Spark Streaming can tolerate a single worker failure.", answer: true, explain: "Receivers replicate to another executor.", src: "Spark slide 31" },
  { id: "tss9", topic: "sparkstream", q: "Checkpointing lets a crashed streaming driver restart and continue from where the previous run got to.", answer: true, explain: "One of the two purposes of checkpointing (with getOrCreate).", src: "Spark slides 39-40" },
  { id: "tss10", topic: "sparkstream", q: "Stateless transformations like join() and cogroup() can combine data from multiple DStreams within each time step.", answer: true, explain: "Key/value DStreams have the same join operations as RDDs.", src: "Spark slide 33" },
  { id: "tss11", topic: "sparkstream", q: "Spark Streaming's main programming abstraction is the DataFrame.", answer: false, explain: "It's the DStream (discretized stream).", src: "Spark slide 27" },

  // ML (slide 8 is a flagged T/F)
  { id: "tml1", topic: "ml", q: "A transformer learns parameters from your data using the .fit() method.", answer: false, explain: "Transformers don't learn; they use .transform(). Estimators use .fit().", src: "ML slide 8" },
  { id: "tml2", topic: "ml", q: "An estimator's .fit() method returns a Model, and that Model is a transformer.", answer: true, explain: "E.g. LinearRegression.fit() → LinearRegressionModel.", src: "ML slide 8" },
  { id: "tml3", topic: "ml", q: "A Pipeline is a transformer, and a PipelineModel is an estimator.", answer: false, explain: "Reversed: Pipelines are estimators; PipelineModels (fitted pipelines) are transformers.", src: "ML slides 8, 14" },
  { id: "tml4", topic: "ml", q: "Transformers return a new DataFrame with one or more columns appended.", answer: true, explain: "E.g. VectorAssembler adds a features column.", src: "ML slides 8, 11" },
  { id: "tml5", topic: "ml", q: "Estimators are eagerly evaluated, while transformers are lazily evaluated.", answer: true, explain: "fit() kicks off Spark jobs.", src: "ML slide 13" },
  { id: "tml6", topic: "ml", q: "VectorAssembler scales and normalizes features before combining them.", answer: false, explain: "It only combines columns into a vector. Modify columns first if needed.", src: "ML slide 11" },
  { id: "tml7", topic: "ml", q: "One-hot encoding a column with 300 categories wastes lots of memory in Spark because it stores every zero.", answer: false, explain: "Spark uses SparseVectors.", src: "ML slide 15" },
  { id: "tml8", topic: "ml", q: "An R² of 0 means the model performs the same as always predicting the average label.", answer: true, explain: "1 = perfect; negative = worse than the mean.", src: "ML slide 20" },
  { id: "tml9", topic: "ml", q: "Decision trees require you to standardize or scale numeric features.", answer: false, explain: "Trees are scale-invariant.", src: "ML slide 22" },
  { id: "tml10", topic: "ml", q: "Very deep decision trees tend to overfit, while very shallow ones underfit.", answer: true, explain: "Depth is the longest root-to-leaf path.", src: "ML slide 22" },
  { id: "tml11", topic: "ml", q: "A hyperparameter is learned during training.", answer: false, explain: "Hyperparameters are set before training; parameters are learned.", src: "ML slide 31" },
  { id: "tml12", topic: "ml", q: "In k-fold cross-validation, every data point is used for validation exactly once.", answer: true, explain: "Each fold is the validation fold once.", src: "ML slide 33" },
  { id: "tml13", topic: "ml", q: "Tuning hyperparameters on the test set is fine because it's unseen data.", answer: false, explain: "Then it's no longer unseen; use a validation set or cross-validation.", src: "ML slide 32" },
  { id: "tml14", topic: "ml", q: "Random search of hyperparameters often outperforms a structured grid search.", answer: true, explain: "Libraries like Hyperopt help.", src: "ML slide 33" },
  { id: "tml15", topic: "ml", q: "Clustering is a supervised learning technique.", answer: false, explain: "It's unsupervised.", src: "ML slide 50" },
  { id: "tml16", topic: "ml", q: "spark.mllib is based on DataFrames, and spark.ml is based on RDDs.", answer: false, explain: "Reversed.", src: "ML slide 7" },

  // Pig
  { id: "tpg1", topic: "pig", q: "Pig Latin scripts are converted into MapReduce jobs that run on a Hadoop cluster.", answer: true, explain: "Pig is an abstraction on top of Hadoop.", src: "Pig slides 3, 9" },
  { id: "tpg2", topic: "pig", q: "In Pig, a schema is mandatory, as in SQL.", answer: false, explain: "Schema is optional; fields can be referred to as $0, $1….", src: "Pig slide 8" },
  { id: "tpg3", topic: "pig", q: "Pig Latin is a declarative language, while SQL is procedural.", answer: false, explain: "Reversed.", src: "Pig slide 8" },
  { id: "tpg4", topic: "pig", q: "The default delimiter of PigStorage is a tab.", answer: true, explain: "Use PigStorage(',') for CSV.", src: "Pig slide 16" },
  { id: "tpg5", topic: "pig", q: "Pig executes each statement as soon as you type it in the Grunt shell.", answer: false, explain: "Nothing runs until DUMP or STORE.", src: "Pig slide 20" },
  { id: "tpg6", topic: "pig", q: "Every tuple in a Pig bag must have the same number of fields.", answer: false, explain: "Bags have a flexible schema, unlike RDBMS tables.", src: "Pig slides 12-13" },
  { id: "tpg7", topic: "pig", q: "Pig's default JOIN is an inner join.", answer: true, explain: "Rows without matching keys aren't included.", src: "Pig slide 21" },
  { id: "tpg8", topic: "pig", q: "Pig and Hive are designed to replace relational databases.", answer: false, explain: "They suit large read-only data and batch processing; RDBMSs suit interactive use.", src: "Pig slide 26" },
  { id: "tpg9", topic: "pig", q: "The Pig compiler turns the optimized logical plan into a series of MapReduce jobs.", answer: true, explain: "Parser → optimizer → compiler → execution engine.", src: "Pig slide 10" },
  { id: "tpg10", topic: "pig", q: "In a Pig map, the key must be a chararray and must be unique.", answer: true, explain: "The value can be any type: [name#Raja, age#30].", src: "Pig slide 12" },

  // new pre-midterm
  { id: "tpm1", topic: "hadoop", q: "The NameNode stores file data blocks so it can serve them quickly to clients.", answer: false, explain: "DataNodes store blocks; the NameNode stores metadata only.", src: "Hadoop slide 17" },
  { id: "tpm2", topic: "hadoop", q: "Network bandwidth is the most precious resource in a data center, according to Hadoop's design.", answer: true, explain: "Hence data locality.", src: "Hadoop slide 9" },
  { id: "tpm3", topic: "mapreduce", q: "Map task output is written to HDFS so it survives node failures.", answer: false, explain: "It goes to the node's local disk, which is why completed maps get rerun when a NodeManager fails.", src: "MR slide 20" },
  { id: "tpm4", topic: "mapreduce", q: "One reduce task writes one output file (a part file) to HDFS.", answer: true, explain: "part-0, part-1…", src: "MR slide 12" },
  { id: "tpm5", topic: "mapreduce", q: "A combiner can change the final result of a job if the function isn't safe to combine (e.g. average).", answer: true, explain: "Average of averages ≠ overall average.", src: "MR slide 13" },
  { id: "tpm6", topic: "streaming", q: "In Hadoop Streaming, a mapper output line with no tab is treated as a key with a null value.", answer: true, explain: "The whole line becomes the key.", src: "MR slide 24" },
  { id: "tpm7", topic: "spark", q: "Spark is designed primarily for distributed storage.", answer: false, explain: "It focuses on computation and relies on Hadoop for storage.", src: "Spark slides 5-6" },
  { id: "tpm8", topic: "spark", q: "The SparkContext distributes application code to executors and collects results from them.", answer: true, explain: "Review 2 Q4: it connects to the cluster manager and tracks task progress.", src: "Review 2 SA 4, Spark slide 10" },
  { id: "tpm9", topic: "sparksql", q: "Spark SQL replaces Spark Core.", answer: false, explain: "It runs as a library on top of Spark Core.", src: "Spark slide 19, Review 2" },
  { id: "tpm10", topic: "bigdata", q: "Prescriptive analytics answers \"what if we try this, and what is the best scenario?\"", answer: true, explain: "Using optimization and randomized testing.", src: "Intro slide 21" }
);

/* ---------------- Short answer ---------------- */
CP.short.push(
  { id: "sa2-spark-faster", topic: "spark", review: 2, marks: 10, q: "What makes Apache Spark faster than traditional MapReduce frameworks, and how does it handle iterative algorithms?",
    points: ["MapReduce writes and reads disk on every iteration (I/O overhead)", "Spark keeps data in worker memory so intermediate results are reused without disk I/O", "DAG execution engine optimizes the data flow and steps", "Iterative algorithms (ML) cache datasets in memory", "Lower latency, faster convergence"],
    model: "Spark gets its speed from in-memory computing. In traditional MapReduce, every iteration has to write data to disk and read it back, which adds a lot of I/O overhead. Spark keeps data in the memory of the worker nodes as much as possible, so intermediate results are reused without repeated disk I/O. For iterative algorithms, Spark's DAG execution engine optimizes the whole data flow and its computation steps, and the dataset is cached in memory across iterations (for example while training a machine learning model), which reduces latency and lets the algorithm converge faster." },
  { id: "sa2-rdd-lineage", topic: "spark", review: 2, marks: 10, q: "Explain the concept of Resilient Distributed Datasets (RDDs) and how fault tolerance is achieved through lineage.",
    points: ["RDDs are immutable, partitioned collections distributed across the cluster", "The fundamental data structure; enable parallel processing", "Each RDD records the transformations that created it (lineage)", "A lost partition is rebuilt by reapplying the lineage to the source data", "No need for expensive replication"],
    model: "RDDs are immutable, partitioned collections of objects distributed across a cluster. They are Spark's fundamental data structure and allow large datasets to be processed in parallel. Fault tolerance comes from lineage: each RDD keeps track of the transformations that created it. If a partition is lost because a node fails, Spark reconstructs it by reapplying those transformations from the lineage graph to the original data source. Because RDDs are immutable, recomputing gives the same result, and Spark avoids expensive data replication across nodes." },
  { id: "sa2-transform-action", topic: "spark", review: 2, marks: 10, q: "How do transformations and actions differ in Spark, and why is lazy evaluation important?",
    points: ["Transformations (map, filter) define a new RDD from an existing one", "They are lazy: define the computation without running it", "Actions (count, saveAsTextFile) trigger execution and return results to the driver or write to storage", "Lazy evaluation lets Spark build a DAG execution plan for the whole job", "Avoids unnecessary computation and optimizes data flow"],
    model: "Transformations such as map and filter define a new RDD by applying a function to an existing RDD. They are lazily evaluated: they only define a computation and don't execute it. Actions such as count or saveAsTextFile trigger execution of the transformations, perform the computation, and return results to the driver program or write data to storage. Lazy evaluation matters because Spark can build an execution plan (a DAG) for the entire job before running it, so transformations are only applied when needed, unnecessary computations are avoided, and the data flow is optimized for better resource use." },
  { id: "sa2-sparkcontext", topic: "spark", review: 2, marks: 10, q: "Discuss the role of the SparkContext and how it interacts with the cluster manager.",
    points: ["Main entry point of a Spark application, in the driver", "Manages the application's lifecycle and coordinates task execution", "Connects to a cluster manager (YARN, Mesos, Kubernetes) which allocates CPU and memory", "Sends application code to executors, which run tasks and store RDDs", "Tracks task progress and collects results"],
    model: "The SparkContext is the main entry point for a Spark application and lives in the driver program. It manages the application's lifecycle and coordinates task execution. It connects to a cluster manager such as YARN, Mesos or Kubernetes, which allocates system resources like CPU and memory to the application. Once connected, the SparkContext sends the application code to the cluster's executors, which run the tasks and manage storage for RDDs, and it tracks task progress and collects the results from the executors." },
  { id: "sa2-pipeline-api", topic: "ml", review: 2, marks: 10, q: "Explain the purpose of the Pipeline API in Spark MLlib and its components.",
    points: ["Organizes a series of ML operations into one structured workflow, built on DataFrames", "Transformer: DataFrame in, DataFrame out, learns nothing (e.g. VectorAssembler), .transform()", "Estimator: learns from data with .fit() and returns a Model/transformer (e.g. LinearRegression)", "Pipeline chains transformers and estimators; fit() returns a PipelineModel used on new data", "Improves code reusability and organization"],
    model: "The Pipeline API organizes a series of machine learning operations into a structured workflow on top of DataFrames, so multiple data transformations and model-building steps are applied in the right order. Its components are: Transformers, which accept a DataFrame and return a modified DataFrame without learning anything from the data, through .transform() (for example VectorAssembler); Estimators, which learn parameters from data through .fit() and return a Model, which is a transformer (for example LinearRegression); and the Pipeline itself, which chains transformers and estimators into one workflow. A Pipeline is an estimator: pipeline.fit(trainDF) produces a PipelineModel, a transformer that applies every stage to new data with transform(testDF). This improves code reusability and organization, especially for complex workflows." },
  { id: "sa2-supervised", topic: "ml", review: 2, marks: 10, q: "Compare supervised and unsupervised learning in the context of Spark MLlib. Provide use cases for each.",
    points: ["Supervised: trained on labelled data (features paired with labels) to predict labels for new data", "Classification (discrete) and regression (continuous) use cases: fraud detection, house prices", "Unsupervised: unlabelled data, finds patterns/structure such as clusters", "Use cases: customer segmentation, dimensionality reduction as preprocessing", "MLlib examples: RandomForestClassifier / LinearRegression vs KMeans"],
    model: "Supervised learning trains a model on labelled data, where input features are paired with output labels, and the goal is to predict the label for new data. Labels can be discrete (classification, such as fraud detection) or continuous (regression, such as house price prediction). Unsupervised learning works with unlabelled data and finds patterns or structure, for example grouping similar data points into clusters; use cases include customer segmentation and dimensionality reduction as a preprocessing step. Spark MLlib supports both, with algorithms like RandomForestClassifier or LinearRegression for supervised learning and KMeans for unsupervised learning." },
  { id: "sa2-pig-advantages", topic: "pig", review: 2, marks: 10, q: "What are the advantages of Apache Pig over traditional MapReduce, and what kind of tasks is Pig best suited for?",
    points: ["High-level language (Pig Latin), concise and SQL-like vs verbose Java", "Easier for analysts and data scientists; ~10 lines vs ~200", "Abstracts MapReduce: focus on logic, not execution details", "Best for complex processing flows with multiple datasets", "ETL on structured/semi-structured/unstructured data; rapid prototyping"],
    model: "Apache Pig simplifies data processing by providing a high-level language, Pig Latin. Traditional MapReduce needs verbose Java code; Pig Latin is concise and SQL-like, so analysts and data scientists can use it, and a task needing about 200 lines of Java takes about 10 lines of Pig. Pig abstracts the complexity of MapReduce (scripts are compiled into MapReduce jobs), so developers focus on the data processing logic instead of execution details, and joins and nested data types come built in. Pig is best suited for complex processing flows involving multiple datasets, ETL tasks on structured, semi-structured or unstructured data, and rapid prototyping of data transformations." },
  { id: "sa2-pig-arch", topic: "pig", review: 2, marks: 10, q: "Describe Apache Pig's architecture and the role of its components.",
    points: ["Parser: checks syntax and types, produces a DAG logical plan (operators = nodes, data flows = edges)", "Optimizer: logical optimizations such as projection and pushdown", "Compiler: turns the optimized plan into a series of MapReduce jobs", "Execution engine: submits the jobs to Hadoop in order and runs them", "Scripts enter through Grunt shell, Pig Server/embedded; results on HDFS"],
    model: "Pig Latin scripts enter through the Grunt shell, a script file or the embedded Pig Server, and pass through four components. The Parser validates the syntax, does type checking and generates a directed acyclic graph (DAG) that represents the logical plan, with logical operators as nodes and data flows as edges. The Optimizer improves the logical plan with techniques like projection and pushdown. The Compiler translates the optimized logical plan into a series of MapReduce jobs. The Execution Engine submits those MapReduce jobs to the Hadoop cluster in sorted order and executes them, reading from and writing to HDFS. This lets Pig process large data efficiently while hiding the complexity of MapReduce from the user." },
  { id: "sa2-checkpointing", topic: "sparkstream", review: 2, marks: 10, q: "Explain the concept of checkpointing in Spark Streaming and its importance.",
    points: ["Saves intermediate state of the computation to reliable storage (HDFS, S3)", "Critical for fault tolerance: recover without starting from scratch", "Limits how much state must be recomputed from lineage", "Metadata checkpointing (context, configuration) vs data checkpointing (RDDs with intermediate state)", "Required for stateful transformations like updateStateByKey; enables driver recovery (getOrCreate)"],
    model: "Checkpointing in Spark Streaming periodically saves the intermediate state of the computation to reliable storage such as HDFS or S3. It is critical for fault tolerance because the application can recover from failures without starting from scratch: recomputing state through lineage could otherwise go back to the beginning of the program, so the checkpoint limits how far back Spark has to go. It also gives the driver fault tolerance: a restarted driver created with StreamingContext.getOrCreate reads the checkpoint and continues where the previous run stopped. There are two types: metadata checkpointing, which saves the streaming context and its configuration, and data checkpointing, which saves the RDDs representing the intermediate state of the stream. Checkpointing is especially important (and required) for stateful transformations like updateStateByKey, which depend on historical data." },
  { id: "sa2-kfold", topic: "ml", review: 2, marks: 10, q: "Discuss hyperparameter tuning in Spark MLlib and the benefits of k-fold cross-validation.",
    points: ["Hyperparameters (numTrees, maxDepth) are set before training, not learned", "Tuning = choosing the best configuration via grid search or random search", "k-fold: split training data into k folds, train on k−1, validate on the remaining one, k times, average", "Benefits: every point used for training and validation, robust estimate, prevents overfitting to one split", "Spark: ParamGridBuilder + evaluator + CrossValidator(numFolds)"],
    model: "Hyperparameters are values that control a model's learning or structure and are not learned during training, such as the number of trees or the maximum depth of a random forest. Hyperparameter tuning selects the best configuration, using grid search or random search. In Spark, you define the estimator, list the values to try with ParamGridBuilder, choose an evaluator (e.g. RegressionEvaluator with RMSE) and run a CrossValidator. K-fold cross-validation splits the training data into k folds; for each configuration it trains on k−1 folds and validates on the remaining fold, k times, and averages the metric. Its benefits: it reduces bias because every data point is used for both training and validation; it gives a more robust estimate of performance on unseen data; it helps prevent overfitting to a single split; and it avoids wasting 25% of the training data on a fixed validation set." },
  { id: "sa2-stateless-stateful", topic: "sparkstream", marks: 10, q: "Compare stateless and stateful transformations in Spark Streaming, with examples. (Slide 38: the prof said this is probably on the quiz.)",
    points: ["Stateless: each batch processed independently; no information from the past", "Examples: map, filter, reduceByKey within one batch; join/cogroup within a time step", "Stateful: uses data or results from previous batches", "Two kinds: windowed operations (e.g. last 30 seconds) and updateStateByKey/mapWithState (running totals, sessions)", "Stateful requires checkpointing to reliable storage"],
    model: "In a stateless transformation the processing of each batch is completely independent of earlier batches. These are the usual RDD transformations applied to every RDD in the DStream, such as map(), filter() and reduceByKey(); reduceByKey only reduces within a single batch, and join() or cogroup() combine DStreams within the same time step. They are simple and efficient but can't use information from the past. A stateful transformation uses data or intermediate results from previous batches to compute the current batch. There are two main kinds: windowed operations, which act on a sliding window of recent batches (for example the top topics over the last 30 seconds, updated every 10 seconds), and updateStateByKey (or mapWithState), which keeps state for each key across events, such as a running total or a user's session. Stateful transformations require checkpointing to reliable storage like HDFS so the state can be recovered after a failure." },
  { id: "sa2-stream-arch", topic: "sparkstream", marks: 10, q: "Describe the architecture of Spark Streaming: how data gets from a source like Kafka to processed results. (The prof's email called this a rich source for the quiz.)",
    points: ["Sources: Kafka, Flume, HDFS, TCP sockets", "StreamingContext in the driver launches receivers (tasks in executors) that store data in memory and replicate it to another executor", "Micro-batches: data in each batch interval (500 ms to seconds) forms an RDD; a DStream is a sequence of these RDDs", "StreamingContext schedules Spark jobs on each batch; results pushed to databases, files, dashboards", "Fault tolerance: replication + lineage + periodic checkpointing to HDFS/S3 (exactly-once)"],
    model: "Data arrives from input sources such as Kafka, Flume, HDFS or TCP sockets. In the driver program, the StreamingContext (the entry point, which also creates a SparkContext) launches receivers for each source; receivers are tasks running inside executors that collect the data, store it in executor memory like cached RDDs, and by default replicate it to a second executor. Spark Streaming uses a micro-batch architecture: data that arrives during each batch interval (typically 500 ms to a few seconds) is grouped into a batch, and each batch becomes an RDD. The resulting sequence of RDDs is a DStream. The StreamingContext periodically schedules Spark jobs that apply the DStream's transformations to each RDD and combine it with results from earlier batches, and the outputs are pushed in batches to databases, file systems or dashboards. For fault tolerance it relies on replication, RDD lineage, and periodic checkpointing of data and state to reliable storage such as HDFS or S3, which gives exactly-once semantics and lets the driver recover." },
  { id: "sa2-ml-pipeline-steps", topic: "ml", marks: 10, q: "Walk through the steps to build, train and evaluate a linear regression model in Spark ML, from raw DataFrame to RMSE. Why use a Pipeline? (Slide 16-17 style.)",
    points: ["randomSplit into train/test (80/20, seed) to measure generalization", "StringIndexer + OneHotEncoder for categorical columns", "VectorAssembler to combine features into one vector column", "LinearRegression estimator; Pipeline(stages=[...]).fit(trainDF) → PipelineModel; transform(testDF) adds prediction", "RegressionEvaluator RMSE / R²; Pipeline keeps steps in order, reusable, applied identically to test data"],
    model: "First split the data once into training and test sets with randomSplit([.8, .2], seed=42), so the test set can measure how the model generalizes. Categorical string columns are converted with a StringIndexer (string to index, handleInvalid=\"skip\" for unseen categories) followed by a OneHotEncoder, because numeric codes would invent an ordering. A VectorAssembler then combines the one-hot columns and numeric columns into a single features vector, since Spark ML models need all features in one column. LinearRegression(featuresCol=\"features\", labelCol=\"price\") is the estimator. These stages go into Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr]); pipeline.fit(trainDF) returns a PipelineModel, and pipelineModel.transform(testDF) adds a prediction column. Finally a RegressionEvaluator with metricName=\"rmse\" (or \"r2\") scores the predictions, compared against a baseline that predicts the mean. The Pipeline means you don't have to remember which steps to apply in which order; the exact same preparation is applied to the test data, it decides fit vs transform for each stage, and the whole thing can be saved and loaded as one PipelineModel." }
);

/* ---------------- Code Lab ---------------- */
const TRIP_SAMPLE = R`1,2015-01-15 19:05:39,2015-01-15 19:23:42,1,1.59,...,12.0,3.25,17.05
2,2015-01-10 20:33:38,2015-01-10 20:53:28,1,3.30,...,14.5,2.00,17.80
1,2015-01-10 20:33:38,2015-01-10 20:43:41,0,1.80,...,9.5,0.00,10.80`;
const TRIP_SCHEMA = R`AS (VendorID:chararray, pickup_datetime:chararray, dropoff_datetime:chararray,
    passenger_count:int, trip_distance:float, pickup_longitude:float,
    pickup_latitude:float, dropoff_longitude:float, dropoff_latitude:float,
    fare_amount:float, tip_amount:float, total_amount:float)`;
const AIRBNB_NOTE = "airbnbDF columns: neighbourhood_cleansed, room_type, property_type (strings), bedrooms, bathrooms, number_of_reviews, … (doubles), price (double, the label).";

CP.code.push(
  /* ===================== Pig Latin ===================== */
  { id: "pig-load-read", track: "pig", level: 1, type: "mcq", title: "Reading a LOAD statement",
    lang: "pig", code: R`records = LOAD '/training/playArea/pig/excite-small.log'
          USING PigStorage()
          AS (userId:chararray, timestamp:long, query:chararray);`,
    q: "Which statement about this LOAD is correct?",
    options: ["PigStorage() splits each line on tabs (the default) and AS names and types the three fields", "It splits each line on commas", "It runs immediately and prints the records", "The path could be written without quotes"],
    answer: 0, explain: "Default load function PigStorage, default delimiter tab. Paths go in single quotes. Nothing runs until DUMP or STORE.", src: "Pig slide 16" },
  { id: "pig-group-read", track: "pig", level: 1, type: "mcq", title: "What does GROUP produce?",
    lang: "pig", code: R`grunt> chars = LOAD '/training/playArea/pig/b.txt' AS (c:chararray);
grunt> charGroup = GROUP chars BY c;
grunt> describe charGroup;`,
    q: "The file contains the letters a, k, c, a, k, c, a. What does `describe charGroup` print, and what does a dumped tuple look like?",
    options: ["`charGroup: {group: chararray, chars: {(c: chararray)}}` and tuples like `(a,{(a),(a),(a)})`", "`charGroup: {c: chararray, count: long}` and tuples like `(a,3)`", "`charGroup: {c: chararray}` and tuples like `(a)`", "`charGroup: {chars: chararray}` and tuples like `{a, a, a}`"],
    answer: 0, explain: "GROUP makes a field called group (the unique key) and a bag named after the original relation holding every matching tuple. To count, you'd add FOREACH charGroup GENERATE group, COUNT(chars).", src: "Pig slide 18" },
  { id: "pig-lazy-read", track: "pig", level: 1, type: "mcq", title: "When does Pig run?",
    lang: "pig", code: R`records = LOAD 'excite-small.log' AS (userId:chararray, timestamp:long, query:chararray);  -- 1
toPrint = LIMIT records 5;                                                             -- 2
DESCRIBE toPrint;                                                                      -- 3
DUMP toPrint;                                                                          -- 4`,
    q: "On which line does Pig actually launch MapReduce work?",
    options: ["Line 4 (DUMP)", "Line 1 (LOAD)", "Line 2 (LIMIT)", "Line 3 (DESCRIBE)"],
    answer: 0, explain: "No action is taken until DUMP or STORE. DESCRIBE only shows the schema.", src: "Pig slides 17, 20" },
  { id: "pig-trip-fill", track: "pig", level: 2, type: "fill", title: "Fill in the yellow taxi script (slides 27-28)",
    lang: "pig", prompt: "Load the CSV, drop invalid trips, and compute the average fare, distance and tip per passenger count. (Keywords aren't case sensitive.)",
    code: R`data = ⟦LOAD⟧ 'yellow_tripdata_2015-01.csv'
    ⟦USING⟧ PigStorage('⟦,⟧')
    AS (VendorID:chararray, pickup_datetime:chararray, dropoff_datetime:chararray,
        passenger_count:int, trip_distance:float, pickup_longitude:float,
        pickup_latitude:float, dropoff_longitude:float, dropoff_latitude:float,
        fare_amount:float, tip_amount:float, total_amount:float);

filtered_data = ⟦FILTER⟧ data BY (passenger_count > 0) ⟦AND⟧ (trip_distance > 0) AND (fare_amount > 0);

group_by_passengers = ⟦GROUP⟧ filtered_data BY passenger_count;

avg_metrics = ⟦FOREACH⟧ group_by_passengers ⟦GENERATE⟧ ⟦group⟧ AS passenger_count,
    ⟦AVG⟧(filtered_data.fare_amount) AS avg_fare,
    AVG(filtered_data.trip_distance) AS avg_distance,
    AVG(⟦filtered_data⟧.tip_amount) AS avg_tip;

⟦STORE|DUMP⟧ avg_metrics INTO 'avg_metrics' USING PigStorage(',');`,
    explain: "LOAD … USING PigStorage(',') AS (schema) → FILTER … BY … AND … → GROUP … BY → FOREACH grouped GENERATE group AS key, AVG(relation.field). Inside FOREACH you refer to the bag by the name of the relation you grouped (filtered_data).", src: "Pig slides 27-28" },
  { id: "pig-join-fill", track: "pig", level: 2, type: "fill", title: "Fill in the inner join (slide 22)",
    lang: "pig", prompt: "Join posts and likes on the user field.",
    code: R`posts = ⟦load⟧ '/training/data/user-posts.txt' using PigStorage('⟦,⟧')
        as (user:chararray, post:chararray, date:long);

likes = load '/training/data/user-likes.txt' using ⟦PigStorage⟧(',')
        as (user:chararray, likes:int, date:long);

userInfo = ⟦join⟧ posts ⟦by⟧ user, likes by ⟦user⟧;

⟦dump⟧ userInfo;`,
    explain: "Load each input into a bag, then `join a by key, b by key`. The default is an inner join: users with no match in both files are dropped.", src: "Pig slides 21-22" },
  { id: "pig-trip-parsons", track: "pig", level: 3, type: "parsons", title: "Arrange the taxi feature script (slides 30-31)",
    lang: "pig", prompt: "Build the script that labels generous tips with the UDF and computes features per passenger count. Two lines are distractors.",
    lines: [
      [R`REGISTER 'TipPercentage.py' USING jython AS myudfs;`, 0],
      [R`data = LOAD 'yellow_tripdata_2015-01.csv' USING PigStorage(',') AS (...);`, 1],
      [R`filtered_data = FILTER data BY (passenger_count > 0) AND (trip_distance > 0) AND (total_amount > 0);`, 2],
      [R`labeled_data = FOREACH filtered_data GENERATE passenger_count, fare_amount, trip_distance, myudfs.is_generous_tip(tip_amount, total_amount) AS generous_tip_label;`, 3],
      [R`grouped_data = GROUP labeled_data BY passenger_count;`, 4],
      [R`features = FOREACH grouped_data GENERATE group AS passenger_count, AVG(labeled_data.fare_amount) AS avg_fare, SUM(labeled_data.generous_tip_label) AS generous_tip_count, COUNT(labeled_data) AS total_trips;`, 5],
      [R`STORE features INTO 'trip_features_with_generous_tip_label' USING PigStorage(',');`, 6]
    ],
    distractors: [R`DUMP data;`, R`grouped_data = GROUP data BY passenger_count;`],
    explain: "REGISTER the UDF first, then LOAD → FILTER → FOREACH (apply the UDF) → GROUP the labeled data → FOREACH with aggregates → STORE. Grouping the raw data would skip the filter and the label.", src: "Pig slides 29-31" },
  { id: "pig-metrics-write", track: "pig", level: 4, type: "write", title: "Write: averages per passenger count",
    lang: "pig",
    prompt: "`filtered_data` is already loaded and filtered (slide 27). Write the two statements that group it by `passenger_count` and compute the average fare, distance and tip for each group, then print the result.",
    data: TRIP_SCHEMA,
    expected: R`(1,11.9,2.6,1.4)
(2,12.6,2.9,1.2)
...`,
    starter: R`-- filtered_data is ready
`,
    hints: [
      "Two steps: GROUP, then FOREACH … GENERATE.",
      "`group_by_passengers = GROUP filtered_data BY passenger_count;`",
      "`FOREACH group_by_passengers GENERATE group AS passenger_count, AVG(filtered_data.fare_amount) AS avg_fare, …;` then `DUMP`."
    ],
    solution: R`group_by_passengers = GROUP filtered_data BY passenger_count;

avg_metrics = FOREACH group_by_passengers GENERATE
    group AS passenger_count,
    AVG(filtered_data.fare_amount) AS avg_fare,
    AVG(filtered_data.trip_distance) AS avg_distance,
    AVG(filtered_data.tip_amount) AS avg_tip;

DUMP avg_metrics;`,
    rubric: [
      { pts: 2, text: "GROUP filtered_data BY passenger_count" },
      { pts: 1, text: "FOREACH over the grouped relation with GENERATE" },
      { pts: 1, text: "Uses group (AS passenger_count) as the key" },
      { pts: 2, text: "AVG of fare_amount, trip_distance and tip_amount, referenced through the bag (filtered_data.field)" },
      { pts: 1, text: "DUMP or STORE to produce output" }
    ],
    checks: [
      { re: "group\\s+filtered_data\\s+by\\s+passenger_count", label: "GROUP … BY passenger_count", flags: "i" },
      { re: "foreach[\\s\\S]*generate", label: "FOREACH … GENERATE", flags: "i" },
      { re: "avg\\s*\\(\\s*filtered_data\\.", label: "AVG over the bag's fields", flags: "i" },
      { re: "dump|store", label: "DUMP or STORE", flags: "i" }
    ],
    explain: "This is slide 28. Remember that after GROUP the bag keeps the original relation's name, so the fields are filtered_data.fare_amount, not just fare_amount.", src: "Pig slide 28" },
  { id: "pig-ex-vendor", track: "pig", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: trips and revenue per vendor (Pig)",
    lang: "pig",
    prompt: "Slide 28's step 4: \"Calculate total number of trips and total revenue.\" Write a complete Pig Latin script that loads `yellow_tripdata_2015-01.csv` (comma separated, schema below), removes trips with passenger_count, trip_distance or fare_amount ≤ 0, and stores, **for each VendorID**, the number of trips and the total revenue (sum of total_amount) into `vendor_stats`.",
    data: TRIP_SAMPLE + "\n\n" + TRIP_SCHEMA,
    expected: R`1,1520332,23810493.55
2,1708210,27013002.10`,
    starter: R`data = LOAD 'yellow_tripdata_2015-01.csv'
`,
    hints: [
      "Same shape as slides 27-28: LOAD → FILTER → GROUP → FOREACH → STORE.",
      "Group by VendorID instead of passenger_count.",
      "`FOREACH … GENERATE group AS VendorID, COUNT(filtered_data) AS total_trips, SUM(filtered_data.total_amount) AS total_revenue;`"
    ],
    solution: R`data = LOAD 'yellow_tripdata_2015-01.csv'
    USING PigStorage(',')
    AS (VendorID:chararray, pickup_datetime:chararray, dropoff_datetime:chararray,
        passenger_count:int, trip_distance:float, pickup_longitude:float,
        pickup_latitude:float, dropoff_longitude:float, dropoff_latitude:float,
        fare_amount:float, tip_amount:float, total_amount:float);

-- Filter out records with missing or invalid values
filtered_data = FILTER data BY (passenger_count > 0) AND (trip_distance > 0) AND (fare_amount > 0);

group_by_vendor = GROUP filtered_data BY VendorID;

vendor_stats = FOREACH group_by_vendor GENERATE
    group AS VendorID,
    COUNT(filtered_data) AS total_trips,
    SUM(filtered_data.total_amount) AS total_revenue;

STORE vendor_stats INTO 'vendor_stats' USING PigStorage(',');`,
    trace: [
      { phase: "input", label: "LOAD", rows: ["(1, …, 1, 1.59, …, 17.05)", "(2, …, 1, 3.30, …, 17.80)", "(1, …, 0, 1.80, …, 10.80)"] },
      { phase: "map", label: "FILTER", rows: ["(1, …, 1, 1.59, …, 17.05)", "(2, …, 1, 3.30, …, 17.80)", "passenger_count 0 dropped"] },
      { phase: "shuffle", label: "GROUP BY VendorID", rows: ["(1, {(1, …, 17.05)})", "(2, {(2, …, 17.80)})"] },
      { phase: "reduce", label: "FOREACH: COUNT, SUM", rows: ["(1, 1, 17.05)", "(2, 1, 17.80)"] }
    ],
    rubric: [
      { pts: 2, text: "LOAD with USING PigStorage(',') and an AS schema" },
      { pts: 2, text: "FILTER with the three conditions joined by AND" },
      { pts: 2, text: "GROUP filtered_data BY VendorID" },
      { pts: 3, text: "FOREACH … GENERATE group, COUNT(filtered_data), SUM(filtered_data.total_amount) with names" },
      { pts: 1, text: "STORE INTO 'vendor_stats' (USING PigStorage(','))" }
    ],
    checks: [
      { re: "load\\s+'[^']+'", label: "LOAD with a quoted path", flags: "i" },
      { re: "pigstorage\\s*\\(\\s*','\\s*\\)", label: "PigStorage(',')", flags: "i" },
      { re: "filter[\\s\\S]*and[\\s\\S]*and", label: "FILTER with AND conditions", flags: "i" },
      { re: "group\\s+\\w+\\s+by\\s+vendorid", label: "GROUP … BY VendorID", flags: "i" },
      { re: "count\\s*\\(", label: "COUNT the trips", flags: "i" },
      { re: "sum\\s*\\(\\s*\\w+\\.total_amount", label: "SUM(….total_amount)", flags: "i" },
      { re: "store\\s+\\w+\\s+into", label: "STORE … INTO", flags: "i" }
    ],
    explain: "Every Pig aggregation answer has this skeleton. The marks are in FILTER conditions, the GROUP key, and referring to fields through the bag inside FOREACH.", src: "Pig slides 16, 20, 27-28" },
  { id: "pig-ex-flights", track: "pig", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: flight delays per origin (Pig)",
    lang: "pig",
    prompt: "Write a Pig Latin script for the flight on-time CSV from the MapReduce slides (`date, airline, flightnum, origin, dest, dep, dep_delay, arv, arv_delay, airtime, distance`). For each **origin** airport, compute the number of flights and the average departure delay, keep only flights with a positive distance, and print the result.",
    data: R`2014-04-01,19805,1,JFK,LAX,0854,-6.00,1217,2.00,355.00,2475.00
2014-04-01,19805,2,LAX,JFK,0944,14.00,1736,-29.00,269.00,2475.00`,
    expected: R`(JFK,1,-6.0)
(LAX,1,14.0)`,
    starter: R`flights = LOAD 'flights.csv'
`,
    hints: [
      "Give every column a name and type in AS (…). Delays and distance are floats.",
      "FILTER flights BY distance > 0, then GROUP … BY origin.",
      "FOREACH … GENERATE group AS origin, COUNT(valid) AS num_flights, AVG(valid.dep_delay) AS avg_delay; then DUMP."
    ],
    solution: R`flights = LOAD 'flights.csv'
    USING PigStorage(',')
    AS (date:chararray, airline:chararray, flightnum:int, origin:chararray,
        dest:chararray, dep:chararray, dep_delay:float, arv:chararray,
        arv_delay:float, airtime:float, distance:float);

valid = FILTER flights BY distance > 0;

by_origin = GROUP valid BY origin;

delays = FOREACH by_origin GENERATE
    group AS origin,
    COUNT(valid) AS num_flights,
    AVG(valid.dep_delay) AS avg_delay;

DUMP delays;`,
    rubric: [
      { pts: 3, text: "LOAD with PigStorage(',') and a schema naming origin, dep_delay and distance" },
      { pts: 1, text: "FILTER on distance > 0" },
      { pts: 2, text: "GROUP … BY origin" },
      { pts: 3, text: "FOREACH … GENERATE group, COUNT(bag), AVG(bag.dep_delay)" },
      { pts: 1, text: "DUMP (or STORE) the result" }
    ],
    checks: [
      { re: "pigstorage\\s*\\(\\s*','\\s*\\)", label: "PigStorage(',')", flags: "i" },
      { re: "dep_delay\\s*:\\s*(float|double)", label: "dep_delay typed as a number", flags: "i" },
      { re: "filter[\\s\\S]*distance\\s*>\\s*0", label: "FILTER distance > 0", flags: "i" },
      { re: "group\\s+\\w+\\s+by\\s+origin", label: "GROUP … BY origin", flags: "i" },
      { re: "avg\\s*\\(\\s*\\w+\\.dep_delay", label: "AVG(….dep_delay)", flags: "i" },
      { re: "dump|store", label: "DUMP or STORE", flags: "i" }
    ],
    explain: "Compare with the Hadoop Streaming version on MR slides 26-27: the mapper/reducer pair becomes GROUP + FOREACH with AVG. That's the \"10 lines instead of 200\" advantage.", src: "Pig slides 16-20, MR slide 26" },

  /* ===================== Spark MLlib ===================== */
  { id: "ml-roles-read", track: "ml", level: 1, type: "mcq", title: "Transformer or estimator? (slide 8)",
    lang: "python", code: R`vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
lr = LinearRegression(featuresCol="features", labelCol="price")
pipeline = Pipeline(stages=[vecAssembler, lr])
pipelineModel = pipeline.fit(trainDF)`,
    q: "Which list classifies these four objects correctly?",
    options: ["vecAssembler: transformer · lr: estimator · pipeline: estimator · pipelineModel: transformer", "vecAssembler: estimator · lr: transformer · pipeline: transformer · pipelineModel: estimator", "All four are estimators", "vecAssembler: transformer · lr: estimator · pipeline: transformer · pipelineModel: estimator"],
    answer: 0, explain: "Transformers .transform() and learn nothing; estimators .fit() and return a model (a transformer). A Pipeline is an estimator; its fitted PipelineModel is a transformer. The prof said slide 8 is a T/F question.", src: "ML slides 8, 14" },
  { id: "ml-sparse-read", track: "ml", level: 1, type: "mcq", title: "Reading a SparseVector",
    lang: "python", code: R`SparseVector(10, [3, 5], [7, 2])`,
    q: "Which dense vector is this?",
    options: ["DenseVector(0, 0, 0, 7, 0, 2, 0, 0, 0, 0)", "DenseVector(3, 5, 7, 2)", "DenseVector(0, 0, 7, 0, 2, 0, 0, 0, 0, 0)", "DenseVector(10, 3, 5, 7, 2)"],
    answer: 0, explain: "SparseVector(size, indices, values): size 10, value 7 at index 3 and 2 at index 5 (0-based). Spark stores one-hot vectors this way so zeros cost nothing.", src: "ML slide 15" },
  { id: "ml-grid-read", track: "ml", level: 1, type: "mcq", title: "How many models does CrossValidator train?",
    lang: "python", code: R`paramGrid = (ParamGridBuilder()
             .addGrid(rf.maxDepth, [2, 4, 6])
             .addGrid(rf.numTrees, [10, 100])
             .build())

cv = CrossValidator(estimator=pipeline, evaluator=evaluator,
                    estimatorParamMaps=paramGrid, numFolds=3, seed=42)
cvModel = cv.fit(trainDF)`,
    q: "How many hyperparameter configurations are there, and how many models are trained during cross-validation?",
    options: ["6 configurations, 18 models (6 × 3 folds)", "5 configurations, 15 models", "6 configurations, 6 models", "3 configurations, 9 models"],
    answer: 0, explain: "3 maxDepth values × 2 numTrees values = 6. Each is trained on k−1 folds k times: 6 × 3 = 18 (then the best is refit on all the training data). The best on the slides: maxDepth=6, numTrees=100.", src: "ML slides 34-36" },
  { id: "ml-predict-read", track: "ml", level: 1, type: "mcq", title: "What does transform(testDF) add?",
    lang: "python", code: R`pipeline = Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])
pipelineModel = pipeline.fit(trainDF)
predDF = pipelineModel.transform(testDF)
predDF.select("features", "price", "prediction").show(5)`,
    q: "What is `predDF`?",
    options: ["testDF with the indexed, one-hot, features and prediction columns appended by each stage", "Only the prediction column", "The trained coefficients", "trainDF with predictions"],
    answer: 0, explain: "Each transformer appends columns. Every stage learned on trainDF is applied to testDF in the same order, which is the point of a Pipeline.", src: "ML slides 14, 17" },
  { id: "ml-lr-fill", track: "ml", level: 2, type: "fill", title: "Fill in the linear regression pipeline (slides 10-14)",
    lang: "python", prompt: "Split the data, assemble features, train linear regression in a pipeline, and predict.",
    code: R`trainDF, testDF = airbnbDF.⟦randomSplit⟧([.8, .2], seed=42)

vecAssembler = ⟦VectorAssembler⟧(inputCols=["bedrooms"], outputCol="⟦features⟧")
lr = LinearRegression(featuresCol="features", ⟦labelCol⟧="price")

pipeline = Pipeline(⟦stages⟧=[vecAssembler, lr])
pipelineModel = pipeline.⟦fit⟧(trainDF)

predDF = pipelineModel.⟦transform⟧(testDF)
predDF.select("bedrooms", "features", "price", "⟦prediction⟧").show(10)`,
    explain: "randomSplit for train/test → VectorAssembler into one features column → LinearRegression estimator → Pipeline(stages=[…]).fit(trainDF) → PipelineModel.transform(testDF) adds prediction.", src: "ML slides 10-14" },
  { id: "ml-ohe-fill", track: "ml", level: 2, type: "fill", title: "Fill in the one-hot encoding pipeline (slide 16, prof flagged)",
    lang: "python", prompt: "Index and one-hot encode the string columns, then assemble everything except the label into features.",
    code: R`categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "⟦string⟧"]
indexOutputCols = [x + "Index" for x in categoricalCols]
oheOutputCols = [x + "OHE" for x in categoricalCols]

stringIndexer = ⟦StringIndexer⟧(inputCols=categoricalCols,
                              outputCols=⟦indexOutputCols⟧,
                              handleInvalid="⟦skip⟧")
oheEncoder = ⟦OneHotEncoder⟧(inputCols=indexOutputCols,
                           outputCols=oheOutputCols)

numericCols = [field for (field, dataType) in trainDF.dtypes
               if ((dataType == "⟦double⟧") & (field != "⟦price⟧"))]
assemblerInputs = ⟦oheOutputCols⟧ + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

lr = LinearRegression(labelCol="price", featuresCol="features")
pipeline = Pipeline(stages=[stringIndexer, ⟦oheEncoder⟧, vecAssembler, lr])`,
    explain: "String columns → StringIndexer (strings to indices; skip unseen categories) → OneHotEncoder (indices to binary vectors) → combine with the numeric columns (excluding the label, price) in VectorAssembler → model. Order in the pipeline matters: indexer, encoder, assembler, model.", src: "ML slides 16-17" },
  { id: "ml-cv-parsons", track: "ml", level: 3, type: "parsons", title: "Arrange the hyperparameter search (slides 34-35)",
    lang: "python", prompt: "Put the random forest cross-validation in order: estimator, grid, evaluator, cross-validator, fit. Two lines are distractors.",
    lines: [
      [R`rf = RandomForestRegressor(labelCol="price", maxBins=40, seed=42)`, 0],
      [R`pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])`, 1],
      [R`paramGrid = ParamGridBuilder().addGrid(rf.maxDepth, [2, 4, 6]).addGrid(rf.numTrees, [10, 100]).build()`, 2],
      [R`evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")`, 2],
      [R`cv = CrossValidator(estimator=pipeline, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3, seed=42)`, 3],
      [R`cvModel = cv.fit(trainDF)`, 4],
      [R`list(zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics))`, 5]
    ],
    distractors: [R`cvModel = cv.fit(testDF)`, R`paramGrid = ParamGridBuilder().addGrid(rf.maxDepth, [2, 4, 6]).fit(trainDF)`],
    explain: "1. estimator (pipeline with rf) → 2. ParamGridBuilder → 3. evaluator (these two can swap) → 4. CrossValidator → fit on TRAINING data → inspect avgMetrics. Fitting on testDF would tune on the test set.", src: "ML slides 34-36" },
  { id: "ml-eval-write", track: "ml", level: 4, type: "write", title: "Write: evaluate with RMSE and R²",
    lang: "python",
    prompt: "`predDF` has columns `price` (label) and `prediction`. Create a RegressionEvaluator, compute the RMSE and the R², and print both.",
    data: R`predDF: features | price | prediction`,
    expected: R`RMSE is 220.6
R2 is 0.159854`,
    starter: R`from pyspark.ml.evaluation import RegressionEvaluator
`,
    hints: [
      "`RegressionEvaluator(labelCol=\"price\", predictionCol=\"prediction\", metricName=\"rmse\")`",
      "`rmse = regressionEvaluator.evaluate(predDF)`",
      "For R², change the metric: `regressionEvaluator.setMetricName(\"r2\").evaluate(predDF)`."
    ],
    solution: R`from pyspark.ml.evaluation import RegressionEvaluator

regressionEvaluator = RegressionEvaluator(predictionCol="prediction",
                                          labelCol="price",
                                          metricName="rmse")
rmse = regressionEvaluator.evaluate(predDF)
print(f"RMSE is {rmse:.1f}")

r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
print(f"R2 is {r2}")`,
    rubric: [
      { pts: 2, text: "Creates RegressionEvaluator with labelCol=\"price\" and predictionCol=\"prediction\"" },
      { pts: 2, text: "metricName=\"rmse\" and evaluate(predDF)" },
      { pts: 2, text: "Computes R² (metricName \"r2\")" },
      { pts: 1, text: "Prints both" }
    ],
    checks: [
      { re: "RegressionEvaluator\\s*\\(", label: "Creates a RegressionEvaluator" },
      { re: "labelCol\\s*=\\s*[\"']price", label: "labelCol=\"price\"" },
      { re: "[\"']rmse[\"']", label: "Uses rmse" },
      { re: "[\"']r2[\"']", label: "Uses r2" },
      { re: "\\.evaluate\\s*\\(\\s*predDF", label: "evaluate(predDF)" }
    ],
    explain: "RMSE closer to 0 is better and must be compared with a baseline; R² is 1 when perfect, 0 for predicting the mean, negative when worse.", src: "ML slides 18-20, Lab" },
  { id: "ml-kmeans-write", track: "ml", level: 4, type: "write", title: "Write: k-means on earthquake locations (slide 51)",
    lang: "python",
    prompt: "`training` is an RDD of (latitude, longitude) vectors and `k = int(sys.argv[2])`. Train a k-means model, print the cluster centers, and compute the Within Set Sum of Squared Errors (WSSSE).",
    data: R`training: RDD of array([lat, long])
k = 6`,
    expected: R`Earthquake cluster centers: [array([38.63, -119.22]), ...]
Within Set Sum of Squared Errors = ...`,
    starter: R`from math import sqrt
from pyspark.mllib.clustering import KMeans
`,
    hints: [
      "`model = KMeans.train(training, k)` and print `model.clusterCenters`.",
      "Error of one point: distance to its center. `center = model.centers[model.predict(point)]`.",
      "WSSSE: `training.map(lambda point: error(point)).reduce(lambda x, y: x + y)`."
    ],
    solution: R`from math import sqrt
from pyspark.mllib.clustering import KMeans

# train model based on training data and k-clusters
model = KMeans.train(training, k)
print("Earthquake cluster centers: " + str(model.clusterCenters))

def error(point):
    center = model.centers[model.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = training.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Errors = " + str(WSSSE))`,
    rubric: [
      { pts: 2, text: "KMeans.train(training, k)" },
      { pts: 1, text: "Prints model.clusterCenters" },
      { pts: 2, text: "error(point) uses model.predict to find the point's center and computes the distance" },
      { pts: 2, text: "WSSSE = map(error) then reduce(add)" }
    ],
    checks: [
      { re: "KMeans\\.train\\s*\\(\\s*training\\s*,\\s*k", label: "KMeans.train(training, k)" },
      { re: "clusterCenters|centers", label: "Uses the cluster centers" },
      { re: "predict\\s*\\(", label: "Uses model.predict(point)" },
      { re: "reduce\\s*\\(", label: "Sums with reduce" }
    ],
    explain: "k-means is unsupervised: no labels, just (lat, long). WSSSE measures how tight the clusters are.", src: "ML slides 50-51" },
  { id: "ml-ex-pipeline", track: "ml", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: random forest temperature pipeline (lab)",
    lang: "python",
    prompt: "From the ML-Spark lab: using the weather DataFrame `df` (columns `AverageTemperature, year, month, Country`), build a Spark ML pipeline that indexes `Country`, assembles `year, month, CountryIndex` into features, trains a **RandomForestRegressor** to predict `AverageTemperature`, splits 80/20, fits on training data, predicts on test data, and prints the RMSE.",
    data: R`+------------------+----+-----+-------+
|AverageTemperature|year|month|Country|
+------------------+----+-----+-------+
|             6.068|1743|   11|Denmark|
|5.7879999999999985|1744|    4|Denmark|`,
    expected: R`Root Mean Squared Error (RMSE): 4.43`,
    starter: R`from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
`,
    hints: [
      "Three stages: StringIndexer(inputCol=\"Country\", outputCol=\"CountryIndex\", handleInvalid=\"skip\"), VectorAssembler, RandomForestRegressor.",
      "`pipeline = Pipeline(stages=[country_indexer, assembler, rf])`; `train_df, test_df = df.randomSplit([0.8, 0.2], seed=...)`.",
      "`model = pipeline.fit(train_df)`, `predictions = model.transform(test_df)`, then RegressionEvaluator with metricName=\"rmse\". maxBins must cover the number of countries (the lab used 200)."
    ],
    solution: R`from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

country_indexer = StringIndexer(inputCol="Country", outputCol="CountryIndex",
                                handleInvalid="skip")

assembler = VectorAssembler(inputCols=["year", "month", "CountryIndex"],
                            outputCol="features")

rf = RandomForestRegressor(labelCol="AverageTemperature", featuresCol="features",
                           maxDepth=10, numTrees=50,
                           maxBins=200)   # >= number of countries (categorical feature)

pipeline = Pipeline(stages=[country_indexer, assembler, rf])

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

model = pipeline.fit(train_df)
predictions = model.transform(test_df)

evaluator = RegressionEvaluator(labelCol="AverageTemperature",
                                predictionCol="prediction",
                                metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")`,
    trace: [
      { phase: "input", label: "df", rows: ["(6.068, 1743, 11, Denmark)", "(5.788, 1744, 4, Denmark)"] },
      { phase: "map", label: "StringIndexer + VectorAssembler", rows: ["CountryIndex = 37.0", "features = [1743, 11, 37.0]"] },
      { phase: "shuffle", label: "pipeline.fit(train_df)", rows: ["RandomForestRegressor learns 50 trees"] },
      { phase: "reduce", label: "transform(test_df) + evaluate", rows: ["prediction column added", "RMSE 4.43, R² 0.817"] }
    ],
    rubric: [
      { pts: 2, text: "StringIndexer on Country (handleInvalid=\"skip\")" },
      { pts: 2, text: "VectorAssembler of year, month, CountryIndex into features" },
      { pts: 2, text: "RandomForestRegressor with labelCol=\"AverageTemperature\"" },
      { pts: 1, text: "Pipeline(stages=[indexer, assembler, rf]) in that order" },
      { pts: 1, text: "randomSplit([0.8, 0.2])" },
      { pts: 1, text: "fit on train, transform on test" },
      { pts: 1, text: "RegressionEvaluator with rmse, printed" }
    ],
    checks: [
      { re: "StringIndexer\\s*\\(", label: "StringIndexer" },
      { re: "VectorAssembler\\s*\\(", label: "VectorAssembler" },
      { re: "RandomForestRegressor\\s*\\(", label: "RandomForestRegressor" },
      { re: "Pipeline\\s*\\(\\s*stages", label: "Pipeline(stages=[…])" },
      { re: "randomSplit", label: "randomSplit" },
      { re: "\\.fit\\s*\\(\\s*train", label: "fit on training data" },
      { re: "\\.transform\\s*\\(\\s*test", label: "transform on test data" },
      { re: "rmse", label: "Evaluates RMSE" }
    ],
    explain: "Same shape as slides 16-17 with a tree model. Trees don't need one-hot encoding (StringIndexer is enough), but maxBins must be at least the number of categories.", src: "Lab: ML-Spark, ML slides 22-30" },
  { id: "ml-ex-ohe-lr", track: "ml", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: Airbnb price pipeline with one-hot encoding (slides 16-17)",
    lang: "python",
    prompt: "Write the full Spark ML code to predict Airbnb `price`: split `airbnbDF` 80/20 (seed 42), index and one-hot encode **all string columns**, assemble them with all **double** columns except price, train LinearRegression in a Pipeline, predict on the test set and compute RMSE. " + AIRBNB_NOTE,
    data: R`neighbourhood_cleansed | room_type       | bedrooms | bathrooms | number_of_reviews | price
Western Addition       | Entire home/apt | 1.0      | 1.0       | 180.0             | 170.0
Bernal Heights         | Entire home/apt | 2.0      | 1.0       | 111.0             | 235.0`,
    expected: R`RMSE is 220.6`,
    starter: R`from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
`,
    hints: [
      "Split first: `trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)`.",
      "Slide 16 builds categoricalCols, indexOutputCols, oheOutputCols, numericCols with list comprehensions over `trainDF.dtypes`.",
      "Slide 17: `Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])`, fit on trainDF, transform testDF, then RegressionEvaluator(rmse)."
    ],
    solution: R`from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)

categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
indexOutputCols = [x + "Index" for x in categoricalCols]
oheOutputCols = [x + "OHE" for x in categoricalCols]

stringIndexer = StringIndexer(inputCols=categoricalCols,
                              outputCols=indexOutputCols,
                              handleInvalid="skip")
oheEncoder = OneHotEncoder(inputCols=indexOutputCols,
                           outputCols=oheOutputCols)

numericCols = [field for (field, dataType) in trainDF.dtypes
               if ((dataType == "double") & (field != "price"))]
assemblerInputs = oheOutputCols + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

lr = LinearRegression(labelCol="price", featuresCol="features")
pipeline = Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])

pipelineModel = pipeline.fit(trainDF)
predDF = pipelineModel.transform(testDF)
predDF.select("features", "price", "prediction").show(5)

regressionEvaluator = RegressionEvaluator(predictionCol="prediction",
                                          labelCol="price",
                                          metricName="rmse")
rmse = regressionEvaluator.evaluate(predDF)
print(f"RMSE is {rmse:.1f}")`,
    rubric: [
      { pts: 1, text: "randomSplit([.8, .2], seed=42)" },
      { pts: 2, text: "Finds string columns and builds Index / OHE output column names" },
      { pts: 2, text: "StringIndexer (handleInvalid=\"skip\") then OneHotEncoder" },
      { pts: 2, text: "VectorAssembler of OHE columns + double columns except price" },
      { pts: 2, text: "Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr]); fit trainDF, transform testDF" },
      { pts: 1, text: "RegressionEvaluator with rmse" }
    ],
    checks: [
      { re: "randomSplit", label: "Splits train/test" },
      { re: "StringIndexer\\s*\\(", label: "StringIndexer" },
      { re: "OneHotEncoder\\s*\\(", label: "OneHotEncoder" },
      { re: "!=\\s*[\"']price", label: "Excludes price from the features" },
      { re: "VectorAssembler\\s*\\(", label: "VectorAssembler" },
      { re: "stages\\s*=\\s*\\[\\s*stringIndexer\\s*,\\s*oheEncoder\\s*,\\s*vecAssembler\\s*,\\s*lr", label: "Stages in the right order" },
      { re: "rmse", label: "Computes RMSE" }
    ],
    explain: "The prof flagged slide 16 (this code) and slide 8 (the roles). Know why each stage exists: indexer (strings to numbers), encoder (no fake order), assembler (one vector column), model (estimator).", src: "ML slides 10-20" },
  { id: "ml-ex-als", track: "ml", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: the ALS dating recommender (slide 44, on the quiz)",
    lang: "python",
    prompt: "The prof said slide 44's code is on the quiz. Ratings lines are `UserID,ProfileID,Rating`. Write: (a) `parse_rating(line, sep=',')` returning `(random integer 1-10, (user_id, profile_id, rating))`; (b) the ratings RDD from `ratings.dat`; (c) a 60% training / 40% validation split using the random key (4 partitions, cached); (d) training the ALS model with `rank`, `num_iterations` and `lambda` (`lambda_` in Python 3); (e) `compute_rmse(model, data, n)`.",
    data: R`ratings.dat
1,133,8
1,720,6
2,8,10`,
    expected: R`Training: 10764531 and validation: 7155773
The model was trained with rank=8, lambda=0.1, and 10 iterations.
Its RMSE on the validation set is 1.9...`,
    starter: R`import random
from math import sqrt
from operator import add
from pyspark.mllib.recommendation import ALS

def parse_rating(line, sep=','):
    pass
`,
    hints: [
      "parse_rating: `fields = line.strip().split(sep)`, convert fields[0], fields[1] to int and fields[2] to float, `return random.randint(1, 10), (user_id, profile_id, rating)`.",
      "Split: `training = ratings.filter(lambda x: x[0] < 6).values().repartition(num_partitions).cache()`; validation uses `x[0] >= 6`.",
      "RMSE (slide 47): predictAll on (user, item) pairs → map to ((user, item), pred) → join with actual ((user, item), rating) → values() → sqrt(map((p - a) ** 2).reduce(add) / float(n))."
    ],
    solution: R`import random
from math import sqrt
from operator import add
from pyspark.mllib.recommendation import ALS

def parse_rating(line, sep=','):
    """
    Parses a rating line
    Returns: tuple of (random integer, (user_id, profile_id, rating))
    """
    fields = line.strip().split(sep)
    user_id = int(fields[0])         # convert user_id to int
    profile_id = int(fields[1])      # convert profile_id to int
    rating = float(fields[2])        # convert rating to float
    return random.randint(1, 10), (user_id, profile_id, rating)

# Create ratings RDD of (randint, (user_id, profile_id, rating))
ratings = sc.textFile("/home/hadoop/hadoop-fundamentals/data/dating/ratings.dat") \
            .map(parse_rating)

# Create the training (60%) and validation (40%) set, based on the random key
num_partitions = 4
training = ratings.filter(lambda x: x[0] < 6) \
                  .values() \
                  .repartition(num_partitions) \
                  .cache()
validation = ratings.filter(lambda x: x[0] >= 6) \
                    .values() \
                    .repartition(num_partitions) \
                    .cache()

num_training = training.count()
num_validation = validation.count()
print("Training: %d and validation: %d\n" % (num_training, num_validation))

# Train model with training data and configured rank and iterations
rank = 8
num_iterations = 10
lambda_ = 0.1
model = ALS.train(training, rank, num_iterations, lambda_)

def compute_rmse(model, data, n):
    """
    Compute Root Mean Squared Error (RMSE), or square root of the average value
    of (actual rating - predicted rating)^2
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictions_ratings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
        .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
        .values()
    return sqrt(predictions_ratings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

validation_rmse = compute_rmse(model, validation, num_validation)
print("The model was trained with rank=%d, lambda=%.1f, and %d iterations." % (rank, lambda_, num_iterations))
print("Its RMSE on the validation set is %f." % validation_rmse)`,
    trace: [
      { phase: "input", label: "ratings.dat lines", rows: ["\"1,133,8\"", "\"1,720,6\"", "\"2,8,10\""] },
      { phase: "map", label: "map(parse_rating)", rows: ["(3, (1, 133, 8.0))", "(7, (1, 720, 6.0))", "(5, (2, 8, 10.0))"] },
      { phase: "shuffle", label: "filter on random key + values()", rows: ["training (< 6): (1, 133, 8.0), (2, 8, 10.0)", "validation (≥ 6): (1, 720, 6.0)"] },
      { phase: "reduce", label: "ALS.train + compute_rmse", rows: ["predictAll → ((1, 720), 6.4)", "join actual → (6.4, 6.0)", "sqrt(mean((p − a)²))"] }
    ],
    rubric: [
      { pts: 3, text: "parse_rating splits the line, converts the three fields, returns (random.randint(1, 10), (user, profile, rating))" },
      { pts: 1, text: "ratings = sc.textFile(...).map(parse_rating)" },
      { pts: 3, text: "training = filter(x[0] < 6).values() …cache(); validation = filter(x[0] >= 6)…" },
      { pts: 1, text: "ALS.train(training, rank, num_iterations, lambda)" },
      { pts: 2, text: "compute_rmse: predictAll on (user, item), join with actual on (user, item), sqrt of mean squared error" }
    ],
    checks: [
      { re: "split\\s*\\(\\s*sep", label: "Splits on the separator" },
      { re: "random\\.randint\\s*\\(\\s*1\\s*,\\s*10", label: "random.randint(1, 10) key" },
      { re: "map\\s*\\(\\s*parse_rating", label: "map(parse_rating)" },
      { re: "<\\s*6", label: "Training keeps keys < 6" },
      { re: ">=\\s*6", label: "Validation keeps keys >= 6" },
      { re: "\\.values\\s*\\(", label: "Drops the random key with values()" },
      { re: "ALS\\.train\\s*\\(", label: "ALS.train(…)" },
      { re: "predictAll", label: "predictAll for RMSE" },
      { re: "sqrt", label: "Square root for RMSE" }
    ],
    explain: "Slide 44 is mostly plain PySpark RDD work: textFile, map, filter, values, cache, count. The ML part is one line (ALS.train). Know why the random key exists (a reproducible 60/40 split) and why values() drops it.", src: "ML slides 43-48" },

  /* ===================== Spark Streaming ===================== */
  { id: "ss-dstream-read", track: "sparkstream", level: 1, type: "mcq", title: "What is a DStream?",
    lang: "python", code: R`ssc = StreamingContext(sc, 1)          # batch interval: 1 second
lines = ssc.socketTextStream("localhost", 7777)
errors = lines.filter(lambda line: "error" in line)`,
    q: "After 5 seconds of running, what is `errors`?",
    options: ["A DStream: a sequence of 5 RDDs, one per 1-second batch, each holding that second's error lines", "One RDD with every error line so far", "A Python list of lines", "A DataFrame with an error column"],
    answer: 0, explain: "A DStream is a sequence of RDDs. filter() on a DStream is applied to every RDD (batch) in it.", src: "Spark slides 26-30" },
  { id: "ss-window-read", track: "sparkstream", level: 1, type: "mcq", title: "Reading window parameters",
    lang: "python", code: R`ssc = StreamingContext(sc, 10)                        # 10-second batches
counts = pairs.reduceByKeyAndWindow(lambda a, b: a + b, None, 30, 20)`,
    q: "What does this compute?",
    options: ["Every 20 seconds, the counts over the last 30 seconds (the last 3 batches)", "Every 30 seconds, the counts over the last 20 seconds", "Counts per 10-second batch only", "A running total since the start"],
    answer: 0, explain: "Window duration 30 s = 3 batches; sliding duration 20 s = every second batch. Both are multiples of the 10 s batch interval.", src: "Spark slides 35-36" },
  { id: "ss-stateless-read", track: "sparkstream", level: 1, type: "mcq", title: "Stateless counts (slide 33)",
    lang: "python", code: R`ipDStream = accessLogsDStream.map(lambda entry: (entry.ip, 1))
ipCountsDStream = ipDStream.reduceByKey(lambda x, y: x + y)
ipCountsDStream.pprint()`,
    q: "IP 1.2.3.4 appears 3 times in batch 1 and 2 times in batch 2. What is printed for it in batch 2?",
    options: ["2", "5", "3", "Nothing until checkpointing is enabled"],
    answer: 0, explain: "reduceByKey is stateless: it reduces within each time step only. A running total (5) needs updateStateByKey.", src: "Spark slide 33" },
  { id: "ss-filter-fill", track: "sparkstream", level: 2, type: "fill", title: "Fill in the error-line filter (slide 26)",
    lang: "python", prompt: "PySpark version of the slide 26 Scala example (the Scala original is in the explanation).",
    code: R`from pyspark.streaming import StreamingContext

# Create a StreamingContext with a 1-second batch size
ssc = ⟦StreamingContext⟧(sc, ⟦1⟧)

# Create a DStream using data received after connecting to port 7777
lines = ssc.⟦socketTextStream⟧("localhost", 7777)

# Filter our DStream for lines with "error"
errorLines = lines.⟦filter⟧(lambda line: "error" in line)

# Print out the lines with errors
errorLines.⟦pprint|print⟧()

# Start our streaming context and wait for it to "finish"
ssc.⟦start⟧()
ssc.⟦awaitTermination⟧()`,
    explain: "Slide 26 in Scala: `val ssc = new StreamingContext(conf, Seconds(1))`, `val lines = ssc.socketTextStream(\"localhost\", 7777)`, `val errorLines = lines.filter(_.contains(\"error\"))`, `errorLines.print()`, `ssc.start()`, `ssc.awaitTermination()`. Nothing runs until start().", src: "Spark slide 26" },
  { id: "ss-update-parsons", track: "sparkstream", level: 3, type: "parsons", title: "Arrange a running word count with updateStateByKey",
    lang: "python", prompt: "Build a streaming job that keeps a running count of every word since the job started. Two lines are distractors.",
    lines: [
      [R`ssc = StreamingContext(sc, 1)`, 0],
      [R`ssc.checkpoint("hdfs://namenode/checkpoints")`, 1],
      [R`lines = ssc.socketTextStream("localhost", 7777)`, 1],
      [R`pairs = lines.flatMap(lambda line: line.split()).map(lambda w: (w, 1))`, 2],
      [R`running = pairs.updateStateByKey(update)`, 3],
      [R`running.pprint()`, 4],
      [R`ssc.start()`, 5],
      [R`ssc.awaitTermination()`, 6]
    ],
    distractors: [R`running = pairs.reduceByKey(add)`, R`ssc.awaitTermination()  # before start()`],
    explain: "Create the context, enable checkpointing (required for stateful ops), build the DStream, map to pairs, updateStateByKey, output, then start() and awaitTermination(). reduceByKey alone would only count within each batch.", src: "Spark slides 26, 34, 37" },
  { id: "ss-window-write", track: "sparkstream", level: 4, type: "write", title: "Write: top error counts over a sliding window",
    lang: "python",
    prompt: "Batches are 10 seconds. `pairs` is a DStream of `(errorCode, 1)`. Count each error code over the **last 30 seconds**, recomputed **every 10 seconds**, and print the result. Remember what windowed operations require.",
    data: R`pairs: DStream of ("E500", 1), ("E404", 1), ...`,
    expected: R`-------------------------------------------
Time: 2026-11-27 10:30:30
-------------------------------------------
('E500', 7)
('E404', 3)`,
    starter: R`ssc = StreamingContext(sc, 10)
`,
    hints: [
      "Windowed operations are stateful, so they need `ssc.checkpoint(\"hdfs://...\")`.",
      "`reduceByKeyAndWindow(func, invFunc, windowDuration, slideDuration)`: window 30, slide 10. You can pass None for invFunc.",
      "Print with `.pprint()`, then `ssc.start()` and `ssc.awaitTermination()`."
    ],
    solution: R`ssc = StreamingContext(sc, 10)                 # 10-second batches
ssc.checkpoint("hdfs://namenode/checkpoints")   # windowed ops are stateful

windowCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y,   # combine
                                          None,                 # no inverse function
                                          30,                   # window duration (3 batches)
                                          10)                   # sliding duration
windowCounts.pprint()

ssc.start()
ssc.awaitTermination()

# Scala (Learning Spark style):
# val windowCounts = pairs.reduceByKeyAndWindow({(x: Int, y: Int) => x + y}, Seconds(30), Seconds(10))`,
    rubric: [
      { pts: 2, text: "Enables checkpointing (ssc.checkpoint)" },
      { pts: 2, text: "Uses a windowed operation (reduceByKeyAndWindow or window + reduceByKey)" },
      { pts: 2, text: "Window 30 s and slide 10 s (multiples of the batch interval)" },
      { pts: 1, text: "Prints, then ssc.start() and awaitTermination()" }
    ],
    checks: [
      { re: "checkpoint\\s*\\(", label: "Enables checkpointing" },
      { re: "reduceByKeyAndWindow|\\.window\\s*\\(", label: "Uses a window operation" },
      { re: "30", label: "30-second window" },
      { re: "start\\s*\\(", label: "Calls start()" }
    ],
    explain: "Window duration = how much history (30 s = last 3 batches). Sliding duration = how often a result comes out (10 s = every batch). Both must be multiples of the batch interval.", src: "Spark slides 34-36" },
  { id: "ss-getorcreate-write", track: "sparkstream", level: 4, type: "write", title: "Write: a driver that recovers from failure (slide 40)",
    lang: "python",
    prompt: "Make the error-filter job (slide 26) recover from a driver crash. Write a `createStreamingContext()` function that builds the context, sets the checkpoint directory and defines the DStream, then create the context the fault-tolerant way.",
    data: R`checkpointDir = "hdfs://namenode/checkpoints"`,
    expected: R`(after a crash, the restarted driver resumes from the checkpoint)`,
    starter: R`checkpointDir = "hdfs://namenode/checkpoints"

def createStreamingContext():
    pass
`,
    hints: [
      "Inside the function: `ssc = StreamingContext(sc, 1)`, `ssc.checkpoint(checkpointDir)`, define lines and the filter, return ssc.",
      "Instead of calling StreamingContext directly, use `StreamingContext.getOrCreate(checkpointDir, createStreamingContext)`.",
      "Then `ssc.start()` and `ssc.awaitTermination()`."
    ],
    solution: R`checkpointDir = "hdfs://namenode/checkpoints"

def createStreamingContext():
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint(checkpointDir)
    lines = ssc.socketTextStream("localhost", 7777)
    errorLines = lines.filter(lambda line: "error" in line)
    errorLines.pprint()
    return ssc

# Recover from the checkpoint if it exists, otherwise build a new context
ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
ssc.start()
ssc.awaitTermination()`,
    rubric: [
      { pts: 2, text: "A function that creates the StreamingContext, sets checkpoint, defines the DStream and returns ssc" },
      { pts: 3, text: "Uses StreamingContext.getOrCreate(checkpointDir, createFunc) instead of a plain constructor" },
      { pts: 2, text: "start() and awaitTermination()" }
    ],
    checks: [
      { re: "def\\s+\\w+\\s*\\(", label: "Defines a create function" },
      { re: "checkpoint\\s*\\(", label: "Sets the checkpoint directory" },
      { re: "getOrCreate", label: "Uses StreamingContext.getOrCreate" },
      { re: "return\\s+ssc", label: "Returns the context" }
    ],
    explain: "This is the driver-fault-tolerance half of checkpointing. The other half is limiting recomputation of state.", src: "Spark slides 39-40" },
  { id: "ss-ex-running", track: "sparkstream", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: per-batch and running hit counts per page",
    lang: "python",
    prompt: "Web server log lines arrive on `localhost:9999`, one per request, formatted `ip page status`. With 5-second batches, write a Spark Streaming job that prints (a) the number of hits per **page in each batch** (stateless) and (b) the **running total** of hits per page since the job started (stateful). Explain in a comment which part needs checkpointing and why.",
    data: R`10.0.0.1 /home 200
10.0.0.2 /cart 200
10.0.0.1 /home 404`,
    expected: R`batch counts:   ('/home', 2) ('/cart', 1)
running totals: ('/home', 17) ('/cart', 6)`,
    starter: R`from operator import add
from pyspark.streaming import StreamingContext
`,
    hints: [
      "Pairs: `lines.map(lambda line: (line.split()[1], 1))`.",
      "(a) is `reduceByKey(add)`: stateless, within each batch.",
      "(b) is `updateStateByKey(update)` where `update(new_values, running_count)` returns `sum(new_values) + (running_count or 0)`. It needs `ssc.checkpoint(...)`."
    ],
    solution: R`from operator import add
from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 5)                    # 5-second batches
ssc.checkpoint("hdfs://namenode/checkpoints")    # needed by updateStateByKey (stateful)

lines = ssc.socketTextStream("localhost", 9999)
pages = lines.map(lambda line: (line.split()[1], 1))      # (page, 1)

# (a) stateless: hits per page within each batch only
batchCounts = pages.reduceByKey(add)
batchCounts.pprint()

# (b) stateful: running total per page across batches
def update(new_values, running_count):
    if running_count is None:          # no previous state for this page
        running_count = 0
    return sum(new_values) + running_count

runningCounts = pages.updateStateByKey(update)
runningCounts.pprint()

ssc.start()
ssc.awaitTermination()

# Only (b) needs checkpointing: it depends on state from earlier batches, which must be
# saved to reliable storage so it can be recovered after a failure without replaying
# the whole lineage from the start of the stream.`,
    trace: [
      { phase: "input", label: "Batch 1 lines", rows: ["10.0.0.1 /home 200", "10.0.0.2 /cart 200", "10.0.0.1 /home 404"] },
      { phase: "map", label: "map → (page, 1)", rows: ["(/home, 1)", "(/cart, 1)", "(/home, 1)"] },
      { phase: "reduce", label: "reduceByKey (this batch)", rows: ["(/home, 2)", "(/cart, 1)"] },
      { phase: "output", label: "updateStateByKey (running)", rows: ["state /home: 15 + 2 = 17", "state /cart: 5 + 1 = 6"] }
    ],
    rubric: [
      { pts: 2, text: "StreamingContext with a 5-second batch, socketTextStream on port 9999" },
      { pts: 2, text: "Maps each line to (page, 1)" },
      { pts: 2, text: "Per-batch counts with reduceByKey" },
      { pts: 2, text: "updateStateByKey with an update function adding new values to the old state (handling no previous state)" },
      { pts: 1, text: "ssc.checkpoint, explained as required for the stateful part" },
      { pts: 1, text: "Outputs, start() and awaitTermination()" }
    ],
    checks: [
      { re: "StreamingContext\\s*\\(\\s*sc\\s*,\\s*5", label: "5-second batches" },
      { re: "socketTextStream", label: "Reads from the socket" },
      { re: "reduceByKey", label: "Per-batch reduceByKey" },
      { re: "updateStateByKey", label: "Running totals with updateStateByKey" },
      { re: "checkpoint\\s*\\(", label: "Enables checkpointing" },
      { re: "start\\s*\\(", label: "Starts the context" }
    ],
    explain: "This one question covers slide 33 (stateless), slide 37 (updateStateByKey), slide 34/39 (checkpointing) and slide 38 (the stateless vs stateful contrast the prof flagged).", src: "Spark slides 26, 33-39" },

  /* ===================== New pre-midterm exam-style items ===================== */
  { id: "java-total-distance", track: "java", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: total distance per airline, with combiner (Java)",
    lang: "java",
    prompt: "Using the Java MapReduce API, compute the **total distance flown by each airline** from the flight CSV (`date, airline, flightnum, origin, dest, dep, dep_delay, arv, arv_delay, airtime, distance`). Write the Mapper and Reducer, and the driver lines that set the combiner. Explain why a combiner is safe here.",
    data: R`2014-04-01,19805,1,JFK,LAX,0854,-6.00,1217,2.00,355.00,2475.00
2014-04-01,19805,2,LAX,JFK,0944,14.00,1736,-29.00,269.00,2475.00
2014-04-01,19930,3,SEA,SFO,0700,0.00,0905,5.00,120.00,679.00`,
    expected: R`19805	4950.0
19930	679.0`,
    starter: R`public class DistanceMapper
    extends Mapper<…> {

}

public class DistanceReducer
    extends Reducer<…> {

}`,
    hints: [
      "Key = airline (fields[1]); value = distance (fields[10]) as a DoubleWritable.",
      "Reducer: sum `value.get()` over all values.",
      "Combiner: `job.setCombinerClass(DistanceReducer.class);` is safe because sums of partial sums give the same total."
    ],
    solution: R`public class DistanceMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    String airline = fields[1];
    double distance = Double.parseDouble(fields[10]);
    context.write(new Text(airline), new DoubleWritable(distance));
  }
}

public class DistanceReducer
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
    double total = 0;
    for (DoubleWritable value : values) {
      total += value.get();
    }
    context.write(key, new DoubleWritable(total));
  }
}

// Driver:
// job.setMapperClass(DistanceMapper.class);
// job.setCombinerClass(DistanceReducer.class);   // safe: sum of partial sums = total
// job.setReducerClass(DistanceReducer.class);
// job.setOutputKeyClass(Text.class);
// job.setOutputValueClass(DoubleWritable.class);`,
    trace: [
      { phase: "input", label: "Input lines", rows: ["…,19805,…,2475.00", "…,19805,…,2475.00", "…,19930,…,679.00"] },
      { phase: "map", label: "Map emits (airline, distance)", rows: ["(19805, 2475.0)", "(19805, 2475.0)", "(19930, 679.0)"] },
      { phase: "shuffle", label: "Shuffle & sort", rows: ["(19805, [2475.0, 2475.0])", "(19930, [679.0])"] },
      { phase: "reduce", label: "Reduce: sum", rows: ["(19805, 4950.0)", "(19930, 679.0)"] }
    ],
    rubric: [
      { pts: 1, text: "Mapper types <LongWritable, Text, Text, DoubleWritable>" },
      { pts: 2, text: "Splits the line on commas; key = fields[1], value = fields[10] parsed as a number" },
      { pts: 1, text: "Emits with context.write" },
      { pts: 1, text: "Reducer types match the mapper output" },
      { pts: 2, text: "Sums value.get() over all values and writes (airline, total)" },
      { pts: 3, text: "setCombinerClass(DistanceReducer.class) and explains sum is safe to combine" }
    ],
    checks: [
      { re: "extends\\s+Mapper\\s*<", label: "Has a Mapper class" },
      { re: "split\\s*\\(\\s*\"\\s*,\\s*\"\\s*\\)", label: "Splits on commas" },
      { re: "\\[\\s*10\\s*\\]", label: "Uses fields[10] (distance)" },
      { re: "\\+=", label: "Sums the values" },
      { re: "setCombinerClass", label: "Sets the combiner" }
    ],
    explain: "Sum is the textbook combiner-safe function, like max on slide 13. Compare with average, which isn't.", src: "MR slides 9-13, 26" },
  { id: "st-count-dest", track: "streaming", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: flights per destination (Python streaming, slide 26-27 classes)",
    lang: "python",
    prompt: "Using the slide 26-27 Mapper and Reducer class structure (`__init__`, `emit`, `map`/`reduce`, `__iter__`), write Hadoop Streaming scripts that count the **number of flights arriving at each destination airport** in the flight CSV.",
    data: R`2014-04-01,19805,1,JFK,LAX,0854,-6.00,1217,2.00,355.00,2475.00
2014-04-01,19805,2,LAX,JFK,0944,14.00,1736,-29.00,269.00,2475.00
2014-04-01,19930,3,SEA,LAX,0700,0.00,0905,5.00,120.00,954.00`,
    expected: R`JFK	1
LAX	2`,
    starter: R`# mapper.py
#!/usr/bin/env python
import sys
import csv

SEP = "\t"


# reducer.py
#!/usr/bin/env python
import sys
from itertools import groupby
from operator import itemgetter

SEP = "\t"
`,
    hints: [
      "Mapper: same as slide 26, but `self.emit(row[4], 1)` (dest, 1).",
      "Reducer: slide 27's groupby loop, but count instead of averaging.",
      "`for current, group in groupby(self, itemgetter(0)):` then `self.emit(current, sum(item[1] for item in group))`."
    ],
    solution: R`# ---------------- mapper.py ----------------
#!/usr/bin/env python
import sys
import csv

SEP = "\t"

class Mapper(object):

    def __init__(self, stream, sep=SEP):
        self.stream = stream
        self.sep    = sep

    def emit(self, key, value):
        sys.stdout.write("{}{}{}\n".format(key, self.sep, value))

    def map(self):
        for row in self:
            self.emit(row[4], 1)          # destination airport, 1

    def __iter__(self):
        reader = csv.reader(self.stream)
        for row in reader:
            yield row

if __name__ == '__main__':
    mapper = Mapper(sys.stdin)
    mapper.map()

# ---------------- reducer.py ----------------
#!/usr/bin/env python
import sys
from itertools import groupby
from operator import itemgetter

SEP = "\t"

class Reducer(object):

    def __init__(self, stream, sep=SEP):
        self.stream = stream
        self.sep    = sep

    def emit(self, key, value):
        sys.stdout.write("{}{}{}\n".format(key, self.sep, value))

    def reduce(self):
        for current, group in groupby(self, itemgetter(0)):
            count = 0
            for item in group:
                count += item[1]
            self.emit(current, count)

    def __iter__(self):
        for line in self.stream:
            try:
                parts = line.split(self.sep)
                yield parts[0], int(parts[1])
            except:
                continue

if __name__ == '__main__':
    reducer = Reducer(sys.stdin)
    reducer.reduce()`,
    trace: [
      { phase: "input", label: "Input lines", rows: ["…,JFK,LAX,…", "…,LAX,JFK,…", "…,SEA,LAX,…"] },
      { phase: "map", label: "Map emits (dest, 1)", rows: ["LAX\t1", "JFK\t1", "LAX\t1"] },
      { phase: "shuffle", label: "sort", rows: ["JFK\t1", "LAX\t1", "LAX\t1"] },
      { phase: "reduce", label: "Reduce: groupby + count", rows: ["JFK\t1", "LAX\t2"] }
    ],
    rubric: [
      { pts: 2, text: "Mapper class with __init__, emit and __iter__ (csv.reader)" },
      { pts: 3, text: "map() emits (row[4], 1)" },
      { pts: 2, text: "Reducer groups with groupby(self, itemgetter(0))" },
      { pts: 2, text: "Counts the values per group and emits (dest, count)" },
      { pts: 1, text: "Both run from an ifmain block" }
    ],
    checks: [
      { re: "class\\s+Mapper", label: "Mapper class" },
      { re: "row\\s*\\[\\s*4\\s*\\]", label: "Uses row[4] (dest)" },
      { re: "def\\s+emit", label: "Defines emit" },
      { re: "groupby\\s*\\(\\s*self\\s*,\\s*itemgetter\\s*\\(\\s*0", label: "groupby(self, itemgetter(0))" },
      { re: "__main__", label: "ifmain block" }
    ],
    explain: "Word count in the slide 26-27 class style. The prof said he's looking for emit and reduce from slide 27.", src: "MR slides 26-28" },
  { id: "ps-max-delay", track: "pyspark", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: worst departure delay per airline (PySpark)",
    lang: "python",
    prompt: "Using PySpark RDDs, find the **maximum departure delay for each airline** in `ontime/flights.csv` and print the airlines sorted from worst to best delay (slide 16 style: sort on the driver).",
    data: R`2014-04-01,19805,1,JFK,LAX,0854,-6.00,1217,2.00,355.00,2475.00
2014-04-01,19805,2,LAX,JFK,0944,14.00,1736,-29.00,269.00,2475.00
2014-04-01,19930,3,SEA,SFO,0700,0.00,0905,5.00,120.00,679.00`,
    expected: R`19805 14.0
19930 0.0`,
    starter: R`from operator import itemgetter

lines = sc.textFile("ontime/flights.csv")
`,
    hints: [
      "Split each line on commas, then map to (airline, float(dep_delay)): fields 1 and 6.",
      "`reduceByKey(lambda a, b: max(a, b))`.",
      "`collect()`, then `sorted(..., key=itemgetter(1), reverse=True)` and print."
    ],
    solution: R`from operator import itemgetter

lines = sc.textFile("ontime/flights.csv")
rows = lines.map(lambda line: line.split(","))
delays = rows.map(lambda r: (r[1], float(r[6])))          # (airline, dep_delay)
worst = delays.reduceByKey(lambda a, b: max(a, b)).collect()
worst = sorted(worst, key=itemgetter(1), reverse=True)

for airline, delay in worst:
    print(airline, delay)`,
    rubric: [
      { pts: 2, text: "textFile and split on commas" },
      { pts: 3, text: "Maps to (airline, float(dep_delay)) using fields 1 and 6" },
      { pts: 3, text: "reduceByKey keeping the max" },
      { pts: 2, text: "collect + sorted descending by delay (itemgetter(1), reverse=True), printed" }
    ],
    checks: [
      { re: "split\\s*\\(\\s*[\"'],[\"']\\s*\\)", label: "Splits on commas" },
      { re: "\\[\\s*1\\s*\\][\\s\\S]*\\[\\s*6\\s*\\]", label: "Uses fields 1 and 6" },
      { re: "reduceByKey", label: "reduceByKey" },
      { re: "max\\s*\\(", label: "Keeps the max" },
      { re: "reverse\\s*=\\s*True", label: "Sorts worst first" }
    ],
    explain: "The same shape as slide 16's total-delay app with max instead of add.", src: "Spark slides 12-16" },
  { id: "sql-airline-df", track: "sql", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: same query, both ways (SQL and DataFrame API)",
    lang: "python",
    prompt: "`flights` is a DataFrame (`date, airline, flightnum, origin, dest, dep, dep_delay, arv, arv_delay, airtime, distance`). For each airline, return the number of flights and the average **arrival** delay rounded to 1 decimal, only for airlines with at least 50 flights, sorted by average arrival delay descending. Write it **twice**: once in Spark SQL (slide 21 style) and once with the DataFrame API (slide 23 style).",
    data: R`2014-04-01,19805,1,JFK,LAX,0854,-6.00,1217,2.00,355.00,2475.00`,
    expected: R`+-------+-----------+-------------+
|airline|num_flights|avg_arr_delay|
+-------+-----------+-------------+
|  19977|       1203|         12.4|
...`,
    starter: R`from pyspark.sql import functions as F

flights.registerTempTable("flights")
`,
    hints: [
      "SQL: SELECT airline, count(1) AS num_flights, round(avg(arv_delay), 1) AS avg_arr_delay FROM flights GROUP BY airline HAVING count(1) >= 50 ORDER BY avg_arr_delay DESC.",
      "DataFrame: groupBy(\"airline\").agg(F.count(\"*\").alias(\"num_flights\"), F.round(F.avg(\"arv_delay\"), 1).alias(\"avg_arr_delay\")).",
      "Then .where(\"num_flights >= 50\").sort(\"avg_arr_delay\", ascending=False)."
    ],
    solution: R`from pyspark.sql import functions as F

# Spark SQL (slide 21 style)
flights.registerTempTable("flights")
by_airline = sqlContext.sql("SELECT airline, "
                            "count(1) AS num_flights, "
                            "round(avg(arv_delay), 1) AS avg_arr_delay "
                            "FROM flights "
                            "GROUP BY airline "
                            "HAVING count(1) >= 50 "
                            "ORDER BY avg_arr_delay DESC")
by_airline.show()

# DataFrame API (slide 23 style)
by_airline_df = flights.groupBy("airline") \
                       .agg(F.count("*").alias("num_flights"),
                            F.round(F.avg("arv_delay"), 1).alias("avg_arr_delay")) \
                       .where("num_flights >= 50") \
                       .sort("avg_arr_delay", ascending=False)
by_airline_df.show()`,
    rubric: [
      { pts: 1, text: "Registers the table before SQL" },
      { pts: 2, text: "SQL: count(1), round(avg(arv_delay), 1) with aliases" },
      { pts: 2, text: "SQL: GROUP BY airline, HAVING count >= 50, ORDER BY … DESC" },
      { pts: 2, text: "DataFrame: groupBy + agg with F.count and F.round(F.avg) aliases" },
      { pts: 2, text: "DataFrame: filters groups after aggregating (where on num_flights) and sorts descending" },
      { pts: 1, text: "show() both" }
    ],
    checks: [
      { re: "registerTempTable|createOrReplaceTempView", label: "Registers the table" },
      { re: "group\\s+by\\s+airline", label: "SQL GROUP BY airline", flags: "i" },
      { re: "having", label: "SQL HAVING", flags: "i" },
      { re: "groupBy\\s*\\(\\s*[\"']airline", label: "DataFrame groupBy(\"airline\")" },
      { re: "F\\.avg\\s*\\(\\s*[\"']arv_delay", label: "F.avg(\"arv_delay\")" },
      { re: "ascending\\s*=\\s*False", label: "Sorts descending" }
    ],
    explain: "HAVING in SQL becomes a .where() AFTER .agg() in the DataFrame API, because it filters groups, not rows.", src: "Spark slides 20-23" }
);
})();

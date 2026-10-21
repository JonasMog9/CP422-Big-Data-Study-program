/* CP422 Midterm Lab: theory content (notes, MCQ, true/false, short answer).
 * Sources are cited per item: Intro = "Programming for Big Data - Introduction",
 * Hadoop = "Intro to Distributed Computing & Big Data OS", MR = "MapReduce Design
 * Patterns & Hadoop Streaming", Spark = "Spark I&II" (slides 1-23 only; Spark
 * Streaming starts at slide 24 and is NOT on the midterm), Review = posted review
 * questions, Lab = Databricks notebooks, Notes = your class notes.
 * Inline markup: `code`, **bold**. */
window.CP = window.CP || {};

CP.exam = {
  name: "CP422 Midterm",
  start: "2026-10-23T10:30:00-04:00",
  end: "2026-10-23T11:20:00-04:00",
  where: "In person, written on MyLS",
  minutes: 50,
  parts: [
    { id: "mcq", label: "Multiple choice", detail: "10 questions", weight: 20 },
    { id: "tf", label: "True / False", detail: "10 questions", weight: 20 },
    { id: "code", label: "Write code", detail: "2 questions: Java MapReduce, Hadoop Streaming, PySpark or Spark SQL", weight: 40 },
    { id: "sa", label: "Short answer", detail: "1 question, one or two parts", weight: 20 }
  ],
  scope: "Every lecture slide up to the start of Spark Streaming (Spark I&II deck, slide 24). Spark Streaming and later is NOT on the midterm."
};

CP.topics = [
  { id: "bigdata", name: "Big Data basics", deck: "Intro deck + Hadoop deck slides 1-5" },
  { id: "hadoop", name: "Hadoop, HDFS & YARN", deck: "Hadoop deck slides 6-18" },
  { id: "mapreduce", name: "MapReduce", deck: "MR deck slides 3-21" },
  { id: "streaming", name: "Hadoop Streaming", deck: "MR deck slides 22-28" },
  { id: "spark", name: "Spark core & RDDs", deck: "Spark deck slides 3-17" },
  { id: "sparksql", name: "Spark SQL & DataFrames", deck: "Spark deck slides 18-23 + labs" }
];

/* Things the professor flagged in class (from your notes). `go` links into the app. */
CP.flags = [
  { when: "Sep 28", text: "MapReduce slide 8 is important: understand how lines become (offset, line), then (year, temp), then (year, [temps]), then (year, max).", go: { view: "code", id: "mr-trace-slide8" } },
  { when: "Sep 28", text: "Possible question: \"I want to find the cheapest airflight. Write the map statement.\"", go: { view: "code", id: "java-cheapest" } },
  { when: "Sep 28", text: "Java code: don't write imports, start from `public class ...`. Review Java inheritance (`extends`).", go: { view: "code", id: "java-types" } },
  { when: "Sep 30", text: "Slide 12 (data flow): no drawing, but expect a question like \"how many reducers if the system has 3 splits?\"", go: { view: "quiz", topic: "mapreduce" } },
  { when: "Oct 5", text: "He is looking for `emit`, `reduce` and the class structure on MR slide 27 (Python streaming reducer).", go: { view: "code", id: "st-flight-reducer-fill" } },
  { when: "Oct 5", text: "The question tells you the language: MapReduce API (Java) or Python. Marked on logic; syntax errors cost nothing.", go: { view: "plan" } },
  { when: "Oct 7", text: "\"Compute the median of this vector using MapReduce\": decide what goes in map, what goes in reduce, and whether you need a combiner.", go: { view: "code", id: "java-median" } },
  { when: "Oct 19", text: "The 2 coding questions come from: Hadoop Streaming, MapReduce/Java, Spark, Spark SQL. Spark DataFrames are \"super fast\".", go: { view: "code" } },
  { when: "Oct 21", text: "There will be a question on what a NodeManager and a ResourceManager do.", go: { view: "short", id: "sa-yarn-roles" } },
  { when: "Oct 21", text: "Short answer is about comparing, describing the difference, and explaining why. MapReduce = offline analytics, Spark = in-memory.", go: { view: "short" } },
  { when: "Oct 21", text: "MCQ: the options are probably all partly correct. Choose the BEST answer.", go: { view: "quiz" } },
  { when: "Oct 21", text: "Answer the way it was taught in the slides. He only marks his way as correct, so no methods from YouTube or other sites. Every model answer in this app follows the slide patterns.", go: { view: "code", id: "slide-style" } }
];

/* The slide/lab patterns to reuse in coding answers ("his way"). */
CP.slideStyle = [
  { track: "java", name: "Java MapReduce", items: [
    "`public class XMapper extends Mapper<LongWritable, Text, Text, IntWritable>` with `@Override public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException` (slide 9).",
    "`String line = value.toString();` … `context.write(new Text(k), new IntWritable(v));`",
    "`public class XReducer extends Reducer<Text, IntWritable, Text, IntWritable>`, `reduce(Text key, Iterable<IntWritable> values, Context context)`, loop `for (IntWritable value : values)`, use `value.get()`, `Math.max` / `Integer.MIN_VALUE` (slide 10).",
    "Driver: `Job job = new Job();` … `setMapperClass` / `setCombinerClass` / `setReducerClass` … `System.exit(job.waitForCompletion(true) ? 0 : 1);` (slides 11, 13)."
  ]},
  { track: "streaming", name: "Hadoop Streaming", items: [
    "Slide 24-25 style: `for line in sys.stdin:` … `sys.stdout.write(\"{}\\t{}\\n\".format(key, value))`; reducer tracks `curkey` and `total` (add the final write after the loop).",
    "Slide 26-27 style: `class Mapper(object)` / `class Reducer(object)` with `__init__(self, stream, sep=SEP)`, `emit(self, key, value)`, `map(self)` / `reduce(self)`, `__iter__`; reducer uses `groupby(self, itemgetter(0))`.",
    "Run inside `if __name__ == '__main__':`; test with `cat data | ./mapper.py | sort | ./reducer.py`."
  ]},
  { track: "pyspark", name: "PySpark", items: [
    "`sc.textFile(...)`, `flatMap`, `map(lambda x: (x, 1))`, `filter`, `reduceByKey(add)` with `from operator import add` (slides 12-13).",
    "Finish with an action: `collect()`, `count()`, `reduce()`, `saveAsTextFile(\"out\")`.",
    "Sort results on the driver like slide 16: `sorted(result, key=itemgetter(1))` (add `reverse=True` for largest first).",
    "Setup if asked: `conf = SparkConf().setMaster(\"local[*]\").setAppName(APP_NAME)`, `sc = SparkContext(conf=conf)`."
  ]},
  { track: "sql", name: "Spark SQL & DataFrames", items: [
    "Register then query (slides 20-21): `parking.registerTempTable(\"parking\")`, `sqlContext.sql(\"SELECT … FROM parking GROUP BY … HAVING … ORDER BY … DESC\")`, `.show()`. In the Databricks labs: `createOrReplaceTempView` + `spark.sql`.",
    "DataFrame API (slide 23): `.select(…).where(…).groupBy(…).agg(F.count(\"*\").alias(…), F.round(F.avg(…), 0).alias(…)).sort(\"count\", ascending=False)`.",
    "Lab moves: `spark.read.csv(path, header=True, inferSchema=True)`, `na.drop(subset=[…])`, `withColumn(\"Year\", year(col(\"dt\")))`, `filter(df[\"City\"] == \"New York\")`, `groupBy(\"Year\").avg(…).orderBy(\"Year\")`."
  ]}
];

CP.plan = [
  { id: "p1", day: "Wed night", text: "Skim Notes for MapReduce + Hadoop Streaming, then Code Lab Hadoop Streaming levels 1-3." },
  { id: "p2", day: "Wed night", text: "Code Lab Java MapReduce levels 1-3 (types, fill-ins, arrange the mapper)." },
  { id: "p3", day: "Wed night", text: "One 20-question quiz on all topics." },
  { id: "p4", day: "Thu morning", text: "Code Lab PySpark + Spark SQL levels 1-4." },
  { id: "p5", day: "Thu morning", text: "Exam-style coding: cheapest flight (Java), median (Java), one PySpark, one SQL." },
  { id: "p6", day: "Thu afternoon", text: "Short answer: YARN roles, MapReduce vs Spark, combiner, Hadoop vs RDBMS." },
  { id: "p7", day: "Thu evening", text: "Full 50-minute mock exam. Then redo everything in the mistakes pile." },
  { id: "p8", day: "Fri 8-10am", text: "Mistakes pile + compare tables + the professor's flags list. No new material." }
];

/* ---------------- Notes (cheat sheet) ---------------- */
CP.notes = {
  bigdata: [
    { h: "Definitions", items: [
      "**Data**: facts and statistics (measurements) collected for further analysis.",
      "**Information**: understanding of a subject, object, event or process, obtained from data.",
      "**Knowledge**: information understanding gained from experience or education.",
      "**Intelligence**: the ability to acquire and apply knowledge and skills (e.g. learning to drive).",
      "Knowledge cycle: Data → Information → Knowledge → Intelligence. Data processing capabilities: Skill → Rule → Knowledge triangle."
    ]},
    { h: "The 4 V's", items: [
      "**Volume**: high volume, more than a single PC can handle.",
      "**Variety**: many diverse sources (speech, text, images).",
      "**Velocity**: changing rapidly.",
      "**Value**: has some positive value (utility), not random.",
      "Definition (McKinsey 2011): data whose scale, distribution, diversity and/or timeliness require **new technical architectures and analytics** to unlock new business value. Needs new architectures, new methods/tools, and multiple skills (not only programming).",
      "Prof's summary: big data is data too big to load on a single PC."
    ]},
    { h: "Data structures (most to least structured)", items: [
      "**Structured**: defined type, format, structure. Transaction data, OLAP, RDBMS tables.",
      "**Semi-structured**: textual with a discernable pattern, parseable. XML defined by an XML schema.",
      "**Quasi-structured**: textual with erratic formats; can be formatted with effort, tools and time. Web clickstream data with inconsistencies.",
      "**Unstructured**: no inherent structure, stored as different file types. Text documents, PDFs, images, video.",
      "80-90% of future data growth comes from non-structured types."
    ]},
    { h: "Data explosion issues", items: [
      "Phones generate half the data traffic → **communication bottleneck**.",
      "Tracking everything, everywhere, anytime → **processing bottleneck**."
    ]},
    { h: "Analytics and BI vs data science", items: [
      "**Descriptive** (what happened? what is the problem?): standard reports, queries, alerts. This is **Business Intelligence**.",
      "**Predictive** (why is it happening? what happens next?): statistical and predictive modeling.",
      "**Prescriptive** (what if we try this? best scenario?): optimization, randomized testing.",
      "Value increases from descriptive to prescriptive. Data science adds predictive + prescriptive on top of BI.",
      "BI goals: optimize operations (sales, pricing, profitability), identify risk (churn, fraud, default), predict opportunities (upsell, cross-sell), meet regulations (anti-money laundering, fair lending).",
      "Typical big data tasks: problem-solving, learning, decision making, planning."
    ]},
    { h: "Data products and pipelines", items: [
      "**Data product**: any application that combines data and algorithms for inference or prediction (a system that learns from data). Examples: Amazon recommendations, outbreak prediction, autonomous vehicles.",
      "**Data science pipeline**: Ingestion → Wrangling → Modeling → Reporting.",
      "**Big data lifecycle**: Discovery (enough info to draft a plan?) → Data prep (enough good data to start building?) → Model planning (idea of model type?) → Model building (robust enough?) → Communicate results → Operationalize.",
      "Prof's question: how do you guarantee a model works once deployed? Test it rigorously and actively try to break it.",
      "Large-scale data analytics = Engineering (tools that turn science into deliverables) + Large-scale (TB/PB, can't fit in one computer's memory) + Analytics (algorithms that answer what happened … what's best)."
    ]},
    { h: "Tools (the analytic sandbox)", items: [
      "Analytics: Mahout, R, Python · High-level: Hive, Pig · Batch: Hadoop · Streaming: Storm, Kafka · In-memory: Spark, Giraph",
      "Data management: HBase, MongoDB, MySQL · Coordination: ZooKeeper · Cluster management: YARN · File systems: HDFS, GPFS",
      "Data scientist skills: competences (ML, data management, research methods, domain, math/stats), tools/platforms, programming, soft skills."
    ]}
  ],
  hadoop: [
    { h: "Why Hadoop?", items: [
      "A traditional file system fails for big data: not fault tolerant, **small blocks (512 B)**, large seek time, only one copy of the data.",
      "Hadoop is an open-source implementation of **two Google papers** describing a complete distributed computing system (storage + computation). Created by **Doug Cutting** (creator of Lucene); origins in **Apache Nutch**.",
      "Hadoop gives a reliable, scalable platform for storage and analysis on **commodity hardware** (unlike data warehouses)."
    ]},
    { h: "Why not an RDBMS with lots of disks?", items: [
      "**Seek time**: seeking is slow (latency) while transfer rate is bandwidth. A database **moves data to code**; Hadoop **moves code to data** and does streaming reads.",
      "**Rigid schema**: Hadoop is **schema-on-read** (interprets data at processing time), so it handles semi-structured and unstructured data, and loading is just a file copy.",
      "**Normal forms and latency**: normalization makes reading one record a non-local operation; Hadoop assumes high-speed streaming reads and writes."
    ]},
    { h: "Data locality", items: [
      "Hadoop co-locates data with compute nodes so access is local and fast. This is the heart of its performance.",
      "**Network bandwidth is the most precious resource** in a data center; Hadoop models network topology to conserve it."
    ]},
    { h: "Distributed system requirements", items: [
      "**Fault tolerance**: a component failure must not fail the whole system; degrade gracefully; a recovered component can rejoin.",
      "**Recoverability**: no data is lost on failure.",
      "**Consistency**: one failed job or task does not change the final result.",
      "**Scalability**: more load means lower performance, not failure; more resources means proportionally more capacity.",
      "How Hadoop meets them: data distributed immediately when added; stored in fixed-size **blocks (64, 128 or 256 MB)** replicated (default 3×); a job is split into **tasks**, one per block; nodes prefer local data; tasks are independent (no inter-process dependencies, so no deadlock); task redundancy for fault tolerance; a master allocates work to many workers in parallel."
    ]},
    { h: "Architecture: HDFS + YARN", items: [
      "**HDFS** manages data stored on disks across the cluster. **YARN** is the cluster resource manager: it allocates CPU and memory on worker nodes to applications.",
      "Together they are an \"operating system for big data\". Both minimize network traffic by keeping data local to computation.",
      "Cluster = machines running HDFS and YARN; each machine is a **node**. Scales **horizontally**: add nodes for a linear increase in capacity and performance. A cluster can be a single node.",
      "Daemons are background services; each runs in its **own JVM**.",
      "**Master nodes** run coordinating services and are entry points (usually on their own node so they don't compete for resources). **Worker nodes** are the majority; they store data and run tasks."
    ]},
    { h: "HDFS daemons", items: [
      "**NameNode** (master): directory tree, file metadata, block locations. Does NOT store data and does NOT pass data to clients. It is a traffic cop.",
      "**Secondary NameNode** (master): housekeeping and **checkpointing** for the NameNode. **Not a backup NameNode.**",
      "**DataNode** (worker): stores and manages blocks on local disk; reports health back to the NameNode.",
      "Read flow: client asks NameNode → gets a list of DataNodes → requests each block **directly from the DataNodes**."
    ]},
    { h: "YARN daemons (prof says this will be asked)", items: [
      "**ResourceManager** (master, one per cluster): allocates and monitors cluster resources (memory, cores) and schedules jobs. Tracks the status of nodes.",
      "**ApplicationMaster** (master, one per application): coordinates one application; negotiates resources with the RM and tracks the job's tasks.",
      "**NodeManager** (worker, one per node): creates containers, runs and manages tasks on its node, reports health and status.",
      "Flow: client requests resources from the RM → RM assigns an ApplicationMaster for the job → AM tracks execution → each NodeManager creates containers and runs tasks."
    ]}
  ],
  mapreduce: [
    { h: "The model", items: [
      "MapReduce is a **programming model** for data processing. Two phases, map and reduce, each with **key-value pairs** as input and output. Hadoop runs MR programs in Java, Ruby, R, Python…",
      "**Map**: transforms each input record into intermediate (key, value) pairs. Data preparation, and the right place to **drop bad records**.",
      "**Shuffle and sort**: the framework groups map output by key and sorts it.",
      "**Reduce**: processes each key with all its values, usually aggregating (max, sum, average…)."
    ]},
    { h: "Slide 8, step by step (prof: important)", items: [
      "Input lines arrive as (key = **byte offset of the line**, value = line text): (0, 0067011990…), (106, 0043011990…), …",
      "Map pulls out year and temperature, ignores the key, filters missing/bad readings: (1950, 0), (1950, 22), (1950, −11), (1949, 111), (1949, 78).",
      "Shuffle & sort groups by key: (1949, [111, 78]), (1950, [0, 22, −11]).",
      "Reduce takes the max of each list: (1949, 111), (1950, 22)."
    ]},
    { h: "NCDC weather records", items: [
      "Fixed-width text, one observation per line, no commas. Year = Java `line.substring(15, 19)`. Temperature = `substring(87, 92)` (sign + 4 digits, tenths of °C, e.g. +0023 = 2.3 °C).",
      "Quality code = `substring(92, 93)`; OK if it matches `[01459]`. **9999 = missing**.",
      "Unix/awk does it on one machine, but parallelizing is hard: (1) unequal file sizes, (2) combining results needs extra work, (3) one machine's capacity, plus coordination and reliability."
    ]},
    { h: "Java API cheat sheet", items: [
      "`public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>`: input key, input value, output key, output value.",
      "`public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException`. Use `value.toString()`; emit with `context.write(new Text(year), new IntWritable(temp))`.",
      "`public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>`, then `public void reduce(Text key, Iterable<IntWritable> values, Context context)`. Loop with `for (IntWritable v : values)` and use `v.get()`.",
      "Driver: `Job job = new Job();` · `setJarByClass` · `setJobName` · `FileInputFormat.addInputPath(job, new Path(args[0]))` · `FileOutputFormat.setOutputPath(job, new Path(args[1]))` · `setMapperClass` · `setCombinerClass` (optional) · `setReducerClass` · `setOutputKeyClass` · `setOutputValueClass` · `System.exit(job.waitForCompletion(true) ? 0 : 1)`.",
      "Types: Text ↔ String, IntWritable ↔ int, LongWritable ↔ long, DoubleWritable ↔ double. `extends` = inheritance: your class is a Mapper and overrides `map()` (`@Override`)."
    ]},
    { h: "Data flow (slide 12)", items: [
      "**One map task per input split.** 3 splits → 3 map tasks.",
      "The number of **reduce tasks is set by the job** (`mapreduce.job.reduces`, `job.setNumReduceTasks(n)`), not by the number of splits. Default is 1.",
      "1 reducer: every map's sorted output is copied to it, merged, reduced, and written as one file (part-0) to HDFS.",
      "Several reducers: each map **partitions** its output by key, so all values for one key reach the same reducer. Each reducer writes its own part file (part-0, part-1, …). More reducers means more parallelism and more output files.",
      "For one global answer (overall max, overall median), use a single reducer, or a constant key."
    ]},
    { h: "Combiner", items: [
      "Runs on each map's output before the shuffle; its output becomes the reducer's input. It cuts the data sent over the network (many jobs are bandwidth-limited).",
      "Safe only when combining partial results gives the same answer: **max, min, sum, count**. `job.setCombinerClass(MaxTemperatureReducer.class)`.",
      "**Not safe for average or median.** The mean of means ≠ the overall mean, and the median of medians ≠ the median. (For an average you can combine (sum, count) pairs.)"
    ]},
    { h: "How a job runs (5 entities)", items: [
      "Client · YARN ResourceManager · YARN NodeManagers · MapReduce ApplicationMaster · shared filesystem (HDFS).",
      "1. `submit()` / `waitForCompletion()` creates a JobSubmitter. 2. It asks the RM for an application ID, **checks the output spec** (fails if output dir missing or already exists) and **computes input splits**. 3. Copies JAR, config and splits to HDFS. 4. `submitApplication()` on the RM.",
      "5. RM scheduler allocates a container; NM launches the AM (**MRAppMaster**). 6. AM sets up bookkeeping. 7. AM creates **one map task per split** plus `mapreduce.job.reduces` reduce tasks.",
      "**Uber task**: small job run inside the AM's JVM, no separate containers. 8. Otherwise the AM requests containers from the RM. 9. AM asks the NM to start the container; **YarnChild** runs the task. 10. YarnChild localizes JAR/config/cache files. 11. Runs map or reduce."
    ]},
    { h: "Failures", items: [
      "**Task**: user code throws → JVM reports to the AM → attempt marked failed, container freed → AM reschedules, **avoiding the node where it failed**. After **4 failed attempts** it isn't retried (configurable). A failed map task fails the job; failed reduce tasks leave partial results (per the slides).",
      "**ApplicationMaster**: sends heartbeats to the RM. On failure the RM starts a new AM in a new container, which uses the **job history** to recover finished tasks. The client times out and asks the RM for the new AM's address (transparent to the user).",
      "**NodeManager**: no heartbeat for **10 minutes** → RM removes it. Completed map tasks of unfinished jobs on that node are **rerun**, because their output was on that node's local disk. The AM **blacklists** a NM if more than **3 tasks** fail on it.",
      "**ResourceManager**: single point of failure by default. HA = **active-standby pair**; state stored in HDFS or ZooKeeper; a failover controller uses **ZooKeeper leader election** so only one RM is active; clients and NMs try RMs **round-robin**."
    ]}
  ],
  streaming: [
    { h: "What it is", items: [
      "Hadoop Streaming is a utility (a JAR shipped with Hadoop) that lets any executable, e.g. a Python script, be the mapper and reducer.",
      "\"Streaming\" means the **Unix standard streams** (stdin, stdout, stderr). It is NOT real-time processing like Spark Streaming or Storm.",
      "You pass the input/output HDFS paths and the mapper and reducer executables to the job."
    ]},
    { h: "Mapper", items: [
      "Each map task launches your script in its own process, pipes input lines to its **stdin**, and collects its **stdout**.",
      "Output format: `key<TAB>value` per line. **Tab is the default separator.** No separator means the whole line is the key and the value is null.",
      "Word count mapper: for each line in `sys.stdin`, for each word in `line.split()`, write `\"{}\\t1\\n\".format(word)`."
    ]},
    { h: "Reducer", items: [
      "Launched after shuffle & sort. Lines arrive on stdin **sorted and grouped by key, one line at a time** (not as lists like in Java).",
      "So track the current key; when the key changes, emit the finished total and reset. **Emit the last key after the loop.** The slide 25 version forgets this and loses the last key.",
      "Output uses the same `key<TAB>value` format as the mapper."
    ]},
    { h: "Class style (slides 26-27, prof: know emit/reduce)", items: [
      "`Mapper.__init__(self, stream, sep=SEP)` stores the stream and separator. `emit(self, key, value)` writes `key<SEP>value\\n` to stdout. `map(self)` loops `for row in self:` and emits `row[3], row[6]` (origin, departure delay). `__iter__` wraps `csv.reader(self.stream)` and yields rows.",
      "`Reducer.reduce(self)`: `for current, group in groupby(self, itemgetter(0))`, sum and count the values, then `self.emit(current, float(total) / float(count))`. `__iter__` splits each line on the separator and yields `(key, float(value))`, skipping bad lines.",
      "`groupby` only works because the input is sorted by key.",
      "Flight CSV fields: 0 date, 1 airline, 2 flightnum, 3 origin, 4 dest, 5 dep, 6 dep_delay, 7 arv, 8 arv_delay, 9 airtime, 10 distance."
    ]},
    { h: "Running it", items: [
      "Both scripts run inside `if __name__ == '__main__':` and start with the shebang `#!/usr/bin/env python`.",
      "`chmod +x mapper.py reducer.py` makes them executable.",
      "Test locally without Hadoop: `cat flights.csv | ./mapper.py | sort | ./reducer.py`. **`sort` plays the role of shuffle & sort.**"
    ]}
  ],
  spark: [
    { h: "What Spark is", items: [
      "A cluster computing platform with a MapReduce-like API, built to be fast for **interactive queries and iterative algorithms** by **caching data in memory** across the cluster.",
      "Runs on YARN, so it can use an existing Hadoop cluster and read HDFS, S3, HBase, Cassandra. APIs in Python, Java, Scala, SQL.",
      "More than map and reduce: filter, sample, join, collect, and more (functional programming).",
      "Prof: MapReduce = offline (batch) analytics on disk; Spark = in-memory."
    ]},
    { h: "Why Spark: iterative algorithms", items: [
      "Iterative algorithms on MapReduce become **chained jobs**, and each job reads its input from disk and writes results back, which causes heavy I/O delay.",
      "Spark keeps the dataset in memory between iterations.",
      "You describe a whole data flow of transformations, then an action. Data flows are **DAGs** (directed acyclic graphs), so the engine plans distribution ahead of time (breaks the job into stages)."
    ]},
    { h: "The Spark stack", items: [
      "**Spark Core** (RDDs, scheduling) under **Spark SQL** (originally HiveQL), **Spark Streaming** (not on midterm), **MLlib** (machine learning: classification, regression), **GraphX** (graphs).",
      "Spark focuses on **computation, not storage**; it relies on Hadoop (HDFS) and runs on a cluster manager such as YARN."
    ]},
    { h: "RDDs", items: [
      "**Resilient Distributed Dataset**: a **read-only (immutable)** collection of objects **partitioned** across machines.",
      "Fault tolerant: can be **rebuilt from lineage** (the recorded history of transformations). Spark recomputes lost partitions instead of replicating them.",
      "Accessed with parallel operations; read from and written to distributed storage; can be **cached in worker memory** for reuse.",
      "You create RDDs by loading data (`sc.textFile`), parallelizing a collection (`sc.parallelize`), or transforming another RDD."
    ]},
    { h: "Transformations vs actions", items: [
      "**Transformations** create a new RDD and are **lazy**: map, flatMap, filter, reduceByKey, sortBy, distinct, join.",
      "**Actions** return a result to the driver or write output, and **trigger execution**: reduce, collect, count, take, first, saveAsTextFile.",
      "Lazy evaluation: Spark looks at the whole chain plus the action before running anything, so it can optimize storage and computation.",
      "Typical flow: define RDDs (load / parallelize / transform / cache) → pass **closures** (functions) to operations → finish with an aggregating action.",
      "Heads-up: slide 13's text calls reduceByKey an \"action\", but in Spark it is a transformation (it returns a new RDD), and the same slide says the job only kicks off at `saveAsTextFile`. The review sheet's examples are safe: actions = reduce(), collect(); transformations = map(), filter(). If a question hinges on reduceByKey, it's worth asking the TA which answer he expects."
    ]},
    { h: "Execution", items: [
      "An application = independent processes coordinated by a **SparkContext** in the **driver program**.",
      "SparkContext connects to a cluster manager (e.g. YARN) which allocates resources. Each worker runs an **executor** that manages computation, storage and caching.",
      "Code is sent from the driver to the executors; executors talk back to the driver, so the **driver should be on the same network** as the cluster (unlike Hadoop, where you can submit from anywhere).",
      "**yarn-client** mode: driver runs in the client process; good for interactive work and immediate results.",
      "**yarn-cluster** mode: driver runs inside the **ApplicationMaster**; good for long-running jobs that need no user input."
    ]},
    { h: "PySpark examples from the slides", items: [
      "`text = sc.textFile(\"shakespeare.txt\")` → `words = text.flatMap(tokenize)` → `wc = words.map(lambda x: (x, 1))` → `counts = wc.reduceByKey(add)` → `counts.saveAsTextFile(\"wc\")` → folder with `part-00000`, `part-00001`, `_SUCCESS`.",
      "`flatMap` flattens each line's list of words into one RDD of words. `map` would give an RDD of lists.",
      "`wc.toDebugString()` prints the lineage.",
      "Airline app: `conf = SparkConf().setMaster(\"local[*]\").setAppName(APP_NAME)`, `sc = SparkContext(conf=conf)`. Load airline names with `dict(sc.textFile(...).map(split).collect())`, ship them with `sc.broadcast(...)`, parse rows into a `Flight` namedtuple, map to `(airline_lookup.value[f.airline], add(f.dep_delay, f.arv_delay))`, `reduceByKey(add).collect()`, then `sorted(delays, key=itemgetter(1))` on the driver. Run with `spark-submit app.py`."
    ]}
  ],
  sparksql: [
    { h: "Spark SQL", items: [
      "A Spark module with a relational interface to structured data. Access via **JDBC/ODBC**, the built-in Hive console, or the built-in APIs (same interface as RDDs, e.g. in Databricks).",
      "Originally gave APIs to query Spark with HiveQL (Hive's SQL)."
    ]},
    { h: "Load and query JSON (slides 20-21)", items: [
      "`sqlContext = SQLContext(sc)`.",
      "`sc.wholeTextFiles(path)` gives an RDD of `(filename, content)` pairs → `.map(lambda (x, y): y)` keeps the content → `.flatMap(lambda x: json.loads(x))` parses each file into records (flatMap because one file can hold many) → `.map(lambda x: json.dumps(x)).saveAsTextFile(out)`.",
      "`parking = sqlContext.read.json(path)` → `parking.registerTempTable(\"parking\")` (newer: `createOrReplaceTempView`). **You must register a table before SQL can reference it.**",
      "`aggr_by_type = sqlContext.sql(\"SELECT primetype, secondtype, count(1) AS count, round(avg(regcap), 0) AS avg_spaces FROM parking GROUP BY primetype, secondtype HAVING trim(primetype) != '' ORDER BY count DESC\")` then `aggr_by_type.show()`."
    ]},
    { h: "SQL clause order", items: [
      "SELECT → FROM → WHERE (filters **rows, before** grouping) → GROUP BY → HAVING (filters **groups, after** grouping; can use aggregates) → ORDER BY … DESC → LIMIT.",
      "`count(1)` / `count(*)`: rows per group. `avg(x)`, `round(x, 0)`, `trim(s)`, `max`, `min`, `sum`."
    ]},
    { h: "DataFrames", items: [
      "A tabular, distributed collection with a **defined schema**. Unlike pandas or R, a Spark DataFrame **wraps an RDD** (an RDD of Row objects).",
      "Optimizations compile the query plan into executable code: up to **4-5× faster** than hand-coded RDDs (10 million integer pairs benchmark). DataFrame speed is about the same in Python, Scala and SQL; Python RDD code is slowest.",
      "Higher-level APIs bring expressive APIs in general-purpose languages plus one unified interface for SQL, streaming and ML."
    ]},
    { h: "DataFrame API (slide 23)", items: [
      "`from pyspark.sql import functions as F`",
      "`parking.select(\"primetype\", \"secondtype\", \"regcap\").where(\"trim(primetype) != ''\").groupBy(\"primetype\", \"secondtype\").agg(F.count(\"*\").alias(\"count\"), F.round(F.avg(\"regcap\"), 0).alias(\"avg_spaces\")).sort(\"count\", ascending=False)`",
      "Advantage over raw SQL: build a complex query step by step, testing each chained operation, with many built-in functions."
    ]},
    { h: "Lab notebook moves (Databricks)", items: [
      "`df = spark.read.csv(path, header=True, inferSchema=True)` (inferSchema detects types; without it every column is a string).",
      "`df.show(5)`, `df.count()`, `df.columns`, `df.printSchema()`, `df.describe().show()`.",
      "`df.na.drop(subset=[\"AverageTemperature\"])` drops rows with nulls in those columns.",
      "`df.withColumn(\"Year\", year(col(\"dt\")))`, `df.filter(df[\"City\"] == \"New York\")`, `df.filter(col(\"Year\") >= 1900)`.",
      "`df.groupBy(\"Year\").avg(\"AverageTemperature\").orderBy(df[\"Year\"].desc())` or `.agg(max(\"AverageTemperature\").alias(\"MaxTemp\"))`.",
      "`.toPandas()` to plot with matplotlib (collects to the driver; only for small results)."
    ]}
  ]
};

/* ---------------- Compare tables ---------------- */
CP.compare = [
  { title: "MapReduce vs Spark", topic: "spark", cols: ["", "Hadoop MapReduce", "Spark"], rows: [
    ["Intermediate data", "Written to disk between steps; chained jobs re-read from disk", "Kept in memory (cached RDDs)"],
    ["Best for", "Offline batch analytics", "Iterative algorithms (ML), interactive queries"],
    ["Programming model", "Only map and reduce", "Many transformations + actions (filter, join, sample…)"],
    ["Planning", "Each job runs as written", "Lazy DAG, optimized before running"],
    ["Fault tolerance", "Re-run failed tasks; HDFS replication", "Recompute lost partitions from lineage"],
    ["Languages", "Java (others via Streaming)", "Python, Java, Scala, R, SQL"]
  ]},
  { title: "YARN daemons", topic: "hadoop", cols: ["", "ResourceManager", "ApplicationMaster", "NodeManager"], rows: [
    ["Type", "Master", "Master (per application)", "Worker"],
    ["How many", "One per cluster (two with HA)", "One per running application", "One per worker node"],
    ["Job", "Allocates and monitors cluster resources; schedules jobs; tracks node status", "Coordinates one job: requests containers, tracks its tasks", "Creates containers, runs tasks, reports node health"],
    ["If it fails", "Single point of failure unless active-standby HA", "RM restarts it; job history recovers done tasks", "RM drops it after 10 min without heartbeat; its map outputs are rerun"]
  ]},
  { title: "HDFS daemons", topic: "hadoop", cols: ["", "NameNode", "Secondary NameNode", "DataNode"], rows: [
    ["Type", "Master", "Master", "Worker"],
    ["Job", "Directory tree, metadata, block locations", "Housekeeping, checkpointing", "Stores blocks on local disk"],
    ["Stores file data?", "No (traffic cop)", "No", "Yes"],
    ["Common trap", "Doesn't pass data to the client", "NOT a backup NameNode", "Reports health to the NameNode"]
  ]},
  { title: "Hadoop vs RDBMS", topic: "hadoop", cols: ["", "Hadoop", "RDBMS"], rows: [
    ["Access", "Streaming reads; move code to data", "Disk seeks; move data to code"],
    ["Schema", "Schema-on-read", "Rigid, predefined schema"],
    ["Data", "Semi-structured and unstructured are fine", "Structured, normalized"],
    ["Loading", "Just a file copy", "Costly loading phase"]
  ]},
  { title: "Transformations vs actions", topic: "spark", cols: ["", "Transformation", "Action"], rows: [
    ["Returns", "A new RDD", "A value to the driver, or writes output"],
    ["Runs when", "Lazily, not until an action", "Immediately; triggers the job"],
    ["Examples", "map, flatMap, filter, reduceByKey, sortBy, join", "reduce, collect, count, take, saveAsTextFile"]
  ]},
  { title: "Combiner vs reducer", topic: "mapreduce", cols: ["", "Combiner", "Reducer"], rows: [
    ["Runs on", "Each map task's local output", "All values for a key, after shuffle"],
    ["Required?", "Optional optimization", "Usually required"],
    ["Purpose", "Cut data sent over the network", "Produce the final answer"],
    ["Safe for", "max, min, sum, count", "Anything"],
    ["Not safe for", "average, median (directly)", "n/a"]
  ]},
  { title: "Reducer input: Java vs Streaming", topic: "streaming", cols: ["", "Java MapReduce", "Hadoop Streaming (Python)"], rows: [
    ["What reduce gets", "`reduce(key, Iterable<values>, context)`, one call per key", "Sorted `key\\tvalue` lines on stdin, one at a time"],
    ["Grouping", "Done for you", "You detect key changes (or use groupby)"],
    ["Emit", "`context.write(k, v)`", "`sys.stdout.write(\"{}\\t{}\\n\".format(k, v))`"],
    ["Gotcha", "Convert with `.get()`", "Emit the last key after the loop"]
  ]},
  { title: "Spark on YARN modes", topic: "spark", cols: ["", "yarn-client", "yarn-cluster"], rows: [
    ["Driver runs in", "The client process", "The ApplicationMaster"],
    ["AM's role", "Only manages progress, requests resources", "Hosts the driver"],
    ["Use for", "Interactive work, immediate results", "Long-running jobs, no user intervention"]
  ]},
  { title: "RDD vs DataFrame vs Spark SQL", topic: "sparksql", cols: ["", "RDD", "DataFrame API", "Spark SQL"], rows: [
    ["Structure", "Any objects, no schema", "Rows with a schema (wraps an RDD)", "Tables (registered DataFrames)"],
    ["Style", "Functional: map, filter, reduceByKey", "Chained methods: select, where, groupBy, agg", "SQL strings via `sqlContext.sql()`"],
    ["Speed", "Slowest (Python RDD slowest of all)", "Up to 4-5× faster than RDDs", "Same engine as DataFrames"]
  ]},
  { title: "Analytics types", topic: "bigdata", cols: ["", "Descriptive (BI)", "Predictive", "Prescriptive"], rows: [
    ["Question", "What happened? What's the problem?", "Why? What happens next?", "What if? What's the best scenario?"],
    ["Methods", "Standard reports, queries, alerts", "Statistical / predictive modeling", "Optimization, randomized testing"]
  ]},
  { title: "Failure handling summary", topic: "mapreduce", cols: ["What fails", "Detected by", "Recovery"], rows: [
    ["Task", "Task JVM reports error to the AM", "Rescheduled on another node; give up after 4 attempts"],
    ["ApplicationMaster", "RM misses its heartbeats", "New AM in new container; job history restores finished tasks"],
    ["NodeManager", "RM: no heartbeat for 10 min", "Removed; completed map tasks of unfinished jobs rerun"],
    ["ResourceManager", "Failover controller (ZooKeeper)", "Standby RM becomes active (only with HA)"]
  ]}
];

/* ---------------- Multiple choice ---------------- */
CP.mcq = [
  // bigdata
  { id: "bd1", topic: "bigdata", q: "Which \"V\" of big data means the data is changing rapidly?", options: ["Volume", "Variety", "Velocity", "Value"], answer: 2, explain: "Volume = high volume, Variety = many diverse sources, Velocity = changing rapidly, Value = has positive utility.", src: "Intro slide 6" },
  { id: "bd2", topic: "bigdata", q: "Web clickstream data that contains some inconsistencies in values and formats is best described as:", options: ["Structured", "Semi-structured", "Quasi-structured", "Unstructured"], answer: 2, explain: "Quasi-structured = textual data with erratic formats that can be formatted with effort, tools and time. Clickstream is the slide's example.", src: "Intro slide 11" },
  { id: "bd3", topic: "bigdata", q: "An XML file that is self-describing and defined by an XML schema is:", options: ["Structured", "Semi-structured", "Quasi-structured", "Unstructured"], answer: 1, explain: "Semi-structured = textual files with a discernable pattern that enables parsing. XML with a schema is the slide's example.", src: "Intro slide 11" },
  { id: "bd4", topic: "bigdata", q: "What is the correct order of the knowledge cycle?", options: ["Information → Data → Knowledge → Intelligence", "Data → Information → Knowledge → Intelligence", "Data → Knowledge → Information → Intelligence", "Intelligence → Knowledge → Information → Data"], answer: 1, explain: "Data (facts) become Information (understanding), then Knowledge (from experience/education), then Intelligence (ability to apply it).", src: "Intro slide 4" },
  { id: "bd5", topic: "bigdata", q: "\"What if we try this? What is the best scenario?\" is answered by which kind of analytics?", options: ["Descriptive", "Predictive", "Prescriptive", "Standard reporting"], answer: 2, explain: "Prescriptive analytics (optimization, randomized testing). Predictive = why/what next. Descriptive = what happened.", src: "Intro slide 21" },
  { id: "bd6", topic: "bigdata", q: "Which BEST describes the difference between Business Intelligence and data science?", options: ["BI handles bigger data than data science", "BI focuses on descriptive analytics (what happened), while data science adds predictive and prescriptive analytics", "Data science only works on structured data", "BI uses machine learning while data science uses reports"], answer: 1, explain: "On slide 21, BI sits at the descriptive level (reports, queries, alerts). Data science moves up to predictive and prescriptive, where more value is.", src: "Intro slides 2, 21" },
  { id: "bd7", topic: "bigdata", q: "Smartphones generating half of all data traffic mainly creates which problem?", options: ["Processing bottleneck", "Communication bottleneck", "Storage bottleneck", "Schema bottleneck"], answer: 1, explain: "Mobile traffic → communication bottleneck. Tracking everything, everywhere, anytime → processing bottleneck.", src: "Intro slide 9" },
  { id: "bd8", topic: "bigdata", q: "\"Do I have enough good quality data to start building the model?\" is asked in which lifecycle phase?", options: ["Discovery", "Data preparation", "Model planning", "Operationalize"], answer: 1, explain: "Discovery = enough info to draft a plan? Data prep = enough good data? Model planning = which model type? Model building = robust enough?", src: "Hadoop slide 5" },
  { id: "bd9", topic: "bigdata", q: "Which is the BEST definition of a data product?", options: ["A dashboard of historical reports", "A cleaned database ready for analysis", "An application that combines data with algorithms for inference or prediction", "Any software that stores large files"], answer: 2, explain: "A data product combines data and statistical algorithms for inference or prediction: a system that learns from data (e.g. Amazon recommendations).", src: "Hadoop slide 3" },
  { id: "bd10", topic: "bigdata", q: "Customer churn, fraud and loan default fall under which business goal?", options: ["Optimize business operations", "Identify business risk", "Predict new business opportunities", "Regulatory requirements"], answer: 1, explain: "Identify business risk: churn, fraud, default. Opportunities = upsell, cross-sell. Regulatory = anti-money laundering, fair lending.", src: "Intro slide 20" },
  { id: "bd11", topic: "bigdata", q: "What are the stages of the data science pipeline?", options: ["Ingestion → Wrangling → Modeling → Reporting", "Map → Shuffle → Reduce → Output", "Discovery → Reporting → Wrangling → Modeling", "Collect → Store → Delete → Archive"], answer: 0, explain: "Ingestion → wrangling (normalize, standardize) → modeling → reporting.", src: "Hadoop slide 4" },
  { id: "bd12", topic: "bigdata", q: "In \"large-scale data analytics\", large-scale means:", options: ["More than 1 million rows", "Data of huge size and different types that cannot fit into a single computer's memory (TB, PB)", "Any data stored in the cloud", "Data that needs more than one database table"], answer: 1, explain: "Large-scale = huge size and different types that can't fit into one computer's memory (TB, PB).", src: "Intro slide 18" },

  // hadoop
  { id: "hd1", topic: "hadoop", review: true, q: "Which of the following best explains why Hadoop is preferred for big data analytics over traditional file systems?", options: ["Hadoop allows the storage of larger data blocks in HDFS", "Traditional file systems cannot handle unstructured data efficiently due to a rigid schema and slow seek times", "Hadoop is designed to work with smaller file sizes but offers better performance than traditional systems", "Hadoop offers better CPU utilization compared to other distributed systems"], answer: 1, explain: "This is the professor's own review answer. Option A is partly true (bigger blocks), which is exactly the \"all partly correct, pick the best\" style. B names the real reasons: rigid schema and seek time.", src: "Review MCQ 1" },
  { id: "hd2", topic: "hadoop", review: true, q: "In a Hadoop cluster, what role does the Secondary NameNode play?", options: ["It acts as a backup for the NameNode in case of failure", "It performs housekeeping tasks like checkpointing to assist the NameNode", "It manages the data storage on worker nodes", "It acts as a primary coordinator for data replication across the cluster"], answer: 1, explain: "Despite its name, it is NOT a backup. It does checkpointing and housekeeping for the NameNode.", src: "Review MCQ 2, Hadoop slide 17" },
  { id: "hd3", topic: "hadoop", review: true, q: "In Hadoop's YARN architecture, what is the role of the ResourceManager?", options: ["It manages the data stored in HDFS", "It assigns computing resources to jobs and handles scheduling across the cluster", "It coordinates data replication across nodes in the cluster", "It monitors task failures and reschedules them when needed"], answer: 1, explain: "RM = allocates and monitors cluster resources and schedules jobs. Rescheduling failed tasks (D) is the ApplicationMaster's job. HDFS data is the NameNode/DataNodes.", src: "Review MCQ 9, Hadoop slide 18" },
  { id: "hd4", topic: "hadoop", q: "A client wants to read a file from HDFS. What happens?", options: ["The NameNode reads the blocks and streams them to the client", "The client asks the NameNode for block locations, then reads each block directly from the DataNodes", "The client asks the ResourceManager, which returns the data", "The Secondary NameNode serves the data while the NameNode is busy"], answer: 1, explain: "The NameNode is a traffic cop: it returns which DataNodes hold the blocks. Data never flows through the NameNode.", src: "Hadoop slide 17" },
  { id: "hd5", topic: "hadoop", q: "Which daemon stores the file system's directory tree, file metadata and block locations?", options: ["DataNode", "NameNode", "NodeManager", "ApplicationMaster"], answer: 1, explain: "NameNode (HDFS master). DataNodes store the actual blocks.", src: "Hadoop slide 17" },
  { id: "hd6", topic: "hadoop", q: "Which daemon creates containers, runs processing tasks on one worker node, and reports their health?", options: ["ResourceManager", "NodeManager", "NameNode", "DataNode"], answer: 1, explain: "NodeManager = YARN worker. The prof said a NodeManager vs ResourceManager question is coming.", src: "Hadoop slide 18" },
  { id: "hd7", topic: "hadoop", q: "Which YARN component coordinates a single application for the duration of the job?", options: ["ResourceManager", "NodeManager", "ApplicationMaster", "Secondary NameNode"], answer: 2, explain: "The RM assigns an application-specific ApplicationMaster, which tracks that job's execution.", src: "Hadoop slide 18" },
  { id: "hd8", topic: "hadoop", q: "A database \"moves data to code\". Hadoop instead:", options: ["Moves code to data", "Moves data to the NameNode", "Moves both data and code to the client", "Copies all data to every node"], answer: 0, explain: "Data locality: computation runs where the data is stored, saving network bandwidth.", src: "Hadoop slides 8-9" },
  { id: "hd9", topic: "hadoop", q: "Hadoop works well on semi-structured and unstructured data mainly because it:", options: ["Normalizes all data on load", "Uses schema-on-read, interpreting data at processing time", "Converts all data into XML", "Requires a predefined schema for every file"], answer: 1, explain: "Schema-on-read gives flexibility and avoids the costly loading phase of an RDBMS (loading is just a file copy).", src: "Hadoop slide 8" },
  { id: "hd10", topic: "hadoop", q: "Why is data normalization a problem for Hadoop processing?", options: ["Normalized data is too small", "It makes reading a record a non-local operation, but Hadoop assumes high-speed streaming reads and writes", "Hadoop cannot store numbers", "Normalization removes the keys MapReduce needs"], answer: 1, explain: "Normalization spreads a record across tables, so reading it needs non-local lookups.", src: "Hadoop slide 8" },
  { id: "hd11", topic: "hadoop", q: "\"If a component fails, the system degrades gracefully, and a recovered component can rejoin.\" Which requirement is this?", options: ["Fault tolerance", "Recoverability", "Consistency", "Scalability"], answer: 0, explain: "Recoverability = no data lost. Consistency = a failed task doesn't change the result. Scalability = more load slows things down without failure.", src: "Hadoop slide 10" },
  { id: "hd12", topic: "hadoop", q: "\"The failure of one job or task should not affect the final result.\" Which requirement is this?", options: ["Fault tolerance", "Recoverability", "Consistency", "Scalability"], answer: 2, explain: "Consistency. The slide's example is computing the average of a very large column.", src: "Hadoop slide 10" },
  { id: "hd13", topic: "hadoop", q: "What block size does Hadoop typically use?", options: ["512 bytes", "4 KB", "64, 128 or 256 MB", "10 GB"], answer: 2, explain: "Large fixed blocks (64/128/256 MB), each replicated. 512 B is the traditional file system block size from slide 6.", src: "Hadoop slides 6, 10" },
  { id: "hd14", topic: "hadoop", q: "Why should Hadoop tasks not communicate with each other during processing?", options: ["To save memory", "To avoid inter-process dependencies that could lead to deadlock", "Because nodes have no network", "So the NameNode can read their data"], answer: 1, explain: "Independent tasks mean no inter-process dependencies and no deadlock, and the system can minimize network traffic.", src: "Hadoop slide 11" },
  { id: "hd15", topic: "hadoop", q: "How does a Hadoop cluster scale?", options: ["Vertically, by buying a bigger master node", "Horizontally: adding nodes increases capacity and performance roughly linearly", "It cannot scale beyond one rack", "Only by adding more NameNodes"], answer: 1, explain: "All clusters scale horizontally; capacity and performance grow linearly as nodes are added.", src: "Hadoop slide 15" },
  { id: "hd16", topic: "hadoop", q: "According to Hadoop's design, the most precious resource in a data center is:", options: ["CPU cycles", "Disk space", "Network bandwidth", "RAM"], answer: 2, explain: "It's easy to saturate network links by copying data around, so Hadoop models network topology to conserve bandwidth.", src: "Hadoop slide 9" },
  { id: "hd17", topic: "hadoop", q: "Which statement about Hadoop's origin is correct?", options: ["It was created at Microsoft for SQL Server", "It is an open-source implementation of two Google papers; created by Doug Cutting with origins in Apache Nutch", "It is a hardware appliance you buy and maintain", "It was designed as a replacement for Python"], answer: 1, explain: "Two Google papers (distributed file system + MapReduce). Doug Cutting, creator of Lucene. Origins in Apache Nutch, a web search engine.", src: "Hadoop slides 6, 15" },
  { id: "hd18", topic: "hadoop", q: "Why are master daemons usually run on their own node?", options: ["Because they need GPUs", "So they don't compete for resources and become a bottleneck", "Because worker nodes can't run Java", "To store more HDFS blocks"], answer: 1, explain: "Master processes are critical. In small clusters they may share one node.", src: "Hadoop slide 16" },

  // mapreduce
  { id: "mr1", topic: "mapreduce", review: true, q: "In MapReduce, which phase is responsible for grouping intermediate key-value pairs by key before sending them to the reducer?", options: ["Map phase", "Sort phase", "Shuffle phase", "Combiner phase"], answer: 2, explain: "The professor's answer is Shuffle. \"Sort\" is partly right (it's called shuffle and sort), which shows how he writes distractors.", src: "Review MCQ 4" },
  { id: "mr2", topic: "mapreduce", review: true, q: "What is the primary advantage of using the Combiner function in MapReduce jobs?", options: ["It merges all intermediate results into a final output", "It reduces the amount of data transferred between the map and reduce phases, improving network efficiency", "It ensures that the reduce phase processes the data in parallel", "It converts semi-structured data into structured data during the map phase"], answer: 1, explain: "Many jobs are limited by cluster bandwidth. The combiner aggregates locally on the map side so less data crosses the network.", src: "Review MCQ 8, MR slide 13" },
  { id: "mr3", topic: "mapreduce", review: true, q: "What happens when a task fails four times in a Hadoop MapReduce job by default?", options: ["The entire MapReduce job fails and does not proceed further", "The job continues, but the failed task is skipped", "The failed task is rescheduled, and after four failures, the job is terminated", "The task is rerun on the same node where it originally failed"], answer: 2, explain: "Professor's answer: C. Failed attempts are rescheduled (avoiding the failing node), and after 4 failures there are no more retries.", src: "Review MCQ 6, MR slide 18" },
  { id: "mr4", topic: "mapreduce", q: "With the default text input format, what is the KEY passed to the map function?", options: ["The line number", "The byte offset of the start of the line in the file", "The year", "The file name"], answer: 1, explain: "Keys are offsets: 0, 106, 212… (each NCDC line is 106 characters). The max temperature mapper ignores the key.", src: "MR slides 7-8" },
  { id: "mr5", topic: "mapreduce", q: "In `extends Mapper<LongWritable, Text, Text, IntWritable>`, what is the third type (Text)?", options: ["Input key type", "Input value type", "Output key type", "Output value type"], answer: 2, explain: "Order: input key, input value, output key, output value. The mapper emits (Text year, IntWritable temperature).", src: "MR slide 9" },
  { id: "mr6", topic: "mapreduce", q: "The reducer receives (1950, [0, 22, −11]). In the max temperature job, what does it write?", options: ["(1950, 11)", "(1950, 22)", "(1950, [22])", "(1950, −11)"], answer: 1, explain: "It loops the values keeping `Math.max`, then writes `(key, maxValue)`.", src: "MR slides 8, 10" },
  { id: "mr7", topic: "mapreduce", q: "For which job can you reuse the reducer class as the combiner without changing the result?", options: ["Average temperature per year", "Median temperature per year", "Maximum temperature per year", "All of them"], answer: 2, explain: "max(max(a, b), max(c)) = max(a, b, c). The average of averages and the median of medians are wrong in general.", src: "MR slide 13" },
  { id: "mr8", topic: "mapreduce", q: "A job's input has 3 splits. Which statement is correct?", options: ["3 map tasks run; the number of reduce tasks is set separately by the job (default 1)", "3 map tasks and exactly 3 reduce tasks always run", "1 map task processes all 3 splits", "The number of reducers must equal the number of mappers"], answer: 0, explain: "The AM creates one map task per split. Reducers come from `mapreduce.job.reduces` / `setNumReduceTasks()`. With 1 reducer you get one output file; with 2 you get part-0 and part-1.", src: "MR slides 12, 16" },
  { id: "mr9", topic: "mapreduce", q: "With multiple reducers (slide 12, figure 2-4), how does each map task's output reach the reducers?", options: ["Every reducer gets a full copy of all map output", "Map output is partitioned by key, so all values for the same key go to the same reducer", "Each map task sends its output to exactly one reducer", "Records are sent to random reducers"], answer: 1, explain: "Partitioning by key guarantees one reducer sees all values for a key. Each reducer writes its own part file to HDFS.", src: "MR slide 12" },
  { id: "mr10", topic: "mapreduce", q: "Which is NOT one of the five entities involved in running a MapReduce job?", options: ["The YARN ResourceManager", "The MapReduce ApplicationMaster", "The Secondary NameNode", "The shared distributed filesystem"], answer: 2, explain: "The five: client, YARN RM, YARN NMs, MR ApplicationMaster, and the shared FS (HDFS) used to share job files.", src: "MR slide 14" },
  { id: "mr11", topic: "mapreduce", q: "Which does the JobSubmitter NOT do during job submission?", options: ["Ask the RM for a new application ID", "Check the output specification", "Compute the input splits", "Run the map tasks"], answer: 3, explain: "Submission: get app ID, check output spec, compute splits, copy JAR/config/splits to HDFS, `submitApplication()`. Map tasks run later in containers.", src: "MR slide 15" },
  { id: "mr12", topic: "mapreduce", q: "A job is submitted but the output directory already exists. What happens?", options: ["Old files are overwritten", "The job fails at submission when the output spec is checked", "Hadoop writes to a new folder automatically", "Output is appended"], answer: 1, explain: "Step 2 checks the output spec: missing or already-existing output directory means the job is not submitted.", src: "MR slide 15" },
  { id: "mr13", topic: "mapreduce", q: "What is an uber task?", options: ["A task that failed 4 times", "A small job that runs inside the ApplicationMaster's JVM without separate containers", "A reduce task that also combines", "A task run on the NameNode"], answer: 1, explain: "If the job is small enough, distributing it isn't worth the overhead, so it runs locally in the AM.", src: "MR slide 16" },
  { id: "mr14", topic: "mapreduce", q: "Which Java class actually runs a map or reduce task inside its container?", options: ["MRAppMaster", "YarnChild", "JobSubmitter", "NameNode"], answer: 1, explain: "MRAppMaster = the ApplicationMaster. YarnChild localizes resources and runs the task.", src: "MR slides 15-16" },
  { id: "mr15", topic: "mapreduce", q: "The RM gets no heartbeat from a NodeManager for 10 minutes. What happens to map tasks that already finished on that node (for an unfinished job)?", options: ["Nothing: their output is safe in HDFS", "They are rerun, because their intermediate output was on that node's local filesystem", "The whole job is cancelled", "The reducers skip that data"], answer: 1, explain: "Map output lives on the local disk, not HDFS, so reducers may not be able to reach it.", src: "MR slide 20" },
  { id: "mr16", topic: "mapreduce", q: "How is an ApplicationMaster failure recovered?", options: ["The client restarts the whole job", "The RM notices missing heartbeats and starts a new AM in a new container, which recovers finished tasks from the job history", "The NameNode replays the edit log", "It can't be recovered"], answer: 1, explain: "The client times out on its next status poll and asks the RM for the new AM's address. This is transparent to the user.", src: "MR slide 19" },
  { id: "mr17", topic: "mapreduce", q: "How is ResourceManager high availability achieved?", options: ["Two active RMs share the work", "An active-standby pair; a failover controller uses ZooKeeper leader election so only one is active", "The Secondary NameNode takes over", "Each NodeManager can act as RM"], answer: 1, explain: "The new RM reads application state from the state store (HDFS or ZooKeeper) and restarts AMs. Clients try RMs round-robin.", src: "MR slide 21" },
  { id: "mr18", topic: "mapreduce", q: "Which is NOT a problem with parallelizing the awk script on one machine (slide 6)?", options: ["Dividing work into equal-size pieces is hard", "Combining the results needs further processing", "You are limited by one machine's capacity", "awk cannot read text files"], answer: 3, explain: "The three problems: unequal pieces, combining results, single-machine limits (plus coordination and reliability with many machines).", src: "MR slide 6" },
  { id: "mr19", topic: "mapreduce", q: "Where is the best place to drop bad records (missing or suspect temperatures)?", options: ["In the map function", "In the reducer only", "In the driver", "In HDFS"], answer: 0, explain: "Filtering in map means bad records never get shuffled across the network.", src: "MR slide 7" },
  { id: "mr20", topic: "mapreduce", q: "When does the ApplicationMaster blacklist a NodeManager?", options: ["When it stops heartbeating for 1 minute", "When more than three tasks fail on it, even if the node manager itself hasn't failed", "Whenever any task fails", "Never; only the RM blacklists"], answer: 1, explain: "Blacklisting is done by the AM and makes it reschedule tasks on different nodes.", src: "MR slide 20" },

  // streaming
  { id: "st1", topic: "streaming", q: "What does \"streaming\" refer to in Hadoop Streaming?", options: ["Real-time unbounded data streams", "Video streaming", "The Unix standard streams stdin, stdout and stderr", "Streaming reads from HDFS"], answer: 2, explain: "Not to be confused with Spark Streaming or Storm, which do real-time analytics in time windows.", src: "MR slides 22-23" },
  { id: "st2", topic: "streaming", q: "What is the default separator between key and value in Hadoop Streaming output?", options: ["Comma", "Space", "Tab (\\t)", "Colon"], answer: 2, explain: "Tab by default. It can be changed with arguments to the streaming job.", src: "MR slide 24" },
  { id: "st3", topic: "streaming", q: "A mapper writes a line with no tab in it. How does Hadoop Streaming treat it?", options: ["It's dropped", "The whole line is the key and the value is null", "The first word is the key", "The job fails"], answer: 1, explain: "No separator means the output is only a key with a null value.", src: "MR slide 24" },
  { id: "st4", topic: "streaming", q: "Why does a streaming reducer only need to compare each line's key with the previous key?", options: ["Because Python is single threaded", "Because the input arrives sorted, so all lines for one key are consecutive", "Because each reducer gets only one key", "Because the mapper already summed them"], answer: 1, explain: "Shuffle & sort guarantees grouping by key. When the key changes, the previous key is finished.", src: "MR slide 25" },
  { id: "st5", topic: "streaming", q: "In `cat flights.csv | ./mapper.py | sort | ./reducer.py`, what does `sort` simulate?", options: ["The combiner", "The shuffle and sort phase", "HDFS replication", "The driver"], answer: 1, explain: "Sorting the mapper output puts identical keys next to each other, which is what the reducer expects.", src: "MR slide 28" },
  { id: "st6", topic: "streaming", q: "Flight rows are `date, airline, flightnum, origin, dest, dep, dep_delay, arv, arv_delay, airtime, distance`. What does `self.emit(row[3], row[6])` emit?", options: ["(airline, arrival delay)", "(origin airport, departure delay)", "(destination, distance)", "(flight number, departure time)"], answer: 1, explain: "Index 3 = origin, index 6 = dep_delay (0-based). The reducer then averages delay per origin airport.", src: "MR slide 26" },
  { id: "st7", topic: "streaming", q: "Why run `chmod +x mapper.py`?", options: ["To compile the Python", "To make the script executable so it can be launched directly (with the `#!/usr/bin/env python` shebang)", "To upload it to HDFS", "To set the separator"], answer: 1, explain: "Streaming launches the mapper and reducer as executables. The shebang says which interpreter to use.", src: "MR slides 27-28" },
  { id: "st8", topic: "streaming", q: "In the slide 27 reducer, what does `groupby(self, itemgetter(0))` do?", options: ["Sorts the input", "Groups consecutive (key, value) pairs that share the same key", "Removes duplicates", "Sends data to other reducers"], answer: 1, explain: "`itemgetter(0)` picks the key. `groupby` only groups neighbours, which works because the input is already sorted.", src: "MR slide 27" },
  { id: "st9", topic: "streaming", q: "How does Hadoop Streaming run your Python mapper?", options: ["It translates it to Java", "Each map task launches the executable in its own process, pipes input lines to its stdin and collects its stdout", "It runs it once on the NameNode", "It imports it as a module into the JVM"], answer: 1, explain: "That's why the mapper reads `sys.stdin` and writes `sys.stdout`.", src: "MR slide 24" },

  // spark
  { id: "sp1", topic: "spark", review: true, q: "Which feature of Apache Spark makes it particularly suited for iterative machine learning algorithms?", options: ["RDDs are mutable and can be modified during computation", "Spark allows storage of data in HDFS, which speeds up iterative processes", "Spark's in-memory computation enables reuse of datasets without reloading from disk in every iteration", "Spark relies heavily on disk-based computations, making it efficient for large data"], answer: 2, explain: "RDDs are immutable (A is false). HDFS is disk (B, D are the MapReduce way).", src: "Review MCQ 3" },
  { id: "sp2", topic: "spark", review: true, q: "Which statement about Spark's Directed Acyclic Graph (DAG) is correct?", options: ["DAG only represents the data flow between map and reduce tasks in Spark", "DAG is used to break jobs into stages, optimizing the execution plan before any action is taken", "DAG is primarily used to store metadata about data in Spark SQL", "DAGs are specific to machine learning algorithms"], answer: 1, explain: "Because the flow is a DAG, Spark knows the whole computation in advance and plans how to distribute it.", src: "Review MCQ 5, Spark slide 4" },
  { id: "sp3", topic: "spark", review: true, q: "In Spark, what is the purpose of lazy evaluation?", options: ["To execute transformations immediately after they are called", "To delay the execution of transformations until an action is called, optimizing the sequence of operations", "To cache data during each step of the transformation to avoid re-execution", "To reduce the number of worker nodes required to execute a job"], answer: 1, explain: "Spark inspects the full chain of transformations plus the action before submitting a job.", src: "Review MCQ 7, Spark slide 7" },
  { id: "sp4", topic: "spark", review: true, q: "In Apache Spark, which of the following is an action that triggers the execution of a data flow?", options: ["map()", "filter()", "reduce()", "flatMap()"], answer: 2, explain: "map, filter and flatMap are transformations (lazy). reduce returns a value to the driver.", src: "Review MCQ 10" },
  { id: "sp5", topic: "spark", q: "Why are RDDs fault tolerant?", options: ["Every partition is replicated 3 times in memory", "They can be rebuilt from their lineage (the history of transformations)", "They are written to disk after every step", "The driver keeps a full copy"], answer: 1, explain: "RDDs are immutable, so Spark can reapply the recorded transformations to recompute lost partitions.", src: "Spark slide 6" },
  { id: "sp6", topic: "spark", q: "Which BEST describes an RDD?", options: ["A mutable table on one machine", "A read-only collection of objects partitioned across machines, rebuildable from lineage and cacheable in memory", "A file in HDFS", "A SQL view"], answer: 1, explain: "Read-only + partitioned + lineage + cacheable is the full slide 6 definition.", src: "Spark slide 6" },
  { id: "sp7", topic: "spark", q: "How does Spark relate to data storage?", options: ["It replaces HDFS with its own file system", "It focuses on computation and relies on systems like HDFS, S3, HBase or Cassandra for storage", "It stores everything in a SQL database", "It cannot read Hadoop data"], answer: 1, explain: "Spark doesn't deal with distributed storage. It runs on YARN and reads any Hadoop data source.", src: "Spark slides 3, 5-6" },
  { id: "sp8", topic: "spark", q: "In yarn-cluster mode, where does the Spark driver run?", options: ["In the client process", "Inside the YARN ApplicationMaster process", "On the NameNode", "On every executor"], answer: 1, explain: "yarn-cluster frees the client process, like a traditional MapReduce job. yarn-client keeps the driver in the client.", src: "Spark slide 10" },
  { id: "sp9", topic: "spark", q: "You are exploring data interactively in the PySpark shell and want immediate results. Which mode fits?", options: ["yarn-client", "yarn-cluster", "uber mode", "standby mode"], answer: 0, explain: "yarn-client is for interactive work. yarn-cluster is for long-running jobs that don't need the user.", src: "Spark slide 10" },
  { id: "sp10", topic: "spark", q: "On each worker, what manages computation as well as storage and caching?", options: ["The executor", "The NodeManager", "The DataNode", "The SparkContext"], answer: 0, explain: "Executors run tasks and hold cached data. The SparkContext lives in the driver.", src: "Spark slides 10-11" },
  { id: "sp11", topic: "spark", q: "`text` is an RDD of lines. What's the difference between `text.flatMap(tokenize)` and `text.map(tokenize)`?", options: ["None", "flatMap gives an RDD of individual words; map gives an RDD of lists of words (one list per line)", "flatMap is an action", "map removes empty lines"], answer: 1, explain: "flatMap flattens the lists. Word count needs one element per word, so it uses flatMap.", src: "Spark slide 12" },
  { id: "sp12", topic: "spark", q: "In the word count example, when does the distributed job actually start?", options: ["At `sc.textFile(...)`", "At `flatMap(tokenize)`", "At `reduceByKey(add)`", "At `saveAsTextFile(\"wc\")`"], answer: 3, explain: "Everything before is a lazy transformation. saveAsTextFile is the action. (The slide text loosely calls reduceByKey an action, but it returns an RDD.)", src: "Spark slide 13" },
  { id: "sp13", topic: "spark", q: "What does `wc.toDebugString()` show?", options: ["The output data", "The RDD's lineage: how it was derived from earlier RDDs", "Error logs", "The cluster's memory"], answer: 1, explain: "It prints the chain PythonRDD ← MappedRDD ← HadoopRDD.", src: "Spark slide 13" },
  { id: "sp14", topic: "spark", q: "Why does the airline app call `sc.broadcast(airlines)`?", options: ["To save the dict to HDFS", "To ship a read-only lookup dictionary to every node once, so closures can use `airline_lookup.value`", "To print the airlines", "To start the job"], answer: 1, explain: "The lookup dict is collected to the driver, then broadcast to the cluster.", src: "Spark slide 16" },
  { id: "sp15", topic: "spark", q: "Why is MapReduce slow for iterative algorithms?", options: ["Java is slow", "Each iteration is a separate chained job that reads from and writes to disk", "It can't run on clusters", "It has no reduce step"], answer: 1, explain: "Disk I/O between iterations causes significant delay. Spark keeps data in memory.", src: "Spark slide 4" },
  { id: "sp16", topic: "spark", q: "Which of these is a transformation?", options: ["count()", "collect()", "filter()", "saveAsTextFile()"], answer: 2, explain: "filter returns a new (smaller) RDD. The other three are actions.", src: "Spark slide 7" },
  { id: "sp17", topic: "spark", q: "Which library in the Spark stack provides scalable classification and regression?", options: ["GraphX", "MLlib", "Spark SQL", "YARN"], answer: 1, explain: "MLlib = machine learning on RDDs. GraphX = graphs. Spark SQL = relational queries.", src: "Spark slide 5" },
  { id: "sp18", topic: "spark", q: "Why should the Spark driver be on the same network as the cluster?", options: ["It stores the HDFS blocks", "Executors communicate back and forth with the driver during the job", "YARN requires it", "To use less memory"], answer: 1, explain: "Unlike Hadoop, where you can submit from anywhere to the RM, the Spark driver is an active participant.", src: "Spark slide 10" },
  { id: "sp19", topic: "spark", q: "In the airline app, where does `sorted(delays, key=itemgetter(1))` run?", options: ["On the executors", "On the driver, after `collect()` brought the results back", "On the NameNode", "On the YARN ResourceManager"], answer: 1, explain: "`collect()` returns a Python list, and plain Python then runs locally in the driver.", src: "Spark slide 16" },

  // sparksql
  { id: "sq1", topic: "sparksql", q: "Which BEST describes a Spark DataFrame?", options: ["A pandas DataFrame on one machine", "A distributed collection with a defined schema that wraps an RDD (an RDD of Row objects)", "A SQL database", "An RDD without partitions"], answer: 1, explain: "Tabular like pandas or R, but distributed, and it wraps an RDD.", src: "Spark slide 22" },
  { id: "sq2", topic: "sparksql", q: "According to the benchmark on slide 22, DataFrame code compared to RDD code was:", options: ["About the same speed", "Up to 4-5× faster, with similar speed in Python, Scala and SQL", "Slower in Python", "Only faster in Scala"], answer: 1, explain: "DataFrame optimizations compile the query plan. Python RDD was the slowest bar in the chart.", src: "Spark slide 22" },
  { id: "sq3", topic: "sparksql", q: "Before `sqlContext.sql(\"SELECT ... FROM parking ...\")` can work, what must you do?", options: ["Save parking to HDFS", "Register the DataFrame as a temporary table: `parking.registerTempTable(\"parking\")`", "Convert it to an RDD", "Call collect()"], answer: 1, explain: "SQL refers to tables by name, so you register the DataFrame first (`createOrReplaceTempView` in newer Spark).", src: "Spark slide 20" },
  { id: "sq4", topic: "sparksql", q: "What is the difference between WHERE and HAVING?", options: ["No difference", "WHERE filters rows before grouping; HAVING filters groups after GROUP BY (and can use aggregates)", "HAVING runs before WHERE", "WHERE only works with numbers"], answer: 1, explain: "Slide 21 uses `HAVING trim(primetype) != ''`. The DataFrame version on slide 23 uses `.where(...)` before `groupBy`.", src: "Spark slides 21, 23" },
  { id: "sq5", topic: "sparksql", q: "In `SELECT primetype, secondtype, count(1) AS count ... GROUP BY primetype, secondtype`, what does `count(1)` compute?", options: ["Always 1", "The number of rows in each (primetype, secondtype) group", "The number of columns", "The first row"], answer: 1, explain: "Counts records per unique combination of the grouping columns.", src: "Spark slide 21" },
  { id: "sq6", topic: "sparksql", q: "What does `sc.wholeTextFiles(input_path)` return?", options: ["An RDD of lines", "An RDD of (filename, content) pairs, one per file", "A DataFrame", "A Python list"], answer: 1, explain: "Each file is read as one whole string. `.map(lambda (x, y): y)` keeps just the content.", src: "Spark slide 20" },
  { id: "sq7", topic: "sparksql", q: "Why `flatMap(lambda x: json.loads(x))` instead of map?", options: ["flatMap is faster", "One file's JSON can hold many records; flatMap flattens them into one record per element", "json.loads needs flatMap", "map can't call functions"], answer: 1, explain: "json.loads may return a list of records. flatMap flattens it into the RDD.", src: "Spark slide 20" },
  { id: "sq8", topic: "sparksql", q: "According to the slides, the main advantage of the DataFrame API over raw SQL strings is:", options: ["It's the only way to group data", "You can build a complex query by chaining and testing operations step by step, with many built-in functions", "It doesn't need Spark", "It runs on the driver only"], answer: 1, explain: "Both run on the same optimized engine. The DataFrame API makes iterating on complex queries easier.", src: "Spark slide 23" },
  { id: "sq9", topic: "sparksql", q: "Which DataFrame call matches `ORDER BY count DESC`?", options: [".sort(\"count\")", ".sort(\"count\", ascending=False)", ".groupBy(\"count\")", ".where(\"count DESC\")"], answer: 1, explain: "`ascending=False` gives descending order. `.orderBy(F.desc(\"count\"))` works too.", src: "Spark slide 23" },
  { id: "sq10", topic: "sparksql", q: "What does `spark.read.csv(path, header=True, inferSchema=True)` do with `inferSchema=True`?", options: ["Uses the first row as column names", "Detects each column's type (double, date…) instead of reading everything as strings", "Drops nulls", "Sorts the data"], answer: 1, explain: "`header=True` uses the first row as names. Without inferSchema every column is a string (see the lab's printSchema).", src: "Lab: EDA Weather" },
  { id: "sq11", topic: "sparksql", q: "What does `df.na.drop(subset=[\"AverageTemperature\", \"AverageTemperatureUncertainty\"])` do?", options: ["Drops those two columns", "Drops every row that has a null in either of those columns", "Fills nulls with 0", "Drops duplicate rows"], answer: 1, explain: "\"You drop the entire record if any of these columns have NA.\"", src: "Lab: EDA Weather" },
  { id: "sq12", topic: "sparksql", q: "Which gives the average temperature per year, newest year first?", options: ["df.groupBy(\"Year\").avg(\"AverageTemperature\").orderBy(df[\"Year\"].desc())", "df.avg(\"AverageTemperature\").groupBy(\"Year\")", "df.orderBy(\"Year\").avg(\"AverageTemperature\")", "df.select(\"Year\").avg()"], answer: 0, explain: "Group first, aggregate, then sort the grouped result. This is exactly the lab code.", src: "Lab: EDA Weather" }
];

/* ---------------- True / False ---------------- */
CP.tf = [
  { id: "tbd1", topic: "bigdata", q: "Big data can be summarized as data that is too big to load and process on a single PC.", answer: true, explain: "The professor's own summary (Sept 16). Volume is constrained by a single PC's processing power.", src: "Notes, Intro slide 6" },
  { id: "tbd2", topic: "bigdata", q: "Text documents, PDFs and images are examples of structured data.", answer: false, explain: "Unstructured: no inherent structure, stored as different file types.", src: "Intro slide 11" },
  { id: "tbd3", topic: "bigdata", q: "Most (80-90%) of future data growth is expected to come from non-structured data types.", answer: true, explain: "Semi-, quasi- and unstructured data drive the growth.", src: "Intro slide 11 notes" },
  { id: "tbd4", topic: "bigdata", q: "Descriptive analytics answers \"what will happen next?\"", answer: false, explain: "That's predictive. Descriptive = what happened (reports, queries, alerts).", src: "Intro slide 21" },
  { id: "tbd5", topic: "bigdata", q: "Big data only requires programming skills.", answer: false, explain: "It requires new architectures, new analytical methods, and integrating multiple skills: business, domain, analytics, programming and soft skills.", src: "Intro slides 7, 27-28" },
  { id: "tbd6", topic: "bigdata", q: "To be confident a deployed model will work, you should test it rigorously and actively try to break it.", answer: true, explain: "Discussed at length in class on Sept 21 (the dashboard handed to a team lead).", src: "Notes Sept 21" },
  { id: "tbd7", topic: "bigdata", q: "Value, as one of the V's, means the data has some positive utility rather than being random.", answer: true, explain: "Value: has some positive value (utility).", src: "Intro slide 6" },

  { id: "thd1", topic: "hadoop", q: "The Secondary NameNode is a hot backup that takes over immediately when the NameNode fails.", answer: false, explain: "Classic trap. It does checkpointing and housekeeping and is NOT a backup NameNode.", src: "Hadoop slide 17" },
  { id: "thd2", topic: "hadoop", q: "The NameNode passes file data from the DataNodes to the client.", answer: false, explain: "It only points clients to the right DataNodes (traffic cop). Clients read blocks directly from DataNodes.", src: "Hadoop slide 17" },
  { id: "thd3", topic: "hadoop", q: "HDFS achieves fault tolerance by replicating each block on multiple nodes.", answer: true, explain: "Redundant copies (3× by default) mean another copy exists if a node fails.", src: "Hadoop slides 7, 12; Review Q4" },
  { id: "thd4", topic: "hadoop", q: "Hadoop requires expensive specialized hardware, like a data warehouse.", answer: false, explain: "Hadoop runs on economical commodity (off-the-shelf) hardware, unlike data warehouses.", src: "Hadoop slides 11, 15" },
  { id: "thd5", topic: "hadoop", q: "YARN manages the data stored on disks across the cluster.", answer: false, explain: "That's HDFS. YARN allocates computational resources (CPU, memory) to applications.", src: "Hadoop slide 14" },
  { id: "thd6", topic: "hadoop", q: "Hadoop handles unstructured data well because it uses schema-on-write.", answer: false, explain: "Schema-on-read: data is interpreted at processing time.", src: "Hadoop slide 8" },
  { id: "thd7", topic: "hadoop", q: "A Hadoop cluster can consist of a single node.", answer: true, explain: "A cluster can have a single node or many thousands.", src: "Hadoop slide 15" },
  { id: "thd8", topic: "hadoop", q: "DataNodes report the health and status of their data stores back to the NameNode.", answer: true, explain: "DataNode (worker) stores blocks and reports to the NameNode.", src: "Hadoop slide 17" },
  { id: "thd9", topic: "hadoop", q: "Worker nodes are the minority of machines in a Hadoop cluster.", answer: false, explain: "Workers are the majority. Masters are few.", src: "Hadoop slide 16" },
  { id: "thd10", topic: "hadoop", q: "For a scalable system, adding load should cause a decline in performance, not a failure.", answer: true, explain: "And increasing resources should give a proportional increase in capacity.", src: "Hadoop slide 10" },
  { id: "thd11", topic: "hadoop", q: "The ResourceManager runs and monitors the tasks inside containers on each worker node.", answer: false, explain: "That's the NodeManager. The RM allocates resources and schedules across the whole cluster.", src: "Hadoop slide 18" },
  { id: "thd12", topic: "hadoop", q: "Each Hadoop daemon runs inside its own Java Virtual Machine.", answer: true, explain: "So each daemon has its own resource allocation and is managed independently by the OS.", src: "Hadoop slide 15" },
  { id: "thd13", topic: "hadoop", q: "In Hadoop, data is distributed across the cluster as soon as it is added, and stored on multiple nodes.", answer: true, explain: "Data is distributed immediately; nodes prefer to process local data.", src: "Hadoop slide 10" },

  { id: "tmr1", topic: "mapreduce", q: "Each MapReduce phase has key-value pairs as both input and output.", answer: true, explain: "The programmer chooses the types and writes the map and reduce functions.", src: "MR slide 7" },
  { id: "tmr2", topic: "mapreduce", q: "A combiner's output becomes the input to the reduce function.", answer: true, explain: "It runs on map output to shrink what gets sent to the reducers.", src: "MR slide 13" },
  { id: "tmr3", topic: "mapreduce", q: "You can always reuse the reducer as a combiner, whatever the reducer computes.", answer: false, explain: "Only for operations like max, min, sum, count. Average and median give wrong answers.", src: "MR slide 13" },
  { id: "tmr4", topic: "mapreduce", q: "The number of reduce tasks always equals the number of input splits.", answer: false, explain: "Map tasks = splits. Reduce tasks are set by `mapreduce.job.reduces` / `setNumReduceTasks()`.", src: "MR slides 12, 16" },
  { id: "tmr5", topic: "mapreduce", q: "The ApplicationMaster creates one map task for each input split.", answer: true, explain: "Step 7: MRAppMaster retrieves the splits and creates a map task object per split.", src: "MR slide 16" },
  { id: "tmr6", topic: "mapreduce", q: "If the output directory already exists, the MapReduce job is not submitted.", answer: true, explain: "The JobSubmitter checks the output specification in step 2.", src: "MR slide 15" },
  { id: "tmr7", topic: "mapreduce", q: "The job JAR, configuration and input splits are copied to the shared filesystem in a directory named after the job ID.", answer: true, explain: "Step 3 of job submission.", src: "MR slide 15" },
  { id: "tmr8", topic: "mapreduce", q: "The main class of the MapReduce ApplicationMaster is YarnChild.", answer: false, explain: "The AM's main class is MRAppMaster. YarnChild runs individual tasks.", src: "MR slides 15-16" },
  { id: "tmr9", topic: "mapreduce", q: "When a task attempt fails, the AM prefers to reschedule it on the same node manager.", answer: false, explain: "It tries to avoid the node manager where the task previously failed.", src: "MR slide 18" },
  { id: "tmr10", topic: "mapreduce", q: "By default, a task that fails four times will not be retried again.", answer: true, explain: "4 attempts, configurable.", src: "MR slide 18" },
  { id: "tmr11", topic: "mapreduce", q: "In the default configuration, the YARN ResourceManager is a single point of failure.", answer: true, explain: "Without HA, if its machine fails all running jobs fail and can't be recovered.", src: "MR slide 21" },
  { id: "tmr12", topic: "mapreduce", q: "Completed map tasks on a failed NodeManager never need rerunning, because map output is stored in HDFS.", answer: false, explain: "Map output is on the node's local filesystem, so tasks of incomplete jobs are rerun.", src: "MR slide 20" },
  { id: "tmr13", topic: "mapreduce", q: "With ResourceManager high availability, both resource managers are active at the same time.", answer: false, explain: "Active-standby. ZooKeeper leader election ensures only one is active.", src: "MR slide 21" },
  { id: "tmr14", topic: "mapreduce", q: "If the ApplicationMaster fails, every completed task must be rerun from scratch.", answer: false, explain: "The new AM uses the job history to recover the state of tasks that already ran.", src: "MR slide 19" },
  { id: "tmr15", topic: "mapreduce", q: "In the max temperature mapper, the input key (the line's byte offset) is ignored.", answer: true, explain: "\"We have no need for this, so we ignore it.\"", src: "MR slides 7-8" },
  { id: "tmr16", topic: "mapreduce", q: "MapReduce is best described as offline (batch) analytics, while Spark is in-memory.", answer: true, explain: "The professor's own contrast in the review class.", src: "Notes Oct 21" },
  { id: "tmr17", topic: "mapreduce", q: "The RM removes a NodeManager from its pool if it hasn't received a heartbeat from it for 10 minutes.", answer: true, explain: "Then it stops scheduling containers there.", src: "MR slide 20" },
  { id: "tmr18", topic: "mapreduce", q: "With two reducers, all values for the same key can be split between both reducers.", answer: false, explain: "Map output is partitioned by key, so one key always goes to one reducer.", src: "MR slide 12" },

  { id: "tst1", topic: "streaming", q: "Hadoop Streaming is used for real-time processing of unbounded data streams, like Spark Streaming.", answer: false, explain: "Streaming refers to the Unix standard streams (stdin/stdout/stderr). It's batch MapReduce.", src: "MR slide 23" },
  { id: "tst2", topic: "streaming", q: "Hadoop Streaming mappers and reducers read input from stdin and write output to stdout.", answer: true, explain: "Python accesses these via the sys module.", src: "MR slide 22" },
  { id: "tst3", topic: "streaming", q: "The default key/value separator in Hadoop Streaming is a comma.", answer: false, explain: "Tab (\\t) by default.", src: "MR slide 24" },
  { id: "tst4", topic: "streaming", q: "Hadoop Streaming lets you write mappers and reducers in languages other than Java, such as Python.", answer: true, explain: "Any executable that reads stdin and writes stdout.", src: "MR slides 22-24" },
  { id: "tst5", topic: "streaming", q: "The reducer on slide 25, exactly as written, prints the total for every key including the last one.", answer: false, explain: "It only writes when the key changes, so the last key is never emitted. You need one more write after the loop.", src: "MR slide 25" },
  { id: "tst6", topic: "streaming", q: "In Hadoop Streaming, the reducer receives each key once with a list of all its values.", answer: false, explain: "It receives sorted `key\\tvalue` lines one at a time. Grouping into lists is the Java API's behaviour.", src: "MR slide 25" },

  { id: "tsp1", topic: "spark", q: "RDDs are immutable: they cannot be modified directly.", answer: true, explain: "Transformations create new RDDs. Immutability is what makes lineage-based recovery possible.", src: "Spark slide 6" },
  { id: "tsp2", topic: "spark", q: "`map()` is an action.", answer: false, explain: "map is a transformation that returns a new RDD.", src: "Spark slide 7" },
  { id: "tsp3", topic: "spark", q: "Spark transformations execute immediately when they are called.", answer: false, explain: "They are lazy, and run only when an action is invoked.", src: "Spark slide 7" },
  { id: "tsp4", topic: "spark", q: "Spark can run on an existing Hadoop cluster through YARN and read data from HDFS.", answer: true, explain: "It can access any Hadoop data source: HDFS, S3, HBase, Cassandra.", src: "Spark slide 3" },
  { id: "tsp5", topic: "spark", q: "Spark provides its own distributed storage layer that replaces HDFS.", answer: false, explain: "Spark focuses on computation and relies on Hadoop for storage.", src: "Spark slides 5-6" },
  { id: "tsp6", topic: "spark", q: "Spark recovers lost partitions by recomputing them from lineage.", answer: true, explain: "Without restarting the entire job.", src: "Spark slide 6; Review Q17" },
  { id: "tsp7", topic: "spark", q: "In yarn-cluster mode, the driver runs in the client process.", answer: false, explain: "That's yarn-client. In yarn-cluster the driver runs inside the ApplicationMaster.", src: "Spark slide 10" },
  { id: "tsp8", topic: "spark", q: "Caching is especially beneficial for iterative algorithms that reuse the same dataset.", answer: true, explain: "Cached RDDs stay in worker memory between iterations.", src: "Spark slide 9; Review Q22" },
  { id: "tsp9", topic: "spark", q: "Spark's API only supports map and reduce operations.", answer: false, explain: "It includes many more distributed operations: sample, filter, join, collect, flatMap…", src: "Spark slide 3" },
  { id: "tsp10", topic: "spark", q: "`collect()` returns all elements of an RDD to the driver program.", answer: true, explain: "It's an action. Use it only on small results.", src: "Spark slides 9, 16" },
  { id: "tsp11", topic: "spark", q: "The SparkContext in the driver program coordinates a Spark application's processes.", answer: true, explain: "It connects to a cluster manager (e.g. YARN), which allocates resources to executors.", src: "Spark slide 10" },
  { id: "tsp12", topic: "spark", q: "Using `flatMap` with a function that splits lines produces an RDD whose elements are lists of words.", answer: false, explain: "flatMap flattens, giving one element per word. map would give lists.", src: "Spark slide 12" },

  { id: "tsq1", topic: "sparksql", q: "A Spark DataFrame is the same as a pandas DataFrame stored on one machine.", answer: false, explain: "A Spark DataFrame is distributed and wraps an RDD of Row objects.", src: "Spark slide 22" },
  { id: "tsq2", topic: "sparksql", q: "DataFrame operations were found to be up to 4-5 times faster than equivalent hand-coded RDD operations.", answer: true, explain: "Benchmark aggregating 10 million integer pairs.", src: "Spark slide 22" },
  { id: "tsq3", topic: "sparksql", q: "HAVING filters rows before they are grouped.", answer: false, explain: "WHERE filters rows before grouping. HAVING filters groups after GROUP BY.", src: "Spark slide 21" },
  { id: "tsq4", topic: "sparksql", q: "To query a DataFrame with SQL, you must first register it as a temporary table (or view).", answer: true, explain: "`parking.registerTempTable(\"parking\")`, then `sqlContext.sql(\"... FROM parking ...\")`.", src: "Spark slide 20" },
  { id: "tsq5", topic: "sparksql", q: "`ORDER BY count DESC` lists the groups with the highest counts first.", answer: true, explain: "DESC = descending.", src: "Spark slide 21" },
  { id: "tsq6", topic: "sparksql", q: "`df.count()` is a transformation.", answer: false, explain: "count returns a number to the driver, so it's an action.", src: "Lab, Spark slide 9" },
  { id: "tsq7", topic: "sparksql", q: "Spark SQL can be accessed through JDBC/ODBC connectors, a Hive console, or its built-in APIs.", answer: true, explain: "The built-in API is the most powerful because it shares the RDD programming interface.", src: "Spark slide 19" }
];

/* ---------------- Short answer (compare / explain why) ---------------- */
CP.short = [
  { id: "sa-yarn-roles", topic: "hadoop", marks: 10, q: "Explain the roles of the ResourceManager, the NodeManager and the ApplicationMaster in YARN. How do they work together when a client submits a job?",
    points: ["RM is the master (one per cluster): allocates and monitors cluster resources (memory, cores) and schedules jobs", "NM is the worker (one per node): creates containers, runs and manages tasks, reports health and status", "AM is one per application: coordinates that job, negotiates resources with the RM, tracks task progress", "Flow: client asks the RM, the RM assigns an AM, the AM requests containers, the NMs launch them and run tasks", "RM tracks node status; AM tracks job/task status (heartbeats)"],
    model: "The ResourceManager is YARN's master daemon, one per cluster. It allocates and monitors the cluster's resources (memory and processor cores) and schedules jobs. The NodeManager is the worker daemon on every node. It creates containers, runs and manages the processing tasks inside them, and reports the node's health and task status. The ApplicationMaster is created per application: it coordinates one job for its whole duration, negotiates containers from the ResourceManager and tracks its tasks. When a client submits a job, it asks the ResourceManager for resources; the RM assigns an application-specific ApplicationMaster (launched in a container by a NodeManager); the AM requests containers for the map and reduce tasks; the NodeManagers start those containers and execute the tasks. The RM keeps track of the nodes, while the AM keeps track of the job." },
  { id: "sa-mr-vs-spark", topic: "spark", marks: 10, q: "Compare Hadoop MapReduce and Apache Spark. Explain why Spark is faster for iterative algorithms such as machine learning.",
    points: ["MapReduce writes intermediate results to disk; iterative work = chained jobs that re-read from disk", "Spark keeps/caches data in memory (RDDs) across iterations", "Spark builds a DAG and evaluates lazily, so it optimizes the plan before running", "Spark has a richer API (filter, join, sample…) than just map and reduce", "Fault tolerance: MapReduce re-runs tasks + HDFS replication; Spark recomputes partitions from lineage", "MapReduce = offline batch analytics; Spark = in-memory, interactive and iterative"],
    model: "Both are distributed processing frameworks that run on Hadoop clusters, but MapReduce is disk-based offline batch processing while Spark is in-memory. An iterative algorithm in MapReduce has to be written as a chain of jobs, and every job reads its input from disk and writes its output back, which causes heavy I/O delay on every iteration. Spark keeps the dataset cached in the memory of the worker nodes as RDDs, so each iteration reuses it without reloading from disk. Spark also describes the whole data flow as a DAG and evaluates transformations lazily, so it can optimize the execution plan before any action runs, and it offers many operations beyond map and reduce. For fault tolerance, MapReduce relies on re-running failed tasks and HDFS replication, while Spark rebuilds lost partitions from their lineage." },
  { id: "sa-hadoop-vs-rdbms", topic: "hadoop", marks: 10, q: "Why can't we just use a relational database with lots of disks for large-scale analysis? Explain why Hadoop is needed.",
    points: ["Seek time: random seeks are slow; DB moves data to code, Hadoop moves code to data with streaming reads", "Rigid schema: Hadoop is schema-on-read, handles semi/unstructured data, loading is just a file copy", "Normal forms: normalization makes record reads non-local; Hadoop assumes streaming reads/writes", "Data locality saves network bandwidth", "Hadoop gives fault tolerance via replication and scales horizontally on commodity hardware"],
    model: "Three reasons from the slides. First, disk seek time: databases rely on seeking to records, and seek latency grows much slower than transfer bandwidth. A database moves data to the code, while Hadoop moves the code to the data and streams through large blocks. Second, rigid schema: an RDBMS needs data to fit a predefined schema and has a costly loading phase. Hadoop is schema-on-read, interpreting data at processing time, so semi-structured and unstructured data are fine and loading is just a file copy. Third, normal forms: normalization keeps integrity but makes reading one record a non-local operation, while Hadoop assumes high-speed streaming reads and writes. On top of that, Hadoop replicates blocks for fault tolerance and scales horizontally on commodity hardware." },
  { id: "sa-combiner", topic: "mapreduce", marks: 10, q: "What is a combiner? Why can the max temperature reducer be used as a combiner, but an average temperature reducer cannot?",
    points: ["Combiner runs on each map task's output before the shuffle; its output feeds the reducer", "Purpose: reduce data transferred between map and reduce (network bandwidth)", "Max works because max of partial maxes = overall max (commutative and associative)", "Average fails because the mean of means ≠ overall mean when groups differ in size (give a numeric example)", "Fix for average: combine (sum, count) pairs and divide only in the reducer"],
    model: "A combiner is an optional function that Hadoop runs on the output of each map task, locally, before the shuffle. Its output becomes the input of the reduce function. Many MapReduce jobs are limited by cluster bandwidth, so pre-aggregating on the map side reduces the data sent over the network. The max reducer is safe as a combiner because the maximum of partial maximums equals the overall maximum: map 1 gives max(0, 20, 10) = 20, map 2 gives max(25, 15) = 25, and max(20, 25) = 25, which is the same as max(0, 20, 10, 25, 15). An average is not safe: mean(0, 20, 10) = 10 and mean(25, 15) = 20, and mean(10, 20) = 15, but the true mean of all five values is 14. To use a combiner for an average, emit (sum, count) pairs, add them in the combiner, and divide only in the reducer." },
  { id: "sa-median", topic: "mapreduce", marks: 10, q: "You must compute the median of a large vector of numbers (one number per line) with MapReduce. What goes in the map function, what goes in the reduce function, and should you use a combiner? Explain why.",
    points: ["Map: parse each line and emit (constant key, number) so all values meet at one reducer", "Shuffle sorts and groups all values under that key", "Reduce: collect values into a list, sort, take the middle (average the two middle values if the count is even)", "No combiner: the median of medians is not the median", "Note: one reducer gets all the data (bottleneck); fine for the exam's logic"],
    model: "The median needs to see every value in order, so the map function only prepares the data: it parses each line into a number and emits it with the same constant key, for example (\"median\", 17). That way the shuffle sends every value to the same reducer. The reduce function receives (\"median\", [all values]); it copies the values into a list, sorts it, and returns the middle element, or the average of the two middle elements when the count is even. A combiner should NOT be used: the median of each map's local median is not the median of the whole vector. For example the medians of [1, 2, 3] and [4, 100] are 2 and 52, but the true median of [1, 2, 3, 4, 100] is 3. The downside is that one reducer handles all the data, but the logic is correct." },
  { id: "sa-trans-actions", topic: "spark", marks: 10, q: "Explain the difference between transformations and actions in Spark, and explain lazy evaluation and why it is beneficial. Give examples.",
    points: ["Transformations create a new RDD from an existing one (map, flatMap, filter, reduceByKey)", "Actions return a result to the driver or write to storage (reduce, collect, count, saveAsTextFile)", "Lazy: transformations are only recorded (lineage/DAG); nothing runs until an action", "Benefit: Spark sees the whole chain and optimizes it (stages, less storage and computation)", "Example: in word count, the job starts at saveAsTextFile"],
    model: "A transformation creates a new RDD from an existing one without returning anything to the driver, for example map, flatMap, filter or reduceByKey. An action returns a result to the driver program or writes data out, which forces the cluster to coordinate across all partitions: reduce, collect, count, take, saveAsTextFile. Spark evaluates transformations lazily. Calling them only records them in the RDD's lineage, a DAG, and nothing executes until an action is invoked. Because Spark sees the complete sequence of transformations plus the action before submitting a job, it can plan stages and optimize storage and computation, for example pipelining map and filter together without materializing intermediate data. In word count, textFile, flatMap, map and reduceByKey just build the plan; saveAsTextFile kicks off the job." },
  { id: "sa-fault-tolerance", topic: "spark", marks: 10, q: "Compare how HDFS and Spark achieve fault tolerance.",
    points: ["HDFS: files are split into blocks, each replicated on multiple DataNodes (3×)", "If a node fails, another replica is used; no data lost", "Spark: RDDs are immutable and remember their lineage", "Lost partitions are recomputed from lineage, not stored twice", "MapReduce tasks: failed task attempts are retried on other nodes (up to 4)"],
    model: "HDFS achieves fault tolerance through replication: every file is divided into large blocks, and each block is stored on several DataNodes (three by default). If a node fails, the data is read from another replica, so nothing is lost. Spark achieves fault tolerance for computation through lineage: RDDs are immutable, and Spark records the history of transformations that produced each one. If a partition is lost because a worker fails, Spark reapplies those transformations to the source data to recompute just that partition, without restarting the whole job and without keeping extra copies in memory. MapReduce itself adds task-level fault tolerance by re-running failed task attempts on other nodes." },
  { id: "sa-hdfs-daemons", topic: "hadoop", marks: 10, q: "Describe the NameNode, Secondary NameNode and DataNode, and explain the steps a client follows to read a file from HDFS.",
    points: ["NameNode (master): directory tree, metadata, block locations", "Secondary NameNode (master): checkpointing/housekeeping, NOT a backup", "DataNode (worker): stores blocks on local disk, reports health to the NameNode", "Read: client asks NameNode, gets the list of DataNodes, reads blocks directly from DataNodes", "NameNode never stores or passes the data (traffic cop)"],
    model: "The NameNode is the HDFS master. It stores the file system's directory tree, the metadata of each file, and the locations of each block in the cluster. The Secondary NameNode is also a master service, but it only performs housekeeping and checkpointing for the NameNode; despite its name it is not a backup. DataNodes are the workers: they store and manage HDFS blocks on their local disks and report the health of their data back to the NameNode. To read a file, the client first asks the NameNode where the data is. The NameNode replies with the list of DataNodes holding each block, and the client then requests each block directly from those DataNodes. The NameNode never stores or relays the data itself; it acts like a traffic cop." },
  { id: "sa-failures", topic: "mapreduce", marks: 10, q: "Explain how Hadoop handles (a) a task failure and (b) a NodeManager failure during a MapReduce job.",
    points: ["Task: JVM reports error to the AM; attempt marked failed; container freed", "AM reschedules the task, avoiding the node where it failed", "After 4 failed attempts the task is not retried (job fails)", "NM: stops heartbeating; RM removes it after 10 minutes", "Completed map tasks of incomplete jobs on that NM are rerun (output on local disk); AM blacklists NMs with >3 task failures"],
    model: "(a) The most common task failure is user code throwing a runtime exception. The task's JVM reports the error to its ApplicationMaster before exiting, the AM marks the attempt as failed and frees the container, then reschedules the task, trying to avoid the node manager where it failed. A task that fails four times (configurable) is not retried again, and the job fails. (b) A node manager that crashes or runs very slowly stops sending heartbeats. If the ResourceManager hears nothing for 10 minutes, it removes the node from its pool. Map tasks that had completed on that node are rerun if their job is still incomplete, because their intermediate output lived on that node's local filesystem and reducers may not reach it. The AM can also blacklist a node manager where more than three tasks failed." },
  { id: "sa-locality", topic: "hadoop", marks: 10, q: "What is data locality and why is it important in Hadoop?",
    points: ["Move computation to the node where the data lives instead of moving data", "Network bandwidth is the most precious resource in a data center", "Local reads are fast; heart of Hadoop's performance", "HDFS + YARN cooperate: tasks are scheduled on nodes holding the blocks", "Contrast with databases (move data to code)"],
    model: "Data locality means Hadoop co-locates computation with the data: instead of copying data across the network to where the program runs, it schedules each task on (or near) the node that already stores that block in HDFS. It matters because network bandwidth is the most precious resource in a data center and is easy to saturate by moving large datasets around, while local disk reads are fast. HDFS and YARN work together so that tasks run where their blocks live, and Hadoop models the network topology to keep traffic low. This is the opposite of a database, which moves data to the code, and it is the reason for Hadoop's good performance." },
  { id: "sa-yarn-modes", topic: "spark", marks: 10, q: "Explain the difference between running Spark in yarn-client mode and yarn-cluster mode, and when you would use each.",
    points: ["yarn-client: driver runs in the client process", "In yarn-client, the AM only manages progress and requests resources", "yarn-cluster: driver runs inside the ApplicationMaster; client is released", "Use client for interactive work and immediate results", "Use cluster for long-running jobs needing no user intervention"],
    model: "In yarn-client mode, the Spark driver program runs inside the client process that submitted the application, and the YARN ApplicationMaster only manages the job's progress and requests resources. Results come straight back to the user, so this mode suits interactive work like a PySpark shell. In yarn-cluster mode the driver runs inside the ApplicationMaster process on the cluster, which releases the client process and behaves more like a traditional MapReduce job. You use it for long-running jobs, or ones that don't need user intervention." },
  { id: "sa-rdd-df", topic: "sparksql", marks: 10, q: "Compare RDDs, DataFrames and Spark SQL. Why are DataFrames usually faster than RDD code?",
    points: ["RDD: low-level, immutable, partitioned objects, functional ops (map, filter, reduceByKey), no schema", "DataFrame: tabular with schema; distributed; wraps an RDD of Row objects", "Spark SQL: query registered tables with SQL strings; same engine as DataFrames", "DataFrame/SQL queries are optimized (query plan compiled), up to 4-5× faster; similar across languages", "DataFrame API lets you chain and test steps; SQL is familiar"],
    model: "An RDD is Spark's low-level abstraction: an immutable, partitioned collection of arbitrary objects that you process with functional operations such as map, filter and reduceByKey; Spark doesn't know the structure of the data. A DataFrame is a distributed table with a defined schema. Under the hood it wraps an RDD of Row objects, but because Spark knows the columns and types, DataFrame operations go through an optimizer that compiles the query plan into efficient code. Spark SQL lets you run SQL statements against DataFrames registered as tables, on the same engine. In the benchmark, DataFrame code was up to 4-5× faster than hand-written RDD code and ran at the same speed from Python, Scala or SQL, while Python RDD code was slowest. The DataFrame API is handy for building a complex query step by step; SQL is familiar and concise." },
  { id: "sa-data-types", topic: "bigdata", marks: 10, q: "Describe the four types of data structures in big data, with an example of each, and explain why the growth of non-structured data matters for big data tools.",
    points: ["Structured: defined type/format (transaction data, OLAP, tables)", "Semi-structured: discernable pattern, parseable (XML with schema)", "Quasi-structured: erratic formats, needs effort to format (clickstream)", "Unstructured: no inherent structure (text docs, PDFs, images, video)", "80-90% of growth is non-structured → needs schema-on-read tools like Hadoop instead of rigid RDBMS"],
    model: "Structured data has a defined data type, format and structure, such as transaction data, OLAP data or relational tables. Semi-structured data is textual with a discernable pattern that allows parsing, such as an XML file defined by an XML schema. Quasi-structured data is textual with erratic formats that can be cleaned up with effort, tools and time, such as web clickstream data with inconsistent values. Unstructured data has no inherent structure and is stored as different kinds of files: text documents, PDFs, images, video. Most future data growth (80-90%) is in the non-structured types. Traditional relational systems need a rigid schema, so we need tools like Hadoop that store any file and interpret it at processing time (schema-on-read)." },
  { id: "sa-requirements", topic: "hadoop", marks: 10, q: "List the four requirements of a distributed system for big data and explain how Hadoop addresses them.",
    points: ["Fault tolerance: degrade gracefully; recovered components rejoin", "Recoverability: no data lost", "Consistency: a failed task doesn't change the result", "Scalability: more load slows down without failing; more resources = proportional capacity", "Hadoop: block replication, independent tasks with redundancy/retries, data locality, master/worker parallelism, horizontal scaling"],
    model: "Fault tolerance: a failed component must not fail the whole system, which should degrade gracefully, and a recovered component should rejoin. Recoverability: no data is lost when something fails. Consistency: the failure of one job or task does not change the final result. Scalability: adding load lowers performance instead of causing failure, and adding resources gives a proportional increase in capacity. Hadoop addresses these by distributing data as soon as it is added and replicating fixed-size blocks across nodes (recoverability, fault tolerance); by splitting jobs into independent tasks, one per block, that don't communicate (no deadlock) and are re-run on failure (consistency); by processing data locally to minimize network traffic; and by having masters hand out work to many workers in parallel, so the cluster scales horizontally on commodity hardware." },
  { id: "sa-bi-ds", topic: "bigdata", marks: 10, q: "Explain the differences between Business Intelligence and data science using descriptive, predictive and prescriptive analytics.",
    points: ["Descriptive: what happened/what's the problem: reports, queries, alerts (BI)", "Predictive: why is this happening/what next: statistical and predictive modeling", "Prescriptive: what if/best scenario: optimization, randomized testing", "BI looks backward at structured data; data science looks forward and creates more value", "Example business goals (churn, fraud, upsell)"],
    model: "Business Intelligence is mostly descriptive analytics: it answers what happened and what the problem is, using standard reports, queries and alerts, usually over structured historical data. Data science moves further up the value curve. Predictive analytics asks why something is happening and what will happen next, using statistical and predictive modeling (for example predicting customer churn or fraud). Prescriptive analytics asks what happens if we try something and what the best scenario is, using optimization and randomized testing. So BI explains the past, while data science predicts and recommends actions, which is where the greater business value is." },
  { id: "sa-streaming", topic: "streaming", marks: 10, q: "Explain how Hadoop Streaming works with Python, how the mapper and reducer communicate, and how you can test a streaming job locally.",
    points: ["Utility JAR; any executable as mapper/reducer; uses Unix stdin/stdout (not real-time)", "Mapper: each map task runs the script in its own process; lines piped to stdin; writes key\\tvalue to stdout", "Shuffle & sort groups by key; reducer gets sorted key\\tvalue lines on stdin", "Reducer tracks the current key and emits when it changes (and at the end)", "Local test: chmod +x; cat data | ./mapper.py | sort | ./reducer.py"],
    model: "Hadoop Streaming is a utility JAR shipped with Hadoop that lets any executable act as the mapper and reducer, communicating through the standard Unix streams (so \"streaming\" here has nothing to do with real-time data). Each map task launches the Python mapper in its own process, pipes the input lines to its stdin, and collects what it writes to stdout. The mapper writes one key<TAB>value pair per line (tab is the default separator). Hadoop then shuffles and sorts the output by key and streams it to the reducer's stdin, guaranteed sorted, so all lines for one key arrive together. The reducer tracks the current key and emits the aggregate when the key changes, and once more at the end. Both scripts run inside an if __name__ == '__main__' block. To test locally, make them executable with chmod +x and run cat data.csv | ./mapper.py | sort | ./reducer.py, where sort simulates the shuffle." }
];

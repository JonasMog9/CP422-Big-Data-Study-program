/* CP422 Midterm Lab: Code Lab content.
 * Levels: 1 Read · 2 Fill in · 3 Arrange · 4 Write (guided) · 5 Exam-style.
 * Fill-in blanks are written ⟦answer|alternative⟧. Parsons lines are [text, group]:
 * lines sharing a group number may appear in any order.
 * Trace phases: input, map, shuffle, reduce, output, rdd (Spark stages). */
window.CP = window.CP || {};
(function () {
const R = String.raw;

CP.levels = [
  { n: 1, name: "Read", what: "Predict what code does" },
  { n: 2, name: "Fill in", what: "Type the missing pieces" },
  { n: 3, name: "Arrange", what: "Put the lines in order" },
  { n: 4, name: "Write", what: "Write it with a scaffold" },
  { n: 5, name: "Exam-style", what: "Full question, like the midterm" }
];

CP.tracks = [
  { id: "streaming", name: "Hadoop Streaming", lang: "python", blurb: "Python mapper.py and reducer.py reading stdin, writing key<TAB>value. The gentlest start if you know Python." },
  { id: "java", name: "Java MapReduce", lang: "java", blurb: "Mapper and Reducer classes, context.write, Iterable values, the driver. Start from public class, no imports." },
  { id: "pyspark", name: "PySpark RDDs", lang: "python", blurb: "textFile, flatMap, map, filter, reduceByKey, collect, saveAsTextFile." },
  { id: "sql", name: "Spark SQL & DataFrames", lang: "sql", blurb: "SQL with GROUP BY / HAVING / ORDER BY, and the same query with the DataFrame API." }
];

const FLIGHT_FIELDS = "Fields (0-based): 0 date · 1 airline · 2 flightnum · 3 origin · 4 dest · 5 dep · 6 dep_delay · 7 arv · 8 arv_delay · 9 airtime · 10 distance";
const FLIGHT_SAMPLE = R`2014-04-01,19805,1,JFK,LAX,0854,-6.00,1217,2.00,355.00,2475.00
2014-04-01,19805,2,LAX,JFK,0944,14.00,1736,-29.00,269.00,2475.00`;
const FARE_SAMPLE = R`2026-10-01,AC,YYZ,YVR,289.50
2026-10-01,WS,YYZ,YVR,249.00
2026-10-02,AC,YYZ,YUL,129.99
2026-10-02,PD,YYZ,YUL,99.00
2026-10-03,WS,YVR,YYZ,310.00`;
const NCDC_NOTE = "NCDC lines are fixed width. Python slices: year = line[15:19], temperature = line[87:92] (sign + 4 digits, tenths of °C), quality = line[92]. 9999 = missing; quality must be one of 0, 1, 4, 5, 9.";
const NCDC_SAMPLE = R`0067011990999991950051507004...9999999N9+00001+99999999999...
0043011990999991950051512004...9999999N9+00221+99999999999...
0043011990999991950051518004...9999999N9-00111+99999999999...
0043012650999991949032412004...0500001N9+01111+99999999999...
0043012650999991949032418004...0500001N9+00781+99999999999...`;
const SLIDE8_TRACE = [
  { phase: "input", label: "Input (offset, line)", rows: ["(0, 0067011990…1950…+0000…)", "(106, 0043011990…1950…+0022…)", "(212, 0043011990…1950…−0011…)", "(318, 0043012650…1949…+0111…)", "(424, 0043012650…1949…+0078…)"] },
  { phase: "map", label: "Map emits (year, temp)", rows: ["(1950, 0)", "(1950, 22)", "(1950, −11)", "(1949, 111)", "(1949, 78)"] },
  { phase: "shuffle", label: "Shuffle & sort", rows: ["(1949, [111, 78])", "(1950, [0, 22, −11])"] },
  { phase: "reduce", label: "Reduce: max", rows: ["(1949, 111)", "(1950, 22)"] }
];
const FARE_TRACE = [
  { phase: "input", label: "Input lines", rows: ["…,AC,YYZ,YVR,289.50", "…,WS,YYZ,YVR,249.00", "…,AC,YYZ,YUL,129.99", "…,PD,YYZ,YUL,99.00", "…,WS,YVR,YYZ,310.00"] },
  { phase: "map", label: "Map emits (route, price)", rows: ["(YYZ-YVR, 289.50)", "(YYZ-YVR, 249.00)", "(YYZ-YUL, 129.99)", "(YYZ-YUL, 99.00)", "(YVR-YYZ, 310.00)"] },
  { phase: "shuffle", label: "Shuffle & sort", rows: ["(YVR-YYZ, [310.00])", "(YYZ-YUL, [129.99, 99.00])", "(YYZ-YVR, [289.50, 249.00])"] },
  { phase: "reduce", label: "Reduce: min", rows: ["(YVR-YYZ, 310.0)", "(YYZ-YUL, 99.0)", "(YYZ-YVR, 249.0)"] }
];

CP.code = [
  /* ===================== Hadoop Streaming ===================== */
  { id: "st-wc-map-read", track: "streaming", level: 1, type: "mcq", title: "What does the word-count mapper print?",
    lang: "python", code: R`#!/usr/bin/env python
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        for word in line.split():
            sys.stdout.write("{}\t1\n".format(word))`,
    q: "The input is one line: `big data is big`. What does this mapper write to stdout?",
    options: ["Four lines: `big\\t1`, `data\\t1`, `is\\t1`, `big\\t1`", "Three lines: `big\\t2`, `data\\t1`, `is\\t1`", "One line: `big data is big\\t1`", "`(big, [1, 1])`, `(data, [1])`, `(is, [1])`"],
    answer: 0,
    explain: "A mapper never adds anything up. It writes one line per word with the value 1. Counting happens later, in the reducer, after shuffle & sort puts equal words next to each other.",
    src: "MR slide 24" },
  { id: "st-wc-sorted-read", track: "streaming", level: 1, type: "mcq", title: "What does the reducer actually receive?",
    lang: "bash", code: R`cat input.txt | ./mapper.py | sort | ./reducer.py`,
    q: "The mapper printed `big\\t1`, `data\\t1`, `is\\t1`, `big\\t1`. After `sort`, what does the streaming reducer read from stdin?",
    options: ["`big\\t1`, `big\\t1`, `data\\t1`, `is\\t1` (one line at a time)", "`big\\t[1, 1]`, `data\\t[1]`, `is\\t[1]`", "`big\\t2`, `data\\t1`, `is\\t1`", "`is\\t1`, `data\\t1`, `big\\t1`, `big\\t1`"],
    answer: 0,
    explain: "In Hadoop Streaming the reducer gets individual sorted lines, NOT lists. All you are guaranteed is that equal keys sit next to each other. That's why the reducer has to watch for the key changing. (Java's reducer is different: it gets the key plus an Iterable of values.)",
    src: "MR slide 25" },
  { id: "st-bug-slide25", track: "streaming", level: 1, type: "mcq", title: "Spot the bug in the slide 25 reducer",
    lang: "python", code: R`#!/usr/bin/env python
import sys

if __name__ == '__main__':
    curkey = None
    total  = 0
    for line in sys.stdin:
        key, val = line.split("\t")
        val = int(val)

        if key == curkey:
            total += val
        else:
            if curkey is not None:
                sys.stdout.write("{}\t{}\n".format(curkey, total))

            curkey = key
            total  = val`,
    q: "This is the reducer exactly as on slide 25. Its input is `big\\t1`, `big\\t1`, `data\\t1`, `is\\t1`. What does it print?",
    options: ["Only `big\\t2` and `data\\t1`", "`big\\t2`, `data\\t1`, `is\\t1`", "`big\\t1`, `big\\t1`, `data\\t1`, `is\\t1`", "Nothing: it crashes on the first line"],
    answer: 0,
    explain: "It only writes when the key CHANGES. Trace: big → nothing printed (curkey was None); big → total 2; data → prints big 2; is → prints data 1; then the loop ends and `is 1` is never printed. Fix: after the loop add `if curkey is not None: sys.stdout.write(\"{}\\t{}\\n\".format(curkey, total))`. Always flush the last key.",
    src: "MR slide 25" },
  { id: "st-wc-fill", track: "streaming", level: 2, type: "fill", title: "Fill in the word-count mapper",
    lang: "python", prompt: "Complete the Hadoop Streaming word-count mapper. Type `\\t` for a tab.",
    code: R`#!/usr/bin/env python
import ⟦sys⟧

if __name__ == "__main__":
    for line in sys.⟦stdin⟧:
        for word in line.⟦split⟧():
            sys.stdout.⟦write⟧("{}⟦\t⟧1\n".format(⟦word⟧))`,
    explain: "Read from `sys.stdin`, split each line into words, and write `word<TAB>1` to `sys.stdout`. The tab separates key from value.",
    src: "MR slide 24" },
  { id: "st-flight-mapper-fill", track: "streaming", level: 2, type: "fill", title: "Fill in the flight-delay Mapper class (slide 26)",
    lang: "python", prompt: "The mapper must emit (origin airport, departure delay). " + FLIGHT_FIELDS,
    data: FLIGHT_SAMPLE,
    code: R`import sys
import csv

SEP = "\t"

class Mapper(object):

    def __init__(self, stream, sep=SEP):
        self.stream = stream
        self.sep    = sep

    def emit(self, key, value):
        sys.stdout.write("{}{}{}\n".format(key, self.⟦sep⟧, value))

    def map(self):
        for row in self:
            self.⟦emit⟧(row[⟦3⟧], row[⟦6⟧])

    def __iter__(self):
        reader = csv.⟦reader⟧(self.stream)
        for row in reader:
            ⟦yield⟧ row

if __name__ == '__main__':
    mapper = Mapper(sys.⟦stdin⟧)
    mapper.⟦map⟧()`,
    explain: "`__iter__` makes the Mapper iterable: `csv.reader` turns each line into a list and `yield` hands rows out one at a time, so `for row in self` works. `map()` emits `row[3]` (origin) and `row[6]` (dep_delay) through `emit`, which writes `key<SEP>value`.",
    src: "MR slide 26" },
  { id: "st-flight-reducer-fill", track: "streaming", level: 2, type: "fill", title: "Fill in the average-delay Reducer class (slide 27)",
    lang: "python", prompt: "The prof said he's looking for `emit`, `reduce` and this structure. Complete the reducer that averages the delay for each key.",
    code: R`import sys
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
        for current, group in ⟦groupby⟧(self, ⟦itemgetter⟧(0)):
            total = 0
            count = 0

            for item in group:
                total += item[⟦1⟧]
                count += ⟦1⟧

            self.⟦emit⟧(current, float(total) / float(⟦count⟧))

    def __iter__(self):
        for line in self.stream:
            try:
                parts = line.split(self.⟦sep⟧)
                yield parts[0], ⟦float⟧(parts[1])
            except:
                continue

if __name__ == '__main__':
    reducer = Reducer(sys.stdin)
    reducer.⟦reduce⟧()`,
    explain: "`__iter__` turns each `key\\tvalue` line into a tuple `(key, float(value))` and skips bad lines. `groupby(self, itemgetter(0))` groups consecutive tuples with the same key (this works because the input is sorted). For each group: sum the values (`item[1]`), count them, and emit the average. Unlike slide 25, groupby handles the last key for you.",
    src: "MR slide 27" },
  { id: "st-reducer-parsons", track: "streaming", level: 3, type: "parsons", title: "Arrange a correct word-count reducer",
    lang: "python", prompt: "Tap the lines in order to build a streaming reducer that sums the counts for each word. It must also print the LAST word. Two lines are distractors.",
    lines: [
      [R`import sys`, 0],
      [R`curkey = None`, 1],
      [R`total = 0`, 1],
      [R`for line in sys.stdin:`, 2],
      [R`    key, val = line.strip().split("\t")`, 3],
      [R`    val = int(val)`, 4],
      [R`    if key == curkey:`, 5],
      [R`        total += val`, 6],
      [R`    else:`, 7],
      [R`        if curkey is not None:`, 8],
      [R`            sys.stdout.write("{}\t{}\n".format(curkey, total))`, 9],
      [R`        curkey = key`, 10],
      [R`        total = val`, 10],
      [R`if curkey is not None:`, 11],
      [R`    sys.stdout.write("{}\t{}\n".format(curkey, total))`, 12]
    ],
    distractors: [R`        total = 0`, R`    print(key, val)`],
    explain: "Start with no current key. For each line, split on the tab and convert the value. Same key: add. New key: print the finished previous key (if there is one), then start the new key with total = val (not 0, or you'd lose this line's count). After the loop, print the last key.",
    src: "MR slide 25 (fixed)" },
  { id: "st-max-delay-write", track: "streaming", level: 4, type: "write", title: "Write reduce(): max delay per airport",
    lang: "python",
    prompt: "The mapper from slide 26 emits `origin<TAB>dep_delay`. Write only the `reduce(self)` method so the job outputs the **maximum** departure delay for each origin airport. The rest of the class is the same as slide 27.",
    data: R`JFK	-6.0
JFK	31.0
JFK	12.0
LAX	14.0
LAX	-2.0`,
    expected: R`JFK	31.0
LAX	14.0`,
    starter: R`class Reducer(object):
    # __init__, emit and __iter__ are the same as slide 27.
    # __iter__ yields (key, float(value)) tuples, sorted by key.

    def reduce(self):
        # your code here
        pass`,
    hints: [
      "Copy the shape of slide 27's `reduce`: the same `for current, group in groupby(self, itemgetter(0)):` loop. Only the math inside changes.",
      "Each `item` in `group` is a tuple `(key, delay)`. Keep the biggest `item[1]` seen so far, starting from `None` or the first value.",
      "After the inner loop finishes, call `self.emit(current, biggest)` once per airport."
    ],
    solution: R`    def reduce(self):
        for current, group in groupby(self, itemgetter(0)):
            biggest = None
            for item in group:
                delay = item[1]
                if biggest is None or delay > biggest:
                    biggest = delay
            self.emit(current, biggest)

    # Shorter version, same logic:
    # def reduce(self):
    #     for current, group in groupby(self, itemgetter(0)):
    #         self.emit(current, max(item[1] for item in group))`,
    rubric: [
      { pts: 2, text: "Groups the input by key (groupby with itemgetter(0), or curkey tracking)" },
      { pts: 2, text: "Loops over all values for the current key" },
      { pts: 4, text: "Keeps the maximum correctly (compares with the best so far, or uses max())" },
      { pts: 2, text: "Emits (key, max) once per key, after that key's values are done" }
    ],
    checks: [
      { re: "groupby|curkey|current", label: "Groups lines by key" },
      { re: "max\\s*\\(|>", label: "Compares to find a maximum" },
      { re: "emit\\s*\\(|stdout\\.write", label: "Emits a result" }
    ],
    explain: "Same skeleton as the average reducer. Swapping `total += …; count += 1` for a running maximum is exactly the kind of small change an exam question asks for.",
    src: "MR slides 26-27" },
  { id: "st-airline-mapper-write", track: "streaming", level: 4, type: "write", title: "Write map(): arrival delay per airline",
    lang: "python",
    prompt: "Change the slide 26 mapper so the job averages the **arrival delay per airline**. Write only the `map(self)` method. " + FLIGHT_FIELDS,
    data: FLIGHT_SAMPLE,
    expected: R`19805	2.00
19805	-29.00`,
    starter: R`class Mapper(object):
    # __init__, emit and __iter__ are the same as slide 26.

    def map(self):
        # your code here
        pass`,
    hints: [
      "You only change which columns are emitted. Which index is the airline? Which is arv_delay?",
      "`for row in self:` then `self.emit(row[?], row[?])`."
    ],
    solution: R`    def map(self):
        for row in self:
            self.emit(row[1], row[8])    # airline, arrival delay`,
    rubric: [
      { pts: 2, text: "Loops over the rows (for row in self / csv.reader)" },
      { pts: 2, text: "Key is the airline: row[1]" },
      { pts: 2, text: "Value is the arrival delay: row[8]" },
      { pts: 1, text: "Emits key and value with the separator (emit or stdout.write with \\t)" }
    ],
    checks: [
      { re: "row\\s*\\[\\s*1\\s*\\]", label: "Uses row[1] (airline) as the key" },
      { re: "row\\s*\\[\\s*8\\s*\\]", label: "Uses row[8] (arv_delay) as the value" },
      { re: "emit\\s*\\(|stdout\\.write", label: "Emits the pair" }
    ],
    explain: "The slide 27 reducer still works unchanged: it averages whatever value arrives for each key. The mapper decides what is grouped (the key) and what is aggregated (the value).",
    src: "MR slide 26" },
  { id: "st-wc-reducer-write", track: "streaming", level: 4, type: "write", title: "Write the word-count reducer from memory",
    lang: "python",
    prompt: "Write a complete streaming `reducer.py` for word count. Input lines are `word<TAB>1`, sorted by word. Print `word<TAB>total` for every word, including the last one.",
    data: R`big	1
big	1
data	1
is	1`,
    expected: R`big	2
data	1
is	1`,
    starter: R`#!/usr/bin/env python
import sys

if __name__ == "__main__":
    # your code here
    pass`,
    hints: [
      "You need two variables before the loop: the key you are currently counting, and its running total.",
      "For each line: split on \"\\t\", convert the value with int(). Same key → add. Different key → print the old key (if any), then reset to the new key.",
      "Don't forget the final print after the loop."
    ],
    solution: R`#!/usr/bin/env python
import sys

if __name__ == "__main__":
    curkey = None
    total = 0
    for line in sys.stdin:
        key, val = line.strip().split("\t")
        val = int(val)
        if key == curkey:
            total += val
        else:
            if curkey is not None:
                sys.stdout.write("{}\t{}\n".format(curkey, total))
            curkey = key
            total = val
    if curkey is not None:
        sys.stdout.write("{}\t{}\n".format(curkey, total))`,
    rubric: [
      { pts: 2, text: "Reads lines from sys.stdin and splits each into key and value on the tab" },
      { pts: 1, text: "Converts the value to a number" },
      { pts: 3, text: "Adds to the running total while the key stays the same" },
      { pts: 2, text: "On a new key: writes the previous key's total, then resets for the new key" },
      { pts: 2, text: "Writes the last key's total after the loop" }
    ],
    checks: [
      { re: "sys\\.stdin", label: "Reads sys.stdin" },
      { re: "split\\s*\\(", label: "Splits each line" },
      { re: "int\\s*\\(", label: "Converts the value to int" },
      { re: "stdout\\.write|print\\s*\\(", label: "Writes output" }
    ],
    explain: "This is the corrected slide 25 reducer. Practise it until you can write it without looking. Most streaming exam answers are this loop with different math.",
    src: "MR slide 25" },
  { id: "st-cheapest", track: "streaming", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: cheapest flight per route (Python streaming)",
    lang: "python",
    prompt: "Write `mapper.py` and `reducer.py` for Hadoop Streaming that find the **cheapest price for each route** (origin-destination). Each input line is `date,airline,origin,dest,price`. Output one line per route: `route<TAB>cheapest_price`.",
    data: FARE_SAMPLE,
    expected: R`YVR-YYZ	310.0
YYZ-YUL	99.0
YYZ-YVR	249.0`,
    starter: R`# mapper.py
#!/usr/bin/env python
import sys


# reducer.py
#!/usr/bin/env python
import sys
`,
    hints: [
      "Ask: what should be grouped together? (the route) That's the key. What is aggregated? (the price) That's the value.",
      "Mapper: split each line on \",\", build `route = fields[2] + \"-\" + fields[3]`, write `route\\tprice`.",
      "Reducer: the word-count reducer, but keep the minimum instead of a sum. Remember the final write after the loop."
    ],
    solution: R`# ---------------- mapper.py (slide 26 style) ----------------
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
            self.emit(row[2] + "-" + row[3], row[4])     # route, price

    def __iter__(self):
        reader = csv.reader(self.stream)
        for row in reader:
            yield row

if __name__ == '__main__':
    mapper = Mapper(sys.stdin)
    mapper.map()

# ---------------- reducer.py (slide 27 style) ----------------
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
            cheapest = None
            for item in group:
                if cheapest is None or item[1] < cheapest:
                    cheapest = item[1]
            self.emit(current, cheapest)

    def __iter__(self):
        for line in self.stream:
            try:
                parts = line.split(self.sep)
                yield parts[0], float(parts[1])
            except:
                continue

if __name__ == '__main__':
    reducer = Reducer(sys.stdin)
    reducer.reduce()

# min is associative, so this reducer could also be used as a combiner.`,
    alt: { label: "Also correct: the slide 25 loop style (with the final write fixed)", code: R`# ---------------- mapper.py ----------------
#!/usr/bin/env python
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        fields = line.strip().split(",")
        if len(fields) < 5:
            continue                              # skip bad lines
        route = fields[2] + "-" + fields[3]       # origin-dest
        price = fields[4]
        sys.stdout.write("{}\t{}\n".format(route, price))

# ---------------- reducer.py ----------------
#!/usr/bin/env python
import sys

if __name__ == "__main__":
    curkey = None
    cheapest = None
    for line in sys.stdin:
        key, val = line.strip().split("\t")
        val = float(val)
        if key == curkey:
            cheapest = min(cheapest, val)
        else:
            if curkey is not None:
                sys.stdout.write("{}\t{}\n".format(curkey, cheapest))
            curkey = key
            cheapest = val
    if curkey is not None:
        sys.stdout.write("{}\t{}\n".format(curkey, cheapest))

# Run locally:  cat fares.csv | ./mapper.py | sort | ./reducer.py` },
    trace: FARE_TRACE,
    rubric: [
      { pts: 2, text: "Mapper reads sys.stdin and splits each CSV line on commas (csv.reader or split)" },
      { pts: 3, text: "Mapper emits key = route (origin + dest) and value = price, tab separated (emit or stdout.write)" },
      { pts: 1, text: "Reducer splits each line on the tab and converts the price to float" },
      { pts: 2, text: "Reducer keeps the minimum price per route (groupby group, or while the key stays the same)" },
      { pts: 2, text: "Reducer emits each route's minimum exactly once (groupby handles the last key; the slide 25 loop needs a final write)" }
    ],
    checks: [
      { re: "split\\s*\\(\\s*[\"'],[\"']\\s*\\)|csv\\.reader", label: "Mapper splits the CSV (csv.reader or split)" },
      { re: "\\[\\s*2\\s*\\][\\s\\S]*\\[\\s*3\\s*\\]", label: "Builds the route from fields 2 and 3" },
      { re: "\\\\t", label: "Uses a tab separator" },
      { re: "min\\s*\\(|<", label: "Reducer finds a minimum" },
      { re: "float\\s*\\(", label: "Converts prices to float" }
    ],
    explain: "The only real decision is key = route, value = price. The rest is the slide 26-27 Mapper/Reducer classes with min instead of an average (the prof said he is looking for emit and reduce from slide 27). If the question asked for the single cheapest flight overall, emit the same constant key for every line (e.g. `cheapest\\troute,price`) so one reducer sees them all.",
    src: "Notes Sep 28 (prof's example), MR slides 24-28" },
  { id: "st-ncdc-max", track: "streaming", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: max temperature per year (Python streaming)",
    lang: "python",
    prompt: "Write `mapper.py` and `reducer.py` (Hadoop Streaming) that output the **maximum valid temperature for each year** from NCDC records. " + NCDC_NOTE,
    data: NCDC_SAMPLE,
    expected: R`1949	111
1950	22`,
    starter: R`# mapper.py
#!/usr/bin/env python
import sys


# reducer.py
#!/usr/bin/env python
import sys
`,
    hints: [
      "This is slide 8 to slide 10 in Python. The mapper pulls (year, temperature) out of each line and drops bad readings.",
      "Mapper: `year = line[15:19]`, `temp = int(line[87:92])`, `q = line[92]`. Emit only if `temp != 9999 and q in \"01459\"`.",
      "Reducer: running maximum per year with the curkey pattern. Final write after the loop."
    ],
    solution: R`# ---------------- mapper.py ----------------
#!/usr/bin/env python
import sys

MISSING = 9999

if __name__ == "__main__":
    for line in sys.stdin:
        year = line[15:19]
        temp = int(line[87:92])          # int() accepts a leading + or -
        quality = line[92]
        if temp != MISSING and quality in "01459":
            sys.stdout.write("{}\t{}\n".format(year, temp))

# ---------------- reducer.py ----------------
#!/usr/bin/env python
import sys

if __name__ == "__main__":
    curkey = None
    maxtemp = None
    for line in sys.stdin:
        key, val = line.strip().split("\t")
        val = int(val)
        if key == curkey:
            maxtemp = max(maxtemp, val)
        else:
            if curkey is not None:
                sys.stdout.write("{}\t{}\n".format(curkey, maxtemp))
            curkey = key
            maxtemp = val
    if curkey is not None:
        sys.stdout.write("{}\t{}\n".format(curkey, maxtemp))`,
    trace: SLIDE8_TRACE,
    rubric: [
      { pts: 2, text: "Mapper extracts the year (line[15:19]) and temperature (line[87:92]) as a number" },
      { pts: 2, text: "Mapper filters out 9999 and bad quality codes (keeps 0, 1, 4, 5, 9)" },
      { pts: 2, text: "Mapper writes year<TAB>temperature to stdout" },
      { pts: 2, text: "Reducer keeps the maximum per year" },
      { pts: 2, text: "Reducer writes on key change and after the loop" }
    ],
    checks: [
      { re: "15\\s*:\\s*19", label: "Year slice [15:19]" },
      { re: "87\\s*:\\s*92", label: "Temperature slice [87:92]" },
      { re: "9999", label: "Filters missing values (9999)" },
      { re: "01459", label: "Checks the quality code" },
      { re: "max\\s*\\(|>", label: "Reducer keeps a maximum" }
    ],
    explain: "Same logic as the Java slides, written as streaming scripts. Filtering bad records in the mapper means they never cross the network.",
    src: "MR slides 3, 7-10" },

  /* ===================== Java MapReduce ===================== */
  { id: "java-types", track: "java", level: 1, type: "mcq", title: "The four types in Mapper<…>",
    lang: "java", code: R`public class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {`,
    q: "What do the four types in `Mapper<LongWritable, Text, Text, IntWritable>` mean, in order?",
    options: ["Input key (byte offset), input value (the line), output key (year), output value (temperature)", "Output key, output value, input key, input value", "Key class, value class, combiner class, reducer class", "Year, temperature, line, offset"],
    answer: 0,
    explain: "`Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>`. The reducer's INPUT types must match the mapper's OUTPUT types, so the reducer is `Reducer<Text, IntWritable, Text, IntWritable>`. `extends` is Java inheritance: your class IS a Mapper and overrides its `map()` method.",
    src: "MR slide 9" },
  { id: "java-extends", track: "java", level: 1, type: "mcq", title: "What does extends + @Override mean?",
    lang: "java", code: R`public class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    ...
  }
}`,
    q: "The prof said to review Java inheritance. What does this structure mean?",
    options: ["MaxTemperatureMapper is a subclass of Hadoop's Mapper; it inherits Mapper's behaviour and replaces (overrides) map() with its own logic, which Hadoop calls once per input record", "It creates a Mapper object called MaxTemperatureMapper", "It imports the Mapper library", "map() runs once per file, not per record"],
    answer: 0,
    explain: "Hadoop's framework calls your overridden `map()` once for every (key, value) record in the split, and `reduce()` once per key. You never call them yourself, which is why the exam says to start from `public class` and skip imports.",
    src: "MR slides 9-10, Notes Sep 28" },
  { id: "mr-trace-slide8", track: "java", level: 1, type: "mcq", title: "Slide 8 walkthrough (prof: important)",
    lang: "text", code: NCDC_SAMPLE,
    q: "The map function emits (1950, 0), (1950, 22), (1950, −11), (1949, 111), (1949, 78). After shuffle and sort, what does the reduce function receive?",
    options: ["(1949, [111, 78]) and (1950, [0, 22, −11])", "(1950, 22) and (1949, 111)", "The original five lines with their offsets", "(1950, 0), (1950, 22), (1950, −11), (1949, 111), (1949, 78) unchanged"],
    answer: 0,
    explain: "Shuffle & sort groups the map output by key and sorts the keys (1949 before 1950). Each reduce call gets one key and ALL of its values. Reduce then outputs (1949, 111) and (1950, 22). Use the flow below to rehearse each stage.",
    trace: SLIDE8_TRACE,
    src: "MR slide 8" },
  { id: "java-reducer-read", track: "java", level: 1, type: "mcq", title: "Reading the max temperature reducer",
    lang: "java", code: R`public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
  int maxValue = Integer.MIN_VALUE;
  for (IntWritable value : values) {
    maxValue = Math.max(maxValue, value.get());
  }
  context.write(key, new IntWritable(maxValue));
}`,
    q: "Which statement about this reducer is correct?",
    options: ["`maxValue` starts at Integer.MIN_VALUE so the first real temperature always replaces it; `value.get()` turns the IntWritable into an int", "It is called once per temperature record", "`values` holds the temperatures for every year", "It must be called by the driver in a loop"],
    answer: 0,
    explain: "reduce() is called once per key (year) with an Iterable of only that year's temperatures. For a minimum you'd start at `Integer.MAX_VALUE` and use `Math.min`.",
    src: "MR slide 10" },
  { id: "java-mapper-fill", track: "java", level: 2, type: "fill", title: "Fill in the MaxTemperatureMapper",
    lang: "java", prompt: "Complete the mapper from slide 9.",
    code: R`public class MaxTemperatureMapper
    extends ⟦Mapper⟧<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = ⟦9999⟧;

  @Override
  public void ⟦map⟧(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.⟦toString⟧();
    String year = line.substring(⟦15⟧, 19);
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.⟦write⟧(new ⟦Text⟧(year), new IntWritable(airTemperature));
    }
  }
}`,
    explain: "Convert the Text value to a String, slice out the year and temperature, skip missing or bad-quality readings, and emit with `context.write(new Text(year), new IntWritable(temp))`.",
    src: "MR slide 9" },
  { id: "java-reducer-fill", track: "java", level: 2, type: "fill", title: "Fill in the MaxTemperatureReducer",
    lang: "java", prompt: "Complete the reducer from slide 10.",
    code: R`public class MaxTemperatureReducer
    extends ⟦Reducer⟧<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, ⟦Iterable⟧<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    int maxValue = Integer.⟦MIN_VALUE⟧;
    for (IntWritable value : ⟦values⟧) {
      maxValue = Math.⟦max⟧(maxValue, value.⟦get⟧());
    }
    context.write(⟦key⟧, new IntWritable(maxValue));
  }
}`,
    explain: "Reducer types match the mapper's output (Text, IntWritable). Loop the Iterable, keep the max, write (key, max).",
    src: "MR slide 10" },
  { id: "java-driver-fill", track: "java", level: 2, type: "fill", title: "Fill in the driver (with combiner)",
    lang: "java", prompt: "Complete the driver from slide 13.",
    code: R`public class MaxTemperatureWithCombiner {

  public static void main(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(MaxTemperatureWithCombiner.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[⟦0⟧]));
    FileOutputFormat.setOutputPath(job, new Path(args[⟦1⟧]));

    job.⟦setMapperClass⟧(MaxTemperatureMapper.class);
    job.setCombinerClass(⟦MaxTemperatureReducer⟧.class);
    job.⟦setReducerClass⟧(MaxTemperatureReducer.class);

    job.setOutputKeyClass(⟦Text⟧.class);
    job.setOutputValueClass(⟦IntWritable⟧.class);

    System.exit(job.⟦waitForCompletion⟧(true) ? 0 : 1);
  }
}`,
    explain: "args[0] = input path, args[1] = output path (which must not exist yet). The combiner is the reducer class, which is safe because max of maxes = max. `waitForCompletion(true)` submits the job and waits.",
    src: "MR slides 11, 13" },
  { id: "java-mapper-parsons", track: "java", level: 3, type: "parsons", title: "Arrange the body of map()",
    lang: "java", prompt: "Put the body of the max temperature `map()` method in order. Two lines are distractors.",
    lines: [
      [R`String line = value.toString();`, 0],
      [R`String year = line.substring(15, 19);`, 1],
      [R`int airTemperature;`, 1],
      [R`if (line.charAt(87) == '+') {`, 2],
      [R`  airTemperature = Integer.parseInt(line.substring(88, 92));`, 3],
      [R`} else {`, 4],
      [R`  airTemperature = Integer.parseInt(line.substring(87, 92));`, 5],
      [R`}`, 6],
      [R`String quality = line.substring(92, 93);`, 7],
      [R`if (airTemperature != MISSING && quality.matches("[01459]")) {`, 8],
      [R`  context.write(new Text(year), new IntWritable(airTemperature));`, 9],
      [R`}`, 10]
    ],
    distractors: [R`String year = line.substring(87, 92);`, R`if (airTemperature == MISSING) {`],
    explain: "Convert to String → slice year → parse temperature (handling the + sign) → read quality → emit only valid readings. `year` and `int airTemperature;` can go in either order.",
    src: "MR slide 9" },
  { id: "java-driver-parsons", track: "java", level: 3, type: "parsons", title: "Assemble the driver",
    lang: "java", prompt: "Build the body of `main()` for the max temperature job with a combiner. The `job.set…` lines can go in any order, but `new Job()` must come first and `System.exit` last. Two lines are wrong; leave them out.",
    lines: [
      [R`Job job = new Job();`, 0],
      [R`job.setJarByClass(MaxTemperatureWithCombiner.class);`, 1],
      [R`job.setJobName("Max temperature");`, 1],
      [R`FileInputFormat.addInputPath(job, new Path(args[0]));`, 1],
      [R`FileOutputFormat.setOutputPath(job, new Path(args[1]));`, 1],
      [R`job.setMapperClass(MaxTemperatureMapper.class);`, 1],
      [R`job.setCombinerClass(MaxTemperatureReducer.class);`, 1],
      [R`job.setReducerClass(MaxTemperatureReducer.class);`, 1],
      [R`job.setOutputKeyClass(Text.class);`, 1],
      [R`job.setOutputValueClass(IntWritable.class);`, 1],
      [R`System.exit(job.waitForCompletion(true) ? 0 : 1);`, 2]
    ],
    distractors: [R`job.setCombinerClass(MaxTemperatureMapper.class);`, R`FileInputFormat.addInputPath(job, new Path(args[1]));`],
    explain: "The combiner must be a reducer-style class (it consumes map output). args[0] is input, args[1] is output.",
    src: "MR slides 11, 13" },
  { id: "java-wc-mapper-write", track: "java", level: 4, type: "write", title: "Write the word-count mapper",
    lang: "java",
    prompt: "Write the body of `map()` so it emits `(word, 1)` for every word in the line.",
    data: R`big data is big`,
    expected: R`(big, 1) (data, 1) (is, 1) (big, 1)`,
    starter: R`public class WordCountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // your code here
  }
}`,
    hints: [
      "First turn the Text into a String: `String line = value.toString();`",
      "Split it on spaces: `line.split(\" \")`, and loop over the words.",
      "Emit each one: `context.write(new Text(word), new IntWritable(1));`"
    ],
    solution: R`public class WordCountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    for (String word : line.split(" ")) {
      if (word.length() > 0) {
        context.write(new Text(word), new IntWritable(1));
      }
    }
  }
}`,
    rubric: [
      { pts: 2, text: "Converts the Text value to a String" },
      { pts: 2, text: "Splits the line into words and loops over them" },
      { pts: 3, text: "Emits (word, 1) with context.write using Text and IntWritable" }
    ],
    checks: [
      { re: "toString\\s*\\(", label: "Converts value to String" },
      { re: "split\\s*\\(|StringTokenizer", label: "Splits into words" },
      { re: "context\\.write", label: "Emits with context.write" },
      { re: "IntWritable\\s*\\(\\s*1\\s*\\)", label: "Value is IntWritable(1)" }
    ],
    explain: "Same pattern as the Python mapper: one output pair per word, value 1. The key (byte offset) is ignored.",
    src: "MR slides 9, 24" },
  { id: "java-wc-reducer-write", track: "java", level: 4, type: "write", title: "Write the word-count reducer",
    lang: "java",
    prompt: "Write the whole `WordCountReducer` class (start from `public class`). It receives `(word, [1, 1, …])` and must write `(word, total)`.",
    data: R`(big, [1, 1])
(data, [1])
(is, [1])`,
    expected: R`(big, 2) (data, 1) (is, 1)`,
    starter: R`public class WordCountReducer
    extends Reducer<…> {

}`,
    hints: [
      "The input types match the mapper's output: Text and IntWritable. So `extends Reducer<Text, IntWritable, Text, IntWritable>`.",
      "Signature: `public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException`.",
      "`int sum = 0;` loop `for (IntWritable value : values) sum += value.get();` then `context.write(key, new IntWritable(sum));`."
    ],
    solution: R`public class WordCountReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }
    context.write(key, new IntWritable(sum));
  }
}`,
    rubric: [
      { pts: 2, text: "Class extends Reducer<Text, IntWritable, Text, IntWritable>" },
      { pts: 2, text: "reduce() takes (Text key, Iterable<IntWritable> values, Context context)" },
      { pts: 2, text: "Sums value.get() over all values" },
      { pts: 2, text: "Writes (key, total) with context.write" }
    ],
    checks: [
      { re: "extends\\s+Reducer\\s*<\\s*Text\\s*,\\s*IntWritable", label: "extends Reducer<Text, IntWritable, …>" },
      { re: "Iterable\\s*<\\s*IntWritable\\s*>", label: "Takes Iterable<IntWritable> values" },
      { re: "\\.get\\s*\\(\\s*\\)", label: "Uses value.get()" },
      { re: "context\\.write", label: "Writes with context.write" }
    ],
    explain: "Unlike the streaming reducer, Java does the grouping for you: one reduce() call per word with all its values.",
    src: "MR slide 10" },
  { id: "java-min-temp", track: "java", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: minimum temperature per year (Java)",
    lang: "java",
    prompt: "Using the Java MapReduce API, write the Mapper and Reducer classes that output the **minimum valid temperature for each year** from NCDC records. Start from `public class`; no imports needed. " + NCDC_NOTE.replace("Python slices: year = line[15:19], temperature = line[87:92] (sign + 4 digits, tenths of °C), quality = line[92]", "Java: year = line.substring(15, 19), temperature = line.substring(87, 92) (if charAt(87) is '+', parse substring(88, 92)), quality = line.substring(92, 93)"),
    data: NCDC_SAMPLE,
    expected: R`1949	78
1950	-11`,
    starter: R`public class MinTemperatureMapper
    extends Mapper<…> {

}

public class MinTemperatureReducer
    extends Reducer<…> {

}`,
    hints: [
      "The mapper is identical to slide 9. Only the reducer changes.",
      "Reducer: start with `int minValue = Integer.MAX_VALUE;` and use `Math.min`.",
      "Types: Mapper<LongWritable, Text, Text, IntWritable>, Reducer<Text, IntWritable, Text, IntWritable>."
    ],
    solution: R`public class MinTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature;
    if (line.charAt(87) == '+') {
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(airTemperature));
    }
  }
}

public class MinTemperatureReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int minValue = Integer.MAX_VALUE;
    for (IntWritable value : values) {
      minValue = Math.min(minValue, value.get());
    }
    context.write(key, new IntWritable(minValue));
  }
}

// Driver: same as slide 11. A combiner is safe here: min of mins = min.`,
    trace: [
      SLIDE8_TRACE[0], SLIDE8_TRACE[1], SLIDE8_TRACE[2],
      { phase: "reduce", label: "Reduce: min", rows: ["(1949, 78)", "(1950, −11)"] }
    ],
    rubric: [
      { pts: 1, text: "Mapper extends Mapper<LongWritable, Text, Text, IntWritable> (types in the right order)" },
      { pts: 2, text: "Mapper converts value to String and extracts year and temperature" },
      { pts: 2, text: "Mapper filters missing (9999) and bad-quality readings" },
      { pts: 1, text: "Mapper emits (year, temperature) with context.write" },
      { pts: 1, text: "Reducer extends Reducer<Text, IntWritable, Text, IntWritable>" },
      { pts: 2, text: "Reducer starts from Integer.MAX_VALUE (or the first value) and keeps Math.min over value.get()" },
      { pts: 1, text: "Reducer writes (year, min)" }
    ],
    checks: [
      { re: "extends\\s+Mapper\\s*<\\s*LongWritable\\s*,\\s*Text\\s*,\\s*Text\\s*,\\s*IntWritable", label: "Mapper types in the right order" },
      { re: "substring\\s*\\(\\s*15\\s*,\\s*19\\s*\\)", label: "Extracts the year" },
      { re: "9999|MISSING", label: "Filters missing temperatures" },
      { re: "extends\\s+Reducer\\s*<", label: "Has a Reducer class" },
      { re: "MAX_VALUE|Math\\.min", label: "Reducer computes a minimum" },
      { re: "context\\.write[\\s\\S]*context\\.write", label: "Both classes write output" }
    ],
    explain: "Only three things change from the slides: the class names, Integer.MAX_VALUE, and Math.min. Exam questions are often a small twist on slide code.",
    src: "MR slides 9-10" },
  { id: "java-cheapest", track: "java", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: cheapest flight per route (Java)",
    lang: "java",
    prompt: "The prof's own example: \"I want to find the cheapest flight.\" Using the Java MapReduce API, write the Mapper and Reducer that output the **cheapest price for each route** (origin-destination). Input lines are `date,airline,origin,dest,price`. Also say whether you'd use a combiner.",
    data: FARE_SAMPLE,
    expected: R`YVR-YYZ	310.0
YYZ-YUL	99.0
YYZ-YVR	249.0`,
    starter: R`public class CheapestFlightMapper
    extends Mapper<…> {

}

public class CheapestFlightReducer
    extends Reducer<…> {

}`,
    hints: [
      "Key = route (origin + \"-\" + dest), value = price. Prices have decimals, so use DoubleWritable.",
      "Mapper: `String[] fields = value.toString().split(\",\");` then `context.write(new Text(route), new DoubleWritable(price));`",
      "Reducer: `double cheapest = Double.MAX_VALUE;` loop with `Math.min(cheapest, value.get())`. A combiner is safe (min of mins = min)."
    ],
    solution: R`public class CheapestFlightMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    if (fields.length < 5) {
      return;                                     // skip bad lines
    }
    String route = fields[2] + "-" + fields[3];   // origin-dest
    double price = Double.parseDouble(fields[4]);
    context.write(new Text(route), new DoubleWritable(price));
  }
}

public class CheapestFlightReducer
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
    double cheapest = Double.MAX_VALUE;
    for (DoubleWritable value : values) {
      cheapest = Math.min(cheapest, value.get());
    }
    context.write(key, new DoubleWritable(cheapest));
  }
}

// Driver lines that change:
// job.setMapperClass(CheapestFlightMapper.class);
// job.setCombinerClass(CheapestFlightReducer.class);  // safe: min of mins = min
// job.setReducerClass(CheapestFlightReducer.class);
// job.setOutputKeyClass(Text.class);
// job.setOutputValueClass(DoubleWritable.class);`,
    trace: FARE_TRACE,
    rubric: [
      { pts: 1, text: "Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> (or another numeric writable)" },
      { pts: 2, text: "Mapper splits the line on commas and reads origin, dest and price" },
      { pts: 2, text: "Mapper emits key = route, value = price with context.write" },
      { pts: 1, text: "Reducer types match the mapper output" },
      { pts: 3, text: "Reducer keeps the minimum price over all values (MAX_VALUE start or first value; Math.min)" },
      { pts: 1, text: "Reducer writes (route, cheapest); says a combiner is safe because min is associative" }
    ],
    checks: [
      { re: "extends\\s+Mapper\\s*<", label: "Has a Mapper class" },
      { re: "split\\s*\\(\\s*\"\\s*,\\s*\"\\s*\\)", label: "Splits the CSV on commas" },
      { re: "\\[\\s*2\\s*\\][\\s\\S]*\\[\\s*3\\s*\\]", label: "Uses fields 2 and 3 for the route" },
      { re: "parseDouble|DoubleWritable|FloatWritable", label: "Treats price as a decimal number" },
      { re: "Math\\.min|<", label: "Reducer finds a minimum" },
      { re: "[Cc]ombiner", label: "Mentions the combiner" }
    ],
    explain: "\"Write the map statement\": the mapper decides the grouping. Route is the key because we want one answer per route. If the question means the single cheapest flight overall, emit a constant key (e.g. `new Text(\"cheapest\")`) with value `route + \",\" + price`, and have the reducer keep the line with the lowest price.",
    src: "Notes Sep 28, MR slides 9-13" },
  { id: "java-median", track: "java", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: median of a vector (Java)",
    lang: "java",
    prompt: "The prof's example: \"Compute the median of this vector using MapReduce.\" The input has one integer per line. Using the Java API, write the Mapper and Reducer. Then explain what goes in map vs reduce and whether you need a combiner.",
    data: R`7
3
9
1
5
8`,
    expected: R`median	6.0`,
    starter: R`public class MedianMapper
    extends Mapper<…> {

}

public class MedianReducer
    extends Reducer<…> {

}

// map does:
// reduce does:
// combiner?`,
    hints: [
      "A median needs ALL the numbers in one place. How do you make every number reach the same reducer? Give them all the same key.",
      "Mapper: parse the line to an int and `context.write(new Text(\"median\"), new IntWritable(n));`",
      "Reducer: copy every `value.get()` into an `ArrayList<Integer>`, `Collections.sort` it, and take the middle (average the two middle values if the size is even). No combiner: the median of medians is not the median."
    ],
    solution: R`public class MedianMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final Text KEY = new Text("median");   // same key for every number

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.isEmpty()) {
      return;
    }
    int number = Integer.parseInt(line);
    context.write(KEY, new IntWritable(number));
  }
}

public class MedianReducer
    extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    List<Integer> numbers = new ArrayList<Integer>();
    for (IntWritable value : values) {
      numbers.add(value.get());     // copy the int (Hadoop reuses the IntWritable object)
    }
    Collections.sort(numbers);
    int n = numbers.size();
    double median;
    if (n % 2 == 1) {
      median = numbers.get(n / 2);
    } else {
      median = (numbers.get(n / 2 - 1) + numbers.get(n / 2)) / 2.0;
    }
    context.write(key, new DoubleWritable(median));
  }
}

// map: only parses and emits every number under ONE constant key.
// reduce: gets all numbers, sorts them, picks the middle.
// No combiner: median of [1,2,3] is 2 and of [4,100] is 52, but the real median of all five is 3.
// Driver: job.setNumReduceTasks(1) (the default), no setCombinerClass.`,
    trace: [
      { phase: "input", label: "Input lines", rows: ["7", "3", "9", "1", "5", "8"] },
      { phase: "map", label: "Map emits (median, n)", rows: ["(median, 7)", "(median, 3)", "(median, 9)", "(median, 1)", "(median, 5)", "(median, 8)"] },
      { phase: "shuffle", label: "Shuffle & sort", rows: ["(median, [7, 3, 9, 1, 5, 8])"] },
      { phase: "reduce", label: "Reduce: sort, take middle", rows: ["sorted = [1, 3, 5, 7, 8, 9]", "n = 6 (even) → (5 + 7) / 2", "(median, 6.0)"] }
    ],
    rubric: [
      { pts: 2, text: "Mapper parses each line to a number" },
      { pts: 2, text: "Mapper emits every number under the same constant key, so all values reach one reducer" },
      { pts: 2, text: "Reducer collects all values into a list (using value.get())" },
      { pts: 2, text: "Reducer sorts and picks the middle, handling an even count (average of the two middle values)" },
      { pts: 2, text: "Explains no combiner: the median of partial medians is not the overall median" }
    ],
    checks: [
      { re: "new\\s+Text\\s*\\(\\s*\"", label: "Uses a constant Text key" },
      { re: "parseInt|parseDouble|parseLong", label: "Parses the number" },
      { re: "List|ArrayList|\\[\\]", label: "Collects values into a list" },
      { re: "sort", label: "Sorts the values" },
      { re: "/\\s*2|%\\s*2", label: "Finds the middle" },
      { re: "[Cc]ombiner", label: "Discusses the combiner" }
    ],
    explain: "Sorting a Java list isn't on the slides (no slide computes a median), so the marks are in the MapReduce logic. This is the prof's \"what goes in map, what goes in reduce, do you need a combiner\" question. Map = prepare + choose the key. Reduce = the part that needs to see the whole group. Combiner = only when partial results combine into the right answer (not for medians or plain averages).",
    src: "Notes Oct 7, MR slides 4, 12-13" },
  { id: "java-avg-temp", track: "java", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: average temperature per year (Java)",
    lang: "java",
    prompt: "Using the Java MapReduce API, compute the **average valid temperature for each year** from NCDC records. Write the Mapper and Reducer (start from `public class`). Can the reducer be reused as a combiner? Explain.",
    data: NCDC_SAMPLE,
    expected: R`1949	94.5
1950	3.6666666666666665`,
    starter: R`public class AvgTemperatureMapper
    extends Mapper<…> {

}

public class AvgTemperatureReducer
    extends Reducer<…> {

}

// Combiner?`,
    hints: [
      "The mapper is the same as slide 9.",
      "Reducer: sum the values AND count them, then write `(double) sum / count`. The output value type is DoubleWritable.",
      "Combiner: no. The average of averages isn't the overall average, unless you pass (sum, count) pairs."
    ],
    solution: R`public class AvgTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature;
    if (line.charAt(87) == '+') {
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(airTemperature));
    }
  }
}

public class AvgTemperatureReducer
    extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    int count = 0;
    for (IntWritable value : values) {
      sum += value.get();
      count++;
    }
    context.write(key, new DoubleWritable((double) sum / count));
  }
}

// Combiner: NOT this reducer. avg(avg(0, 22), avg(-11)) = avg(11, -11) = 0,
// but avg(0, 22, -11) = 3.67. (Its output type DoubleWritable also wouldn't
// match the reducer's IntWritable input.) To use a combiner you'd emit
// (sum, count) pairs and divide only in the reducer.`,
    trace: [
      SLIDE8_TRACE[0], SLIDE8_TRACE[1], SLIDE8_TRACE[2],
      { phase: "reduce", label: "Reduce: sum / count", rows: ["(1949, (111 + 78) / 2 = 94.5)", "(1950, (0 + 22 − 11) / 3 = 3.67)"] }
    ],
    rubric: [
      { pts: 3, text: "Mapper: correct types, extracts year and temperature, filters bad readings, emits (year, temp)" },
      { pts: 1, text: "Reducer types: input (Text, IntWritable), output value DoubleWritable" },
      { pts: 3, text: "Reducer sums and counts all values, then divides as a double" },
      { pts: 1, text: "Writes (year, average)" },
      { pts: 2, text: "Explains the reducer can't be the combiner (average of averages is wrong; (sum, count) fix)" }
    ],
    checks: [
      { re: "extends\\s+Mapper\\s*<", label: "Has a Mapper class" },
      { re: "extends\\s+Reducer\\s*<", label: "Has a Reducer class" },
      { re: "count\\s*\\+\\+|count\\s*\\+=", label: "Counts the values" },
      { re: "sum\\s*\\+=|total\\s*\\+=", label: "Sums the values" },
      { re: "\\(\\s*double\\s*\\)|DoubleWritable|/\\s*\\(double\\)", label: "Divides as a double" },
      { re: "[Cc]ombiner", label: "Answers the combiner question" }
    ],
    explain: "Compare with max: the reducer logic is tiny, and the marks are in getting the types right and explaining the combiner.",
    src: "MR slides 9-13" },

  /* ===================== PySpark RDDs ===================== */
  { id: "ps-flatmap-read", track: "pyspark", level: 1, type: "mcq", title: "flatMap then count",
    lang: "python", code: R`lines = sc.parallelize(["big data", "big spark"])
words = lines.flatMap(lambda line: line.split())
print(words.count())`,
    q: "What is printed?",
    options: ["4", "2", "3", "An error, because count() is a transformation"],
    answer: 0,
    explain: "flatMap flattens each line's word list: [\"big\", \"data\", \"big\", \"spark\"], so 4 elements. With `map` you'd get 2 elements (two lists). count() is an action and returns a number to the driver.",
    src: "Spark slide 12" },
  { id: "ps-which-runs", track: "pyspark", level: 1, type: "mcq", title: "When does the job start?",
    lang: "python", code: R`text = sc.textFile("shakespeare.txt")        # line 1
words = text.flatMap(tokenize)              # line 2
wc = words.map(lambda x: (x, 1))            # line 3
counts = wc.reduceByKey(add)                # line 4
counts.saveAsTextFile("wc")                 # line 5`,
    q: "On which line does Spark actually start running a job on the cluster?",
    options: ["Line 5 (saveAsTextFile)", "Line 1 (textFile)", "Line 4 (reduceByKey)", "Every line runs immediately"],
    answer: 0,
    explain: "Lines 1-4 only build the lineage (lazy evaluation). saveAsTextFile is an action, so it kicks off the distributed job. Slide 13's text loosely calls reduceByKey an action, but it returns an RDD, so it's a transformation.",
    src: "Spark slides 7, 13" },
  { id: "ps-reducebykey-read", track: "pyspark", level: 1, type: "mcq", title: "What does reduceByKey return?",
    lang: "python", code: R`from operator import add
pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)])
print(pairs.reduceByKey(add).collect())`,
    q: "What is printed (ignoring order)?",
    options: ["[('a', 4), ('b', 6), ('c', 5)]", "[('a', [1, 3]), ('b', [2, 4]), ('c', [5])]", "15", "[('a', 1), ('b', 2), ('a', 3), ('b', 4), ('c', 5)]"],
    answer: 0,
    explain: "reduceByKey combines the values of each key with the function (add), like a reducer that sums per key. `reduce(add)` on plain numbers would give one value instead.",
    src: "Spark slide 13" },
  { id: "ps-actions-read", track: "pyspark", level: 1, type: "mcq", title: "Spot the actions",
    lang: "python", code: R`# Which of these trigger a Spark job?`,
    q: "Which list contains ONLY actions?",
    options: ["collect, count, saveAsTextFile, reduce", "map, filter, flatMap", "reduceByKey, collect, map", "textFile, parallelize, count"],
    answer: 0,
    explain: "Actions return a value to the driver or write output. map, filter, flatMap and reduceByKey are transformations. textFile and parallelize just define an RDD.",
    src: "Spark slides 7, 9" },
  { id: "ps-wc-fill", track: "pyspark", level: 2, type: "fill", title: "Fill in PySpark word count",
    lang: "python", prompt: "Complete the word count from slides 12-13.",
    code: R`from operator import add

text = sc.⟦textFile⟧("shakespeare.txt")

def tokenize(text):
    return text.⟦split⟧()

words = text.⟦flatMap⟧(tokenize)
wc = words.⟦map⟧(lambda x: (x, ⟦1⟧))
counts = wc.⟦reduceByKey⟧(add)
counts.⟦saveAsTextFile⟧("wc")`,
    explain: "Load → flatMap into words → map each word to (word, 1) → reduceByKey(add) sums per word → save (the action that runs it all).",
    src: "Spark slides 12-13" },
  { id: "ps-airline-fill", track: "pyspark", level: 2, type: "fill", title: "Fill in the airline delay app",
    lang: "python", prompt: "Complete the airline delays app from slides 15-16.",
    code: R`conf = SparkConf().setMaster("local[*]")
conf = conf.setAppName(APP_NAME)
sc   = ⟦SparkContext⟧(conf=conf)

# Load the airlines lookup dictionary and broadcast it to the cluster
airlines = dict(sc.textFile("ontime/airlines.csv").map(split).⟦collect⟧())
airline_lookup = sc.⟦broadcast⟧(airlines)

# Read the CSV data into an RDD of Flight named tuples
flights = sc.textFile("ontime/flights.csv").map(split).map(⟦parse⟧)

# Map the total delay to the airline name
delays = flights.map(lambda f: (airline_lookup.⟦value⟧[f.airline],
                                add(f.dep_delay, f.arv_delay)))

# Reduce the total delay per airline, then sort on the driver
delays = delays.⟦reduceByKey⟧(add).collect()
delays = sorted(delays, key=itemgetter(⟦1⟧))`,
    explain: "`collect()` brings the small airlines table to the driver so `dict()` can build a lookup, and `broadcast` ships it to every node; read it with `.value`. `itemgetter(1)` sorts by total delay.",
    src: "Spark slides 15-16" },
  { id: "ps-ncdc-parsons", track: "pyspark", level: 3, type: "parsons", title: "Arrange: max temperature per year in PySpark",
    lang: "python", prompt: "Build the RDD version of the max temperature job. Two lines are distractors.",
    lines: [
      [R`lines = sc.textFile("ncdc.txt")`, 0],
      [R`records = lines.map(lambda line: (line[15:19], int(line[87:92]), line[92]))`, 1],
      [R`valid = records.filter(lambda r: r[1] != 9999 and r[2] in "01459")`, 2],
      [R`pairs = valid.map(lambda r: (r[0], r[1]))`, 3],
      [R`maxTemps = pairs.reduceByKey(lambda a, b: max(a, b))`, 4],
      [R`for year, temp in sorted(maxTemps.collect()):`, 5],
      [R`    print(year, temp)`, 6]
    ],
    distractors: [R`maxTemps = pairs.reduce(lambda a, b: max(a, b))`, R`valid = records.map(lambda r: r[1] != 9999)`],
    explain: "map = parse (the mapper's job), filter = drop bad records, map to (year, temp) pairs, reduceByKey = the reducer, collect = action. `reduce` would give one value for ALL years, and `map` with a condition gives True/False instead of filtering.",
    src: "Spark slides 7-9, MR slides 7-10" },
  { id: "ps-total-delay-write", track: "pyspark", level: 4, type: "write", title: "Write: worst 5 destination airports",
    lang: "python",
    prompt: "`flights` is an RDD of Flight named tuples with fields `date, airline, flightnum, origin, dest, dep, dep_delay, arv, arv_delay, airtime, distance` (delays are floats). Compute the **total arrival delay per destination airport** and print the 5 airports with the largest totals.",
    data: R`Flight(date=..., airline='19805', origin='JFK', dest='LAX', ..., arv_delay=2.0, ...)
Flight(date=..., airline='19805', origin='LAX', dest='JFK', ..., arv_delay=-29.0, ...)`,
    expected: R`LAX	512 minutes
ORD	430 minutes
...`,
    starter: R`from operator import add, itemgetter

# flights is already loaded (slide 16)
`,
    hints: [
      "Key = destination (`f.dest`), value = `f.arv_delay`.",
      "`totals = flights.map(lambda f: (f.dest, f.arv_delay)).reduceByKey(add)`",
      "Slide 16 style: `collect()` the totals, then sort on the driver with `sorted(..., key=itemgetter(1), reverse=True)` and keep `[:5]`."
    ],
    solution: R`from operator import add, itemgetter

delays = flights.map(lambda f: (f.dest, f.arv_delay))
totals = delays.reduceByKey(add).collect()
worst = sorted(totals, key=itemgetter(1), reverse=True)[:5]

for airport, minutes in worst:
    print("%s\t%0.f minutes" % (airport, minutes))`,
    rubric: [
      { pts: 2, text: "Maps each flight to (dest, arrival delay)" },
      { pts: 2, text: "Uses reduceByKey(add) (or equivalent) to total per airport" },
      { pts: 2, text: "Collects, sorts descending by total on the driver (sorted + itemgetter(1)) and keeps 5" },
      { pts: 1, text: "Prints the results" }
    ],
    checks: [
      { re: "\\.dest|\\[\\s*4\\s*\\]", label: "Uses the destination as the key" },
      { re: "arv_delay|\\[\\s*8\\s*\\]", label: "Uses arrival delay as the value" },
      { re: "reduceByKey", label: "Aggregates with reduceByKey" },
      { re: "take\\s*\\(\\s*5|\\[\\s*:\\s*5\\s*\\]|takeOrdered", label: "Keeps 5 results" }
    ],
    explain: "Same shape as slide 16 with a different key and value. Sorting after collect() with sorted(..., key=itemgetter(1)) is exactly how the slide sorts. (RDD sortBy/take also work but aren't on the slides.)",
    src: "Spark slide 16" },
  { id: "ps-errors-write", track: "pyspark", level: 4, type: "write", title: "Write: count and save error lines",
    lang: "python",
    prompt: "Load `server.log`. Keep only the lines that contain the word `error`, print how many there are, and save them to the folder `errors`. The filtered RDD is used by two actions, so cache it.",
    data: R`INFO started
error disk full
INFO ok
error timeout`,
    expected: R`2
(and folder errors/ with part-00000 ...)`,
    starter: R`logs = sc.textFile("server.log")
`,
    hints: [
      "`filter` keeps elements where the function returns True: `lambda line: \"error\" in line`.",
      "`errors.cache()` keeps the RDD in memory so the second action doesn't re-read the file.",
      "`count()` and `saveAsTextFile(\"errors\")` are the two actions."
    ],
    solution: R`logs = sc.textFile("server.log")
errors = logs.filter(lambda line: "error" in line)
errors.cache()                     # reused by two actions below
print(errors.count())
errors.saveAsTextFile("errors")`,
    rubric: [
      { pts: 3, text: "Uses filter with a condition that keeps lines containing \"error\"" },
      { pts: 1, text: "Caches the filtered RDD" },
      { pts: 2, text: "count() to print the number of error lines" },
      { pts: 1, text: "saveAsTextFile(\"errors\")" }
    ],
    checks: [
      { re: "filter\\s*\\(", label: "Uses filter" },
      { re: "[\"']error[\"']\\s+in", label: "Checks for \"error\"" },
      { re: "cache\\s*\\(|persist\\s*\\(", label: "Caches the RDD" },
      { re: "count\\s*\\(", label: "Counts" },
      { re: "saveAsTextFile", label: "Saves the result" }
    ],
    explain: "Without cache(), each action re-runs the whole lineage from the file. Caching is why Spark is fast for reuse and iteration.",
    src: "Spark slides 6-9" },
  { id: "ps-cheapest", track: "pyspark", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: cheapest flight per route (PySpark)",
    lang: "python",
    prompt: "Using PySpark RDDs, find the **cheapest price for each route** (origin-destination) in `fares.csv` (`date,airline,origin,dest,price`). Print each route and its cheapest price, sorted by route. `sc` already exists.",
    data: FARE_SAMPLE,
    expected: R`YVR-YYZ 310.0
YYZ-YUL 99.0
YYZ-YVR 249.0`,
    starter: R`lines = sc.textFile("fares.csv")
`,
    hints: [
      "Split each line on \",\" with map.",
      "Map each row to a pair (route, price): `(r[2] + \"-\" + r[3], float(r[4]))`.",
      "`reduceByKey(lambda a, b: min(a, b))`, then `collect()` and sort on the driver."
    ],
    solution: R`lines = sc.textFile("fares.csv")
rows = lines.map(lambda line: line.split(","))
routes = rows.map(lambda r: (r[2] + "-" + r[3], float(r[4])))
cheapest = routes.reduceByKey(lambda a, b: min(a, b))

for route, price in sorted(cheapest.collect()):
    print(route, price)`,
    trace: [
      { phase: "input", label: "textFile", rows: ["\"…,AC,YYZ,YVR,289.50\"", "\"…,WS,YYZ,YVR,249.00\"", "\"…,AC,YYZ,YUL,129.99\"", "\"…,PD,YYZ,YUL,99.00\"", "\"…,WS,YVR,YYZ,310.00\""] },
      { phase: "map", label: "map → (route, price)", rows: ["(YYZ-YVR, 289.5)", "(YYZ-YVR, 249.0)", "(YYZ-YUL, 129.99)", "(YYZ-YUL, 99.0)", "(YVR-YYZ, 310.0)"] },
      { phase: "reduce", label: "reduceByKey(min)", rows: ["(YYZ-YVR, 249.0)", "(YYZ-YUL, 99.0)", "(YVR-YYZ, 310.0)"] },
      { phase: "output", label: "collect + sorted (driver)", rows: ["YVR-YYZ 310.0", "YYZ-YUL 99.0", "YYZ-YVR 249.0"] }
    ],
    rubric: [
      { pts: 2, text: "Loads the file with sc.textFile and splits lines on commas" },
      { pts: 3, text: "Maps to (route, price) pairs with price converted to float" },
      { pts: 3, text: "reduceByKey keeping the minimum" },
      { pts: 2, text: "Uses an action (collect/saveAsTextFile) and outputs sorted results" }
    ],
    checks: [
      { re: "split\\s*\\(\\s*[\"'],[\"']\\s*\\)", label: "Splits on commas" },
      { re: "float\\s*\\(", label: "Converts price to float" },
      { re: "reduceByKey", label: "Uses reduceByKey" },
      { re: "min\\s*\\(|<", label: "Keeps the minimum" },
      { re: "collect\\s*\\(|saveAsTextFile|take\\s*\\(", label: "Ends with an action" }
    ],
    explain: "Compare with the Java and streaming versions: the map step chooses (key, value) and reduceByKey replaces the whole reducer class.",
    src: "Spark slides 12-16" },
  { id: "ps-avg-temp", track: "pyspark", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: average temperature per year (PySpark)",
    lang: "python",
    prompt: "Using PySpark RDDs, compute the **average valid temperature per year** from `ncdc.txt` and save it to `avg_temps`. " + NCDC_NOTE,
    data: NCDC_SAMPLE,
    expected: R`('1949', 94.5)
('1950', 3.6666666666666665)`,
    starter: R`lines = sc.textFile("ncdc.txt")
`,
    hints: [
      "Parse and filter exactly like the max temperature job.",
      "You can't average with a simple reduceByKey(add). Carry (sum, count): map to `(year, (temp, 1))`.",
      "`reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))`, then map each `(year, (s, c))` to `(year, s / float(c))`."
    ],
    solution: R`lines = sc.textFile("ncdc.txt")
records = lines.map(lambda line: (line[15:19], int(line[87:92]), line[92]))
valid = records.filter(lambda r: r[1] != 9999 and r[2] in "01459")

pairs = valid.map(lambda r: (r[0], (r[1], 1)))                     # (year, (temp, 1))
sums = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # (year, (sum, count))
averages = sums.map(lambda kv: (kv[0], kv[1][0] / float(kv[1][1])))
# (mapValues would also work, but it isn't on the slides)

averages.saveAsTextFile("avg_temps")`,
    trace: [
      { phase: "map", label: "map + filter → (year, (temp, 1))", rows: ["(1950, (0, 1))", "(1950, (22, 1))", "(1950, (−11, 1))", "(1949, (111, 1))", "(1949, (78, 1))"] },
      { phase: "reduce", label: "reduceByKey: add sums and counts", rows: ["(1950, (11, 3))", "(1949, (189, 2))"] },
      { phase: "output", label: "map: sum / count", rows: ["(1950, 3.67)", "(1949, 94.5)"] }
    ],
    rubric: [
      { pts: 2, text: "Parses year and temperature from each line" },
      { pts: 2, text: "Filters missing and bad-quality readings" },
      { pts: 3, text: "Aggregates sum AND count per year (pairs with reduceByKey, or groupByKey then compute)" },
      { pts: 2, text: "Divides sum by count to get the average" },
      { pts: 1, text: "Saves or collects the result (an action)" }
    ],
    checks: [
      { re: "15\\s*:\\s*19", label: "Extracts the year" },
      { re: "filter\\s*\\(", label: "Filters bad records" },
      { re: "reduceByKey|groupByKey|aggregateByKey", label: "Aggregates per key" },
      { re: ",\\s*1\\s*\\)", label: "Carries a count of 1" },
      { re: "/", label: "Divides to get the average" }
    ],
    explain: "The (sum, count) trick is also how you'd make a MapReduce combiner work for averages.",
    src: "Spark slides 7-9, MR slide 13" },
  { id: "ps-top-words", track: "pyspark", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: top 10 words (PySpark)",
    lang: "python",
    prompt: "Using PySpark RDDs, print the **10 most frequent words** in `shakespeare.txt` (lowercase them first) with their counts, most frequent first.",
    data: R`To be or not to be
That is the question`,
    expected: R`to 2
be 2
...`,
    starter: R`from operator import add

text = sc.textFile("shakespeare.txt")
`,
    hints: [
      "Start with the slide 12-13 word count.",
      "Lowercase inside flatMap: `lambda line: line.lower().split()`.",
      "Slide 16 style: `collect()`, then `sorted(..., key=itemgetter(1), reverse=True)[:10]` on the driver."
    ],
    solution: R`from operator import add, itemgetter

text = sc.textFile("shakespeare.txt")
words = text.flatMap(lambda line: line.lower().split())
wc = words.map(lambda x: (x, 1))
counts = wc.reduceByKey(add).collect()
top10 = sorted(counts, key=itemgetter(1), reverse=True)[:10]

for word, n in top10:
    print(word, n)`,
    rubric: [
      { pts: 2, text: "flatMap splits lines into lowercase words" },
      { pts: 2, text: "map to (word, 1)" },
      { pts: 2, text: "reduceByKey(add) for counts" },
      { pts: 3, text: "Collects, sorts by count descending (sorted + itemgetter(1), reverse=True) and keeps 10" },
      { pts: 1, text: "Prints word and count" }
    ],
    checks: [
      { re: "flatMap", label: "Uses flatMap" },
      { re: "lower\\s*\\(", label: "Lowercases" },
      { re: "reduceByKey", label: "Counts with reduceByKey" },
      { re: "take\\s*\\(\\s*10|takeOrdered\\s*\\(\\s*10|\\[\\s*:\\s*10\\s*\\]", label: "Takes 10" },
      { re: "reverse\\s*=\\s*True|ascending\\s*=\\s*False", label: "Sorts descending" }
    ],
    explain: "Word count from slides 12-13 plus the slide 16 sort. collect() is the action that triggers the job.",
    src: "Spark slides 12-13" },

  /* ===================== Spark SQL & DataFrames ===================== */
  { id: "sql-having-read", track: "sql", level: 1, type: "mcq", title: "What does HAVING do here?",
    lang: "sql", code: R`SELECT primetype, secondtype,
       count(1) AS count,
       round(avg(regcap), 0) AS avg_spaces
FROM parking
GROUP BY primetype, secondtype
HAVING trim(primetype) != ''
ORDER BY count DESC`,
    q: "What does `HAVING trim(primetype) != ''` do?",
    options: ["After grouping, removes the groups whose primetype is blank", "Removes rows before grouping (exactly what WHERE means)", "Trims spaces from primetype in the output", "Sorts groups by primetype"],
    answer: 0,
    explain: "HAVING filters GROUPS after GROUP BY. WHERE filters rows before grouping. The DataFrame version on slide 23 uses `.where(...)` before `groupBy`, which gives the same result here.",
    src: "Spark slides 21, 23" },
  { id: "sql-df-equiv", track: "sql", level: 1, type: "mcq", title: "SQL ↔ DataFrame",
    lang: "sql", code: R`SELECT Country, avg(AverageTemperature) AS avg_temp
FROM weather
GROUP BY Country
ORDER BY avg_temp DESC`,
    q: "Which DataFrame chain gives the same result?",
    options: ["df.groupBy(\"Country\").agg(F.avg(\"AverageTemperature\").alias(\"avg_temp\")).sort(\"avg_temp\", ascending=False)", "df.groupBy(\"Country\").agg(F.avg(\"AverageTemperature\").alias(\"avg_temp\")).sort(\"avg_temp\")", "df.select(\"Country\").avg(\"AverageTemperature\").orderBy(\"avg_temp\")", "df.groupBy(\"AverageTemperature\").agg(F.avg(\"Country\"))"],
    answer: 0,
    explain: "GROUP BY → groupBy, aggregate AS alias → agg(F.avg(...).alias(...)), ORDER BY … DESC → sort(..., ascending=False) as on slide 23. Option B sorts ascending (the default).",
    src: "Spark slides 21, 23" },
  { id: "sql-slide21-fill", track: "sql", level: 2, type: "fill", ci: true, title: "Fill in the slide 21 query",
    lang: "python", prompt: "Complete the Spark SQL aggregation from slide 21 (not case sensitive).",
    code: R`aggr_by_type = sqlContext.⟦sql⟧("SELECT primetype, secondtype, "
                             "⟦count⟧(1) AS count, "
                             "round(⟦avg⟧(regcap), 0) AS avg_spaces "
                             "FROM ⟦parking⟧ "
                             "⟦GROUP BY⟧ primetype, secondtype "
                             "⟦HAVING⟧ trim(primetype) != '' "
                             "ORDER BY count ⟦DESC⟧")

aggr_by_type.⟦show⟧()`,
    explain: "sql() runs the query on the registered table `parking`. count(1) = rows per group, avg + round for average spaces, HAVING drops blank types, DESC for the highest counts first, show() prints it.",
    src: "Spark slide 21" },
  { id: "sql-load-fill", track: "sql", level: 2, type: "fill", title: "Fill in: load data and register a table",
    lang: "python", prompt: "Load JSON (slide 20) and CSV (the lab), then register tables for SQL.",
    code: R`# Slide 20: JSON
sqlContext = SQLContext(sc)
parking = sqlContext.read.⟦json⟧("../data/sf_parking/sf_parking_clean.json")
parking.⟦registerTempTable|createOrReplaceTempView⟧("parking")

# Lab: CSV with a header row and real column types
df = spark.read.⟦csv⟧(file_path, ⟦header⟧=True, ⟦inferSchema⟧=True)
df.⟦printSchema⟧()
df.createOrReplaceTempView("weather")
spark.sql("SELECT count(*) FROM weather").⟦show⟧()`,
    explain: "A DataFrame must be registered as a table/view before SQL can reference it by name. header=True uses the first row as column names; inferSchema=True detects types.",
    src: "Spark slide 20, Lab" },
  { id: "sql-df-fill", track: "sql", level: 2, type: "fill", title: "Fill in the DataFrame API version (slide 23)",
    lang: "python", prompt: "Complete the DataFrame API chain that matches the slide 21 SQL.",
    code: R`from pyspark.sql import functions as F

aggr_by_type = parking.⟦select⟧("primetype", "secondtype", "regcap") \
                      .⟦where|filter⟧("trim(primetype) != ''") \
                      .⟦groupBy⟧("primetype", "secondtype") \
                      .⟦agg⟧(
                          F.⟦count⟧("*").alias("count"),
                          F.round(F.avg("regcap"), 0).⟦alias⟧("avg_spaces")
                      ) \
                      .sort("count", ascending=⟦False⟧)`,
    explain: "select columns → where (filter rows) → groupBy → agg with named aggregates → sort descending. You can test each step as you chain, which is the advantage the slide mentions.",
    src: "Spark slide 23" },
  { id: "sql-clause-parsons", track: "sql", level: 3, type: "parsons", title: "Arrange the SQL clauses",
    lang: "sql", prompt: "Build a query for the 10 hottest countries since 1900 that have more than 100 readings. Two lines are wrong; leave them out.",
    lines: [
      [R`SELECT Country, round(avg(AverageTemperature), 2) AS avg_temp`, 0],
      [R`FROM weather`, 1],
      [R`WHERE Year >= 1900`, 2],
      [R`GROUP BY Country`, 3],
      [R`HAVING count(*) > 100`, 4],
      [R`ORDER BY avg_temp DESC`, 5],
      [R`LIMIT 10`, 6]
    ],
    distractors: [R`WHERE avg_temp > 20`, R`GROUP BY avg_temp`],
    explain: "SELECT → FROM → WHERE (rows) → GROUP BY → HAVING (groups) → ORDER BY → LIMIT. WHERE can't use an aggregate like avg_temp; that belongs in HAVING.",
    src: "Spark slide 21" },
  { id: "sql-weather-sql", track: "sql", level: 4, type: "write", title: "Write SQL: hottest countries since 1900",
    lang: "sql",
    prompt: "Table `weather` has columns `dt, AverageTemperature, AverageTemperatureUncertainty, City, Country, Latitude, Longitude, Year`. Write a Spark SQL query for the **10 countries with the highest average temperature since 1900**, highest first.",
    data: R`dt         | AverageTemperature | City     | Country | Year
1901-01-01 | 26.1               | Abidjan  | Côte D'Ivoire | 1901
1901-01-01 | -8.3               | Moscow   | Russia  | 1901`,
    expected: R`Country | avg_temp
Djibouti | 29.1
Mali     | 28.4
...`,
    starter: R`spark.sql("""

""").show()`,
    hints: [
      "You need: SELECT … FROM weather WHERE … GROUP BY … ORDER BY … LIMIT …",
      "Filter rows with `WHERE Year >= 1900` (rows, before grouping), then `GROUP BY Country`.",
      "`SELECT Country, avg(AverageTemperature) AS avg_temp` … `ORDER BY avg_temp DESC LIMIT 10`."
    ],
    solution: R`spark.sql("""
    SELECT Country, avg(AverageTemperature) AS avg_temp
    FROM weather
    WHERE Year >= 1900
    GROUP BY Country
    ORDER BY avg_temp DESC
    LIMIT 10
""").show()`,
    rubric: [
      { pts: 2, text: "SELECT Country and avg(AverageTemperature) with an alias" },
      { pts: 2, text: "WHERE Year >= 1900 (a row filter, before grouping)" },
      { pts: 2, text: "GROUP BY Country" },
      { pts: 2, text: "ORDER BY the average DESC and LIMIT 10" }
    ],
    checks: [
      { re: "avg\\s*\\(\\s*AverageTemperature", label: "Averages AverageTemperature", flags: "i" },
      { re: "where[\\s\\S]*1900", label: "Filters Year >= 1900 with WHERE", flags: "i" },
      { re: "group\\s+by\\s+country", label: "Groups by Country", flags: "i" },
      { re: "desc", label: "Sorts descending", flags: "i" },
      { re: "limit\\s+10", label: "Limits to 10", flags: "i" }
    ],
    explain: "If you wrote HAVING Year >= 1900 it would fail or be wrong: Year isn't a grouped column. Row conditions go in WHERE.",
    src: "Spark slide 21, Lab" },
  { id: "sql-weather-df", track: "sql", level: 4, type: "write", title: "Write the same query with the DataFrame API",
    lang: "python",
    prompt: "Same question as before (10 hottest countries since 1900), but with the DataFrame API on `df`. Use `from pyspark.sql import functions as F`.",
    data: R`df columns: dt, AverageTemperature, AverageTemperatureUncertainty, City, Country, Latitude, Longitude, Year`,
    expected: R`+--------+--------+
| Country|avg_temp|
+--------+--------+
|Djibouti|    29.1|
...`,
    starter: R`from pyspark.sql import functions as F

result = df
result.show()`,
    hints: [
      "Order of methods: filter → groupBy → agg → sort → show.",
      "`df.filter(df[\"Year\"] >= 1900).groupBy(\"Country\")`",
      "`.agg(F.avg(\"AverageTemperature\").alias(\"avg_temp\")).sort(\"avg_temp\", ascending=False)` then `.show(10)` (slide 23 style)."
    ],
    solution: R`from pyspark.sql import functions as F

result = df.filter(df["Year"] >= 1900) \
           .groupBy("Country") \
           .agg(F.avg("AverageTemperature").alias("avg_temp")) \
           .sort("avg_temp", ascending=False)
result.show(10)`,
    rubric: [
      { pts: 2, text: "filter/where on Year >= 1900" },
      { pts: 2, text: "groupBy(\"Country\")" },
      { pts: 2, text: "agg with avg(AverageTemperature) and an alias (or .avg())" },
      { pts: 2, text: "Sort descending (sort(..., ascending=False)) and show 10" }
    ],
    checks: [
      { re: "filter\\s*\\(|where\\s*\\(", label: "Filters the years" },
      { re: "groupBy\\s*\\(", label: "Uses groupBy" },
      { re: "avg", label: "Averages" },
      { re: "desc|ascending\\s*=\\s*False", label: "Sorts descending" },
      { re: "limit\\s*\\(|show\\s*\\(\\s*10", label: "Keeps 10" }
    ],
    explain: "Line up the two versions: WHERE ↔ filter/where, GROUP BY ↔ groupBy, avg(...) AS x ↔ agg(F.avg(...).alias(\"x\")), ORDER BY … DESC ↔ sort(\"x\", ascending=False), LIMIT 10 ↔ show(10).",
    src: "Spark slide 23" },
  { id: "sql-ny-pipeline", track: "sql", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: New York average per year (lab pipeline)",
    lang: "python",
    prompt: "From the lab: load `GlobalLandTemperaturesByCity.csv` (it has a header row) with real column types, drop rows where `AverageTemperature` is null, add a `Year` column from the `dt` date, and show the **average temperature per year for New York**, oldest year first. Use PySpark DataFrames (SQL is fine too if you register a view).",
    data: R`dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
1743-11-01,3.264,1.665,New York,United States,40.99N,74.56W
1744-04-01,9.788,3.624,New York,United States,40.99N,74.56W`,
    expected: R`+----+-----------------------+
|Year|avg(AverageTemperature)|
+----+-----------------------+
|1743|                  3.264|
|1744|     13.709142857142856|
...`,
    starter: R`from pyspark.sql.functions import col, year

file_path = "/Volumes/workspace/default/test/GlobalLandTemperaturesByCity.csv"
`,
    hints: [
      "`spark.read.csv(file_path, header=True, inferSchema=True)`",
      "`df.na.drop(subset=[\"AverageTemperature\"])` then `withColumn(\"Year\", year(col(\"dt\")))`.",
      "`filter(df[\"City\"] == \"New York\").groupBy(\"Year\").avg(\"AverageTemperature\").orderBy(\"Year\").show()`"
    ],
    solution: R`from pyspark.sql.functions import col, year

file_path = "/Volumes/workspace/default/test/GlobalLandTemperaturesByCity.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)
df = df.na.drop(subset=["AverageTemperature"])
df = df.withColumn("Year", year(col("dt")))

ny = df.filter(df["City"] == "New York") \
       .groupBy("Year") \
       .avg("AverageTemperature") \
       .orderBy("Year")
ny.show(10)

# Same thing in Spark SQL:
# df.createOrReplaceTempView("weather")
# spark.sql("""SELECT Year, avg(AverageTemperature) AS avg_temp
#              FROM weather WHERE City = 'New York'
#              GROUP BY Year ORDER BY Year""").show(10)`,
    trace: [
      { phase: "rdd", label: "read.csv", rows: ["8,599,212 rows, typed columns"] },
      { phase: "map", label: "na.drop + withColumn(Year)", rows: ["rows without null temperature", "+ Year column (1743, 1744, …)"] },
      { phase: "shuffle", label: "filter(New York) + groupBy(Year)", rows: ["1743 → [3.264]", "1744 → [9.788, …]"] },
      { phase: "reduce", label: "avg + orderBy(Year)", rows: ["(1743, 3.264)", "(1744, 13.709)", "…"] }
    ],
    rubric: [
      { pts: 2, text: "Reads the CSV with header=True and inferSchema=True" },
      { pts: 2, text: "Drops rows with a null AverageTemperature (na.drop / dropna with subset)" },
      { pts: 2, text: "Adds Year from dt (withColumn + year())" },
      { pts: 2, text: "Filters City == \"New York\"" },
      { pts: 2, text: "Groups by Year, averages AverageTemperature, orders by Year and shows" }
    ],
    checks: [
      { re: "read\\.csv|format\\s*\\(\\s*[\"']csv", label: "Reads the CSV" },
      { re: "header\\s*=\\s*True|\"header\"\\s*,\\s*\"true\"", label: "Uses the header row" },
      { re: "inferSchema", label: "Infers types" },
      { re: "na\\.drop|dropna", label: "Drops null temperatures" },
      { re: "withColumn\\s*\\(\\s*[\"']Year", label: "Adds a Year column" },
      { re: "New York", label: "Filters New York" },
      { re: "groupBy\\s*\\(\\s*[\"']Year|group\\s+by\\s+year", label: "Groups by Year", flags: "i" },
      { re: "orderBy|order\\s+by", label: "Sorts by year", flags: "i" }
    ],
    explain: "This is the lab notebook in five lines. Know `read.csv` options, `na.drop(subset=...)`, `withColumn`, `filter`, `groupBy().avg()`, `orderBy`.",
    src: "Lab: EDA Weather, Feature Engineering" },
  { id: "sql-flights-sql", track: "sql", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: busy airports with the worst delays (Spark SQL)",
    lang: "sql",
    prompt: "The DataFrame `flights` (columns `date, airline, flightnum, origin, dest, dep, dep_delay, arv, arv_delay, airtime, distance`) has been loaded. Register it and write a Spark SQL query that returns, for each **origin** airport, the number of flights and the average departure delay (rounded to 2 decimals), **only for airports with more than 100 flights**, sorted by average delay from worst to best.",
    data: FLIGHT_SAMPLE,
    expected: R`+------+-----------+---------+
|origin|num_flights|avg_delay|
+------+-----------+---------+
|   ORD|       2154|    18.42|
...`,
    starter: R`flights.registerTempTable("flights")

result = sqlContext.sql("""

""")
result.show()`,
    hints: [
      "Group by origin, count rows with count(1) or count(*), and average dep_delay.",
      "\"Only airports with more than 100 flights\" is a condition on a GROUP, so it's HAVING count(1) > 100.",
      "ORDER BY avg_delay DESC."
    ],
    solution: R`flights.registerTempTable("flights")      # Databricks lab: createOrReplaceTempView

result = sqlContext.sql("""                 # Databricks lab: spark.sql
    SELECT origin,
           count(1) AS num_flights,
           round(avg(dep_delay), 2) AS avg_delay
    FROM flights
    GROUP BY origin
    HAVING count(1) > 100
    ORDER BY avg_delay DESC
""")
result.show()

# DataFrame API version:
# from pyspark.sql import functions as F
# flights.groupBy("origin") \
#        .agg(F.count("*").alias("num_flights"),
#             F.round(F.avg("dep_delay"), 2).alias("avg_delay")) \
#        .where("num_flights > 100") \
#        .sort("avg_delay", ascending=False).show()`,
    rubric: [
      { pts: 1, text: "Registers the DataFrame as a table (registerTempTable / createOrReplaceTempView)" },
      { pts: 2, text: "SELECT origin, count(1) AS num_flights" },
      { pts: 2, text: "round(avg(dep_delay), 2) AS avg_delay" },
      { pts: 2, text: "GROUP BY origin" },
      { pts: 2, text: "HAVING count(1) > 100 (not WHERE)" },
      { pts: 1, text: "ORDER BY avg_delay DESC" }
    ],
    checks: [
      { re: "registerTempTable|createOrReplaceTempView|createTempView", label: "Registers the table" },
      { re: "count\\s*\\(", label: "Counts flights", flags: "i" },
      { re: "avg\\s*\\(\\s*dep_delay", label: "Averages dep_delay", flags: "i" },
      { re: "group\\s+by\\s+origin", label: "Groups by origin", flags: "i" },
      { re: "having[\\s\\S]*100", label: "HAVING … > 100", flags: "i" },
      { re: "order\\s+by[\\s\\S]*desc", label: "Orders descending", flags: "i" }
    ],
    explain: "This is slide 21's query pattern: aggregate per group, filter groups with HAVING, sort with ORDER BY … DESC.",
    src: "Spark slides 20-21" },
  { id: "sql-temp-range", track: "sql", level: 5, type: "write", exam: true, marks: 20, title: "Exam-style: temperature range per country (lab)",
    lang: "python",
    prompt: "From the lab: for each country, using readings from 1900 onward, compute the max and min `AverageTemperature` and their difference `TempIncrease`, sorted by `TempIncrease` from largest to smallest. Use either the DataFrame API or Spark SQL on the `weather` view.",
    data: R`df / weather columns: dt, AverageTemperature, AverageTemperatureUncertainty, City, Country, Latitude, Longitude, Year`,
    expected: R`+-------------+-------+-------+------------+
|      Country|MaxTemp|MinTemp|TempIncrease|
+-------------+-------+-------+------------+
|       Russia| 29.253|-42.704|      71.957|
...`,
    starter: R`from pyspark.sql.functions import col, max, min
`,
    hints: [
      "filter Year >= 1900 → groupBy(\"Country\") → agg(max(...).alias(\"MaxTemp\"), min(...).alias(\"MinTemp\")).",
      "Add the difference with `withColumn(\"TempIncrease\", col(\"MaxTemp\") - col(\"MinTemp\"))`.",
      "`orderBy(col(\"TempIncrease\").desc())`."
    ],
    solution: R`from pyspark.sql.functions import col, max, min

stats = df.filter(df["Year"] >= 1900) \
          .groupBy("Country") \
          .agg(max("AverageTemperature").alias("MaxTemp"),
               min("AverageTemperature").alias("MinTemp")) \
          .withColumn("TempIncrease", col("MaxTemp") - col("MinTemp")) \
          .orderBy(col("TempIncrease").desc())
stats.show(10)

# Spark SQL version:
# spark.sql("""
#     SELECT Country,
#            max(AverageTemperature) AS MaxTemp,
#            min(AverageTemperature) AS MinTemp,
#            max(AverageTemperature) - min(AverageTemperature) AS TempIncrease
#     FROM weather
#     WHERE Year >= 1900
#     GROUP BY Country
#     ORDER BY TempIncrease DESC
# """).show(10)`,
    rubric: [
      { pts: 2, text: "Filters Year >= 1900 before grouping" },
      { pts: 2, text: "Groups by Country" },
      { pts: 3, text: "Computes max and min AverageTemperature with names" },
      { pts: 2, text: "Computes TempIncrease = max − min" },
      { pts: 1, text: "Sorts by TempIncrease descending" }
    ],
    checks: [
      { re: "1900", label: "Filters from 1900" },
      { re: "groupBy\\s*\\(\\s*[\"']Country|group\\s+by\\s+country", label: "Groups by Country", flags: "i" },
      { re: "max\\s*\\(", label: "Computes max" },
      { re: "min\\s*\\(", label: "Computes min" },
      { re: "TempIncrease", label: "Names the difference TempIncrease" },
      { re: "desc", label: "Sorts descending", flags: "i" }
    ],
    explain: "Straight from the lab notebook. It shows the DataFrame habit of aggregating, then deriving a new column with withColumn.",
    src: "Lab: EDA Weather" }
];
})();

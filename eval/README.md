# Small inference evaluation

Run from the repository root with your server-side `JEV_API_KEY` exported:

```sh
make eval
```

Ten synthetic operations, each assessed twice: 20 paid API requests, with the
processor's exact metadata builder, questions, and response validator. No caching,
real spans, or production data are used. Normal tests skip live inference.
Optionally set `JEV_EVAL_MODEL` to an available pinned model ID; the default is
`jev-latest`. The runner does not load `.env` files or print credentials.

`cases.json` contains **provisional, assistant-authored labels**, in order:
diagnostic value, business criticality, keep. Have engineers review these labels
before inspecting model results. Null means insufficient context and is excluded
from agreement; the unknown operation remains visible in the report.

JSON reports go to ignored `eval/results/`. They include probabilities, latency,
failures, dataset/question hashes, and the requested model. Agreement uses a fixed
0.5 threshold, compared with an always-yes baseline. CriticalLow counts positive
business-criticality labels predicted below 0.5. Counts include both repeats;
these are not independent samples. Model disagreements do not fail the command;
API failures do, after saving the report. No billing amount is estimated.

Compare repeat 1/2 for stability and cases with the same group for wording
consistency. These are exploratory comparisons, not statistical proof. This tiny
set does not measure production accuracy, calibration, or safe sampling. The
runner assesses operation metadata directly; it does not evaluate local protection
rules or whole-trace outcomes. Add independently reviewed representative examples
before using the results to justify deployment decisions.

# Deck text (visible text and speaker notes), from the Slides artifact source

## Slide 1 (cover)

Visible text:

- COSC-6315 Distributed Database Systems · Midterm progress · Oct 2026
- Finding new data in a vector database split across servers
- How to spend a fixed network budget when new passages keep arriving
- Panhapiseth Lim
- Department of Computer Science, University of Texas Permian Basin
- Shard data
- Summary
- New data
- Question

Speaker notes:

About 30 seconds

My project asks a practical question about databases that search by meaning. These databases keep their data on many servers, and new data arrives all the time. Keeping the search accurate means sending extra bytes between servers, and the number of bytes is limited.

So if you can only send so many bytes, what should you spend them on? I will show what I built, how I tested it, what I found, and what is left before the conference deadline.

The picture behind me shows the idea. Each cloud of white dots is one server’s data, and the rings are the short summaries the system keeps of each server. Orange dots are new data arriving. The moving teal dot is a question, sent to the two servers whose summaries look closest to it.

## Slide 2 (problem)

Visible text:

- The problem
- Summaries go out of date
- A vector database finds the stored passages that are closest in meaning to a question.
- Ours keeps 100,000 passages on 32 servers, called shards. A coordinator, like a front desk, sends each question to only 2 shards. It chooses them from a short summary of each shard: 16 points that mark the main topics the shard holds.
- New passages keep arriving, but the coordinator uses each shard’s old summary until the shard sends a fresh one. A question can then miss a good new match.
- Animation: one time step, simplified
- 02 / 15

Speaker notes:

About 50 seconds

First, what is a vector database? It stores passages of text as lists of numbers that capture their meaning. When a question comes in, it finds the stored passages whose meaning is closest.

Ours keeps one hundred thousand passages on thirty-two servers, which we call shards. To save work, a coordinator, which works like a front desk, sends each question to only two shards. It decides which two by looking at a short summary of every shard: sixteen points that mark the main topics the shard holds.

Here is the problem, in the animation. New passages land on shard three. But the coordinator still has shard three’s old summary, so it sends the question to shards one and two and misses the new match. Once shard three sends a fresh summary, the question goes to shard three and finds it. Fresh summaries help, but each one means more bytes sent over the network.

## Slide 3 (question)

Visible text:

- The question
- Where should the network bytes go?
- Each method with a budget may send the same number of bytes each time step: the size of 2, 4 or 8 summaries. The default, 4 summaries, is 98,304 bytes.
- Refresh
- A shard sends the coordinator a fresh summary of what it now holds.
- 24,576 bytes per summary
- Extra search
- When the coordinator is unsure, the question also goes to a third shard.
- 1,656 bytes per extra search
- Push
- Shards send copies of their newest passages, and the coordinator checks them for every question.
- 1,544 bytes per passage
- And if we refresh, which shards should go first? We compare five rules, one of them our own.
- Byte sizes as counted by the simulator
- 03 / 15

Speaker notes:

About 45 seconds

So the system has a fixed allowance of network bytes each time step, and every method that has a budget gets exactly the same allowance. That keeps the comparison fair.

There are three ways to spend it, shown in the three small animations. Refresh: a shard sends the coordinator a fresh summary of what it now holds. Extra search: when the coordinator is unsure which shards hold the answer, the question also goes to a third shard. Push: shards send copies of their newest passages, and the coordinator checks those copies for every question.

The second question is about refreshing itself. If we can only refresh a few shards each step, which ones should go first? We compare five rules, and one of them is our own.

## Slide 4 (earlier)

Visible text:

- Earlier work
- What others did, and what we add
- Refreshing under a budget
- A search engine’s page collector can first fetch the new pages that would show up most often in top results [1].
- Out-of-date summaries
- Out-of-date summaries lower search accuracy across many text collections [2]. Ipeirotis et al. plan updates for the summaries most likely to be out of date [3].
- Keeping one search index current
- Systems such as SPFresh and Quake keep one search index accurate as its data changes [4, 5].
- Search spread over many servers
- In Semord, servers work as equals with no central coordinator. They ask extra servers to make up for questions sent to the wrong ones [6].
- What this project adds
- A coordinator whose summaries go out of date, a fixed byte budget each time step, and refresh, extra search and push compared at equal bytes.
- A refresh rule adapted from the crawling work: 64 sample questions estimate how many new best matches a fresh summary would bring back.
- A simulator that counts the bytes of every message and checks every answer against the true best matches, with the plan written down before the main analysis.
- Numbers in brackets match the references on the last slide
- 04 / 15

Speaker notes:

About 45 seconds

None of the parts of this idea is brand new, and the paper credits each one. A search engine’s page collector, called a web crawler, can choose which new pages to fetch first by how often they would show up in top search results. Work on searching many text collections showed that out-of-date summaries lower accuracy, and earlier work planned updates for the summaries most likely to be out of date. A single search index is already kept up to date as its data changes. And in Semord, a recent system with no central coordinator, servers ask extra servers to make up for questions sent to the wrong ones.

What we add is the combination: a coordinator whose summaries go out of date, a fixed byte budget, and a fair comparison of refreshing, extra searches and pushing at the same number of bytes. We also adapt the crawling idea into a refresh rule that uses sixty-four sample questions, and we test everything in a simulator that checks every answer, with the plan written down before the main analysis.

## Slide 5 (simulator)

Visible text:

- What we built
- One simulated time step
- At the start, about half of the passages are split into 32 shards by similarity of meaning. Each shard sets up its own fast search and its 16-point summary. Then 50 time steps follow, each like this:
- 1
- New passages arrive
- About 1,000 per step. A placement rule picks each one’s shard.
- 2
- True answers found
- A slow, exact search over everything finds each test question’s real best matches, so answers can be graded.
- 3
- Summaries prepared
- Each shard that changed computes a fresh summary but does not send it yet.
- 4
- The budget is spent
- On refreshes, extra searches, pushed copies or sample questions. Every message’s bytes are counted.
- 5
- Questions answered
- Each goes to 2 shards, plus any extra search and pushed copies. The best 10 results are kept.
- 6
- Graded against the truth
- The run stops if a budgeted method spends more than its share.
- Everything runs on one computer. Bytes are counted exactly, but network delay, waiting in line and server failures are not modeled.
- How the simulator works
- 05 / 15

Speaker notes:

About 50 seconds

This is the simulator I built, shown as one time step. At the start, about half of the passages are split into thirty-two shards by similarity of meaning, and every shard sets up its own fast search and its summary.

Then each of the fifty time steps goes the same way. New passages arrive, about a thousand per step, and a placement rule decides which shard stores each one. A slow, exact search over everything finds the true best answers, so we can grade every answer. Shards that changed prepare a fresh summary but do not send it yet. Then the method spends its byte budget, and every message is counted with its size. Questions are answered from two shards plus whatever extra the method paid for. Finally every answer is graded against the truth, and the run stops if a method overspends.

One limit: it all runs on one computer. We count bytes exactly, but we do not model network delay, waiting in line, or failures.

## Slide 6 (tools)

Visible text:

- Tools and technology
- What the project is built with
- 1 · Data
- MS MARCO passages and questions, downloaded from Hugging Face.
- 2 · Meaning
- Sentence-Transformers, run with PyTorch, turns each text into 384 numbers.
- 3 · Grouping
- Faiss groups similar passages into shards and summaries, and finds the true best matches.
- 4 · Shard search
- hnswlib gives each shard its own fast search.
- 5 · Simulator
- Our own Python code with NumPy. pandas and PyArrow store the results.
- Code history
- Git keeps every change, and the code is public on GitHub. Eight named save points mark the steps of the plan.
- Testing
- pytest runs 24 automated tests on the simulator and the analysis.
- Settings and reruns
- Each experiment has one settings file. A script restarts only the runs that did not finish.
- Writing
- LaTeX with the IEEE template for both papers. matplotlib draws the charts.
- Hardware: one Windows 10 computer with Python 3.12. Each simulation process used about 6.5 GB of memory.
- Tools, code and version control
- 06 / 15

Speaker notes:

About 40 seconds

These are the tools behind the simulator, in the order the data moves through them. The passages and questions come from MS MARCO, which we download from Hugging Face. A Sentence-Transformers model, run with PyTorch, turns each passage and question into a list of three hundred eighty-four numbers. Faiss groups similar passages into shards and summaries, and it also finds the true best matches we grade against. A library called hnswlib gives each shard its own fast search. The simulator itself is our own Python code, and the results are stored as tables with pandas and PyArrow.

Around the code, Git keeps every change, and the code is public on GitHub, with eight named save points that mark the steps of the plan. pytest runs twenty-four automated tests. Every experiment has its own settings file, and a script restarts only the runs that did not finish. Both papers are written in LaTeX with the IEEE template.

Everything ran on one Windows computer, and each simulation process used about six and a half gigabytes of memory.

## Slide 7 (methods)

Visible text:

- Methods compared
- Twelve methods, one budget
- Classical refresh rules (baselines)
- Rotation: shards take turns.
- Largest shard: the biggest shards go first.
- Most-searched: shards that questions visit most go first.
- Most-changed: each shard sends a tiny 4-byte score for how badly its old summary fits its new passages; the worst go first.
- Our proposed rule
- Sample-query probe: the coordinator sends 64 sample questions and counts how many new best matches each fresh summary would bring back. The highest go first.
- Its messages use 22% of the budget at the default, which leaves room for one fewer summary.
- Other ways to spend the bytes
- Planned extra search: looks at all of a step’s questions first and gives a third shard to the closest calls.
- Live extra search: decides question by question, as they arrive.
- Push: copies of new passages go to the coordinator.
- For comparison, no budget
- Never refresh: keeps the first summaries and spends nothing; the starting point.
- Always fresh: all 32 summaries rebuilt every step.
- Best-shard and all-shard search: how much better choosing shards could get.
- Baselines adapted from Shokouhi et al. [2] and Ipeirotis et al. [3]
- 07 / 15

Speaker notes:

About 50 seconds

We compare twelve methods. Four are classical refresh rules, which serve as our baselines. Rotation lets shards take turns. Largest shard and most-searched come from earlier work on searching many text collections. Most-changed asks each shard for a tiny four-byte score that says how badly its old summary fits its new passages, and refreshes the worst first. We come back to these tiny scores later.

Our own rule is the sample-query probe. The coordinator sends sixty-four sample questions and counts how many new best matches each fresh summary would bring back. That costs bytes too: at the default budget its messages use about a fifth of the allowance, so it can send one fewer summary.

Three methods spend the bytes another way: a planned extra search that looks at all of a step’s questions first, a fair live version that decides question by question, and push. Finally, methods without a budget show the range. Never refreshing is the starting point, and the others show how much better choosing shards could get without a budget.

## Slide 8 (data)

Visible text:

- Data and measures
- What arrives, and how we score it
- Blocks are shards. Shaded blocks receive the orange new passages.
- Data: 100,000 web passages and 1,000 real search questions from MS MARCO, a public data set. A small language model turns each into 384 numbers that capture its meaning.
- Placement: starting center (each passage goes to the shard whose center at the start is closest), one shard (all of a step’s passages on one shard), or eight shards (the closest of eight active shards, so some get more).
- Topics: mixed (random order) or new (held back at the start). Budgets of 2, 4 or 8 summaries; 50, 200 or 800 questions per step. Each setting is repeated five times with different random starts.
- Recent recall, our main score
- Of a question’s 10 true best matches from the last two steps, the share the system found. Overall recall counts all 10.
- When a difference counts
- A difference is clear when our statistics test says chance alone would rarely produce it. One point means one percentage point.
- Data, test settings and scores
- 08 / 15

Speaker notes:

About 55 seconds

The data are one hundred thousand passages from MS MARCO, a public set of web passages and real search questions. A small language model turns each passage into a list of three hundred eighty-four numbers that capture its meaning. We use a thousand test questions, and keep sixty-four other questions apart for the probe.

The animation shows the three placements we test, meaning where a new passage is stored: on the shard whose starting center is closest, all on one shard per time step, or on whichever of eight active shards has the closest topic, so some of the eight get more than others. New passages come either in mixed topic order or from new topics that were held back at the start.

Our main score is recent recall. Take a question’s ten true best matches, keep the ones that arrived in the last two steps, and ask what share the system found. Overall recall is the same, counting all ten. Every setting is repeated five times with different random starts. We call a difference clear only when our statistics test says chance alone would rarely produce it, and we give differences in points, where one point is one percentage point. We wrote the plan and the tests down before the main analysis, and the one follow-up we added after seeing results is clearly labeled as added later.

## Slide 9 (result1)

Visible text:

- Result 1 · starting-center placement
- Here, extra search and push win
- Gain over the best refresh rule, in points
- +2.76
- +2.55
- +1.88
- +1.97
- +1.84
- +2.09
- Mixed topics
- New topics
- Planned extra search
- Live extra search
- Push
- Here each new passage goes to the shard with the nearest starting center, so the old summaries often already send questions to the right shards.
- +2.29
- points from refreshing every summary every step, on new topics. On mixed topics, +0.55, not a clear difference.
- With the same number of bytes, every bar is a clear win over the best refresh rule.
- Recent recall · budget 4 · 200 questions per step · five repeats
- 09 / 15

Chart or image description: Bar chart. Gain over the best refresh rule in recent recall points, at budget 4 and 200 questions per step. Mixed topics: planned extra search 2.76, live extra search 2.55, push 1.88. New topics: 1.97, 1.84 and 2.09.

Speaker notes:

About 45 seconds

First case: each new passage goes to the shard whose starting center is nearest. Here the old summaries mostly keep working, because new passages often land where the old summaries already send questions.

Even refreshing every summary at every step, which ignores the budget, gains only about two and a quarter points on new topics, and on mixed topics the half-point change is not a clear difference.

So at the same number of bytes, it is better to spend them elsewhere. The chart shows the gain over the best refresh rule. Both kinds of extra search and push win by about two to three points, on both kinds of topics, and every one of these differences is clear.

## Slide 10 (result2)

Visible text:

- Result 2 · each step’s new passages go to one shard
- One active shard: the topic decides
- Never refresh
- Refresh (all rules tie)
- Push
- New topics · recent recall (%)
- Mixed topics · recent recall (%)
- 6.0
- 28.6
- 9.2
- 6.0
- 28.6
- 12.6
- 6.0
- 28.6
- 19.0
- 6.3
- 7.9
- 9.3
- 6.3
- 7.9
- 12.2
- 6.3
- 7.9
- 18.1
- Budget 2
- Budget 4
- Budget 8
- Budget 2
- Budget 4
- Budget 8
- Refresh gains 22.6 points over never refreshing and beats push at every budget.
- Refresh gains only 1.7 points and slightly lowers overall recall. Push wins at every budget.
- All five refresh rules tie: each refreshes the one shard that changed
- 10 / 15

Chart or image description: Bar chart, new topics, one active shard. Never refresh 6.0 at budgets 2, 4 and 8. Refresh, where all rules tie, 28.6 at every budget. Push 9.2, 12.6 and 19.0.
Chart or image description: Bar chart, mixed topics, one active shard. Never refresh 6.3 at every budget. Refresh, where all rules tie, 7.9 at every budget. Push 9.3, 12.2 and 18.1.

Speaker notes:

About 45 seconds

Second case: all of a time step’s new passages go to a single shard. Now the old summaries fail, and the kind of topic decides what helps.

On the left, new topics. When the new passages share a new topic, one fresh summary describes them well. Refreshing wins by a lot, about twenty-two and a half points over never refreshing, and it beats push at every budget.

On the right, mixed topics. When the new passages mix all topics, sixteen summary points cannot describe them. Refreshing gains less than two points, it slightly lowers recall on older matches, and push wins at every budget.

One more detail: here all five refresh rules tie, because they all refresh the one shard that changed. A code review caught that before we ran anything, and it is why we added the eight-shard setting on the next slide.

## Slide 11 (result3)

Visible text:

- Result 3 · eight active shards per time step
- Eight active shards: the budget decides
- Recent recall (%), new topics
- 28.9
- 27.4
- 26.8
- 31.3
- 30.3
- 27.8
- 31.6
- 35.2
- 30.0
- Budget 2
- Budget 4
- Budget 8
- Most-changed
- Push
- Probe
- Never refresh, 24.9
- Budget 2
- Refresh leads push by 1.5 points, but only while most-changed’s tiny score messages are not charged to the budget. Charged, they cost one of its two summaries, and the lead disappears.
- Budget 4
- No clear winner: refresh is ahead by 1.0 point, but chance alone could explain a gap that small.
- Budget 8
- Push wins, by 3.6 points.
- Mixed topics: push wins at every budget on new matches. Counting older matches too, live extra search beats push.
- Eight shards take turns being active and share the new passages · five repeats
- 11 / 15

Chart or image description: Bar chart of recent recall in percent on new topics with eight active shards. Most-changed: 28.9, 31.3 and 31.6 at budgets 2, 4 and 8. Push: 27.4, 30.3 and 35.2. Sample-query probe: 26.8, 27.8 and 30.0. A dashed line marks never refresh at 24.9.

Speaker notes:

About 50 seconds

Third case: eight shards share each time step’s new passages. This is the hardest case, and here the size of the budget decides. The dashed line is never refreshing.

On new topics, compare the teal and orange bars. With the smallest budget, refreshing is ahead of push by about one and a half points. But that lead is fragile. The most-changed rule needs each shard to send a tiny four-byte score, and in our main runs those bytes were not charged to the budget. The budget buys whole summaries, so when we charged for those scores, the rule could afford only one summary instead of two, and the lead disappeared. With a budget of four there is no clear winner, and with a budget of eight push wins by about three and a half points.

On mixed topics, push wins at every budget for finding new matches. If you count older matches too, live extra search does even better than push.

## Slide 12 (probe)

Visible text:

- Result 4 · our own rule
- Our probe lost to a simple rule
- Recent recall · eight shards · new topics · budget 4
- 31.35%
- Most-changed
- 27.75%
- Sample-query probe
- Where the 3.60-point gap came from
- 2.66 · picking worse shards
- 0.94 · one fewer summary
- How well each estimate predicts which shards gain most
- 0.63 · count of new passages
- 0.26 · sample-query probe
- Our plan said the probe had to beat the other rules by at least 5 points. Instead it lost by 3.60 points, a clear difference.
- Its own messages use 22% of the budget, so it sends one fewer summary. That accounts for 0.94 points.
- The other 2.66 points remain even when both rules send the same number of summaries, so they come from picking worse shards. Its estimate was zero for every shard in 62 of 250 time steps (5 repeats × 50 steps), so it could not tell the shards apart.
- A plain count of new passages per shard tracked the real benefit better. We did not test that count as a refresh rule.
- Prediction score: 1 = perfect order, 0 = no better than guessing
- 12 / 15

Chart or image description: Stacked bar: of the 3.60-point gap, 2.66 points come from picking worse shards and 0.94 points from sending one fewer summary.
Chart or image description: Two bars of rank agreement with the true benefit: a plain count of new passages per shard, 0.63; the sample-query probe, 0.26.

Speaker notes:

About 55 seconds

Now our own rule. It lost. In the eight-shard setting on new topics, with a budget of four, the simple most-changed rule found about thirty-one percent of new matches and our probe about twenty-eight. Our plan said the probe had to win by at least five points; instead it lost by three point six.

Where did the gap come from? A small part, about one point, is the probe’s own messages: they cost about a fifth of the budget, so it sends one fewer summary. The bigger part, about two and two-thirds points, stays even when both rules send the same number of summaries, so it comes from picking worse shards. In sixty-two of two hundred fifty time steps, the probe’s estimate was zero for every shard, so it could not tell the shards apart.

The bars at the bottom show how well each estimate predicts which shards gain most from a refresh. A plain count of new passages per shard did much better than the probe, although we did not test that count as a refresh rule.

## Slide 13 (checks)

Visible text:

- Checks and road blockers
- What surprised us, and what we fixed
- Surprises in the data
- Trial run. Refresh gained little, because old summaries already pointed to the new passages. We added ways of placing new passages where old summaries fail.
- One-shard tie. Every rule must refresh the one changed shard. Code review caught this before the runs.
- Seeing ahead. Planned extra search sees the whole step. A fair live version showed this was worth at most 0.21 points with starting-center placement, 0.30 with eight shards.
- Uncharged messages. Most-changed’s 128 score bytes per step went uncharged. The budget buys whole summaries, so charging them costs one summary; a labeled follow-up did that.
- Chance check. With one shard and mixed topics, never refreshing found 6.27% of new matches: about random guessing, 2 of 32 shards.
- Road blockers
- Memory. Each simulation process used about 6.5 GB, and 113 runs crashed in the first pass. A script that restarts only unfinished runs, fewer runs at once and watching free memory got every run done; later batches still needed some reruns.
- AI drafting. An early draft typed numbers with no source. Now every result number comes from scripts, and separate reviewers check each claim.
- Readability. The full paper was too dense, so we wrote a plain-English version for beginners.
- Checks, surprises and risks
- 13 / 15

Speaker notes:

About 50 seconds

These are the checks that changed the project along the way. A trial run showed that refreshing barely helped when new passages join the shard with the nearest starting center, so we added placements where old summaries really fail. A code review found that with one active shard every rule must make the same choice, before we wasted any runs. The planned extra search could see a whole step’s questions in advance, so we added a fair live version; seeing ahead was worth very little. We noticed that the most-changed rule’s tiny score messages were free, and a clearly labeled follow-up charged them. And a chance-level check matched simple math: two shards out of thirty-two.

The road blockers were practical. Each simulation process needed several gigabytes of memory, and the first big batch crashed, so I built a script that restarts only the unfinished runs and ran fewer jobs at once; later batches still needed a few reruns. An AI drafting tool once typed numbers with no source, so every result number is now generated by scripts and checked. And the first paper was too dense, so there is now a plain-English version.

## Slide 14 (status)

Visible text:

- Where it stands
- Done, and what is left
- 1,158
- simulation runs, none missing
- 24 / 24
- automated tests pass
- 8 tags
- in the public repository, marking the steps of the plan
- 10 + 8
- pages: full paper and plain-English version
- Now
- Experiments done, both papers written, code public.
- Before 24 Nov
- Final checks, my own full read, and the choice of version to submit.
- 24 Nov
- Send the title and short summary (abstract) to CCGrid 2027.
- 1 Dec
- Submit the paper.
- 2 to 11 Dec
- Course presentation and final code package.
- One risk: the paper is anonymous for review, but the public repository shows my name. Before 24 November I will check whether the conference allows the paper to be public online.
- Paper status and plan to the deadline
- 14 / 15

Speaker notes:

About 40 seconds

Here is where the project stands. All one thousand one hundred fifty-eight simulation runs are finished, with none missing, and all twenty-four automated tests pass. The code is public, with eight named save points for the steps of the plan, so anyone can check that each part of the plan was saved before the runs it covers. There is a full ten-page paper in conference format and an eight-page plain-English version.

What is left is writing and submission, not experiments. Before the abstract deadline on the twenty-fourth of November, I will finish the final checks, read the whole paper myself, and choose which version to submit. The paper is due on the first of December, and the course presentation follows.

One risk: the paper must be anonymous for review, but the public repository shows my name, so I will check whether the conference allows the paper to be public online.

## Slide 15 (refs)

Visible text:

- Thank you · Questions?
- References and AI use
- [1] S. Pandey and C. Olston. Crawl ordering by search impact. WSDM 2008.
- [2] M. Shokouhi, M. Baillie and L. Azzopardi. Updating collection representations for federated search. SIGIR 2007.
- [3] P. G. Ipeirotis, A. Ntoulas, J. Cho and L. Gravano. Modeling and managing content changes in text databases. ICDE 2005.
- [4] Y. Xu et al. SPFresh: Incremental in-place update for billion-scale vector search. SOSP 2023.
- [5] J. Mohoney et al. Quake: Adaptive indexing for vector search. OSDI 2025.
- [6] S. Wang et al. Semord: Learned semantic-preserving placement and low-fanout routing for distributed vector search. arXiv:2609.25514, 2026.
- [7] T. Nguyen et al. MS MARCO: A human generated machine reading comprehension dataset. CoCo workshop at NIPS 2016.
- [8] N. Reimers and I. Gurevych. Sentence-BERT: Sentence embeddings using Siamese BERT-networks. EMNLP-IJCNLP 2019.
- [9] Yu. A. Malkov and D. A. Yashunin. Efficient and robust approximate nearest neighbor search using hierarchical navigable small world graphs. IEEE Transactions on Pattern Analysis and Machine Intelligence, 2020.
- [10] M. Douze et al. The Faiss library. arXiv:2401.08281, 2024.
- AI use
- Gemini (Google) drafted the simulator code, the analysis code and an early draft of the paper.
- Claude (Anthropic) rewrote the plain-English paper, checked each citation against its source, recomputed the reported numbers and built these slides.
- Gemini, Claude and Codex (OpenAI) reviewed drafts. Every result number comes from scripts run on the saved results. I am responsible for the content.
- Scan for the code, the results and both papers.
- github.com/BryanLim0214/vector-search-refresh-budget
- 15 / 15

Chart or image description: QR code linking to the project's GitHub repository

Speaker notes:

About 45 seconds

To finish, where new data lands decides how a vector database should spend a fixed network budget. The simple most-changed rule beat our own probe, and in one setting a few uncharged bytes were enough to create a lead for refresh.

These are the works I mentioned, and both papers give the full references. The numbers in brackets match the numbers on the earlier slides.

About AI use. Gemini drafted the simulator code, the analysis code and an early draft of the paper. Claude rewrote the plain-English paper, checked each citation against its source, recomputed the reported numbers and built these slides. Gemini, Claude and Codex also reviewed drafts. Every result number comes from scripts run on the saved results, and I am responsible for the content.

The code, the results and both papers are on GitHub at the address at the bottom. Thank you. I am happy to take questions.

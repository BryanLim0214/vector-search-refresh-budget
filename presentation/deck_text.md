# Deck text (visible text and speaker notes), from the Slides artifact source

## Slide 1 (cover)

Visible text:

- COSC-6315 Distributed Database Systems · Midterm progress · Oct 2026
- Finding new data in a vector database split across servers
- How to spend a fixed network budget when new passages keep arriving
- Panhapiseth Lim
- Shard data
- Summary
- New data
- Question

Speaker notes:

About 30 seconds

My project asks a practical question about databases that search by meaning. These databases keep their data on many servers, and new data arrives all the time. Keeping the search accurate costs network traffic, and the traffic budget is limited.

So the question is simple. If you can only send so many bytes, what should you spend them on? I will show what I built, how I tested it, what I found, and what is left before the conference deadline.

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

Here is the problem, in the animation. New passages land on shard three. But the coordinator still has shard three’s old summary, so it sends the question to shards one and two and misses the new match. Once shard three sends a fresh summary, the question goes to shard three and finds it. Fresh summaries help, but each one costs network traffic.

## Slide 3 (question)

Visible text:

- The question
- Where should the network bytes go?
- Every budgeted method gets the same allowance each time step: the bytes of 2, 4 or 8 summaries. The default, 4 summaries, is 98,304 bytes.
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
- Web crawlers refresh the stored pages whose update would most improve search (Pandey and Olston).
- Out-of-date summaries
- Old summaries hurt search across many collections (Shokouhi et al.), so Ipeirotis et al. update them when they have likely changed.
- Keeping one vector index current
- Systems such as SPFresh and Quake keep a single vector index accurate as its data changes.
- Search spread over many servers
- Semord asks extra servers when routing goes wrong; its central baseline assumes routing data is always fresh.
- What this project adds
- A coordinator whose summaries go out of date, a fixed byte budget each time step, and refresh, extra search and push compared at equal bytes.
- A refresh rule adapted from the crawling work: 64 sample questions estimate how many new best matches a fresh summary would bring back.
- A simulator that counts the bytes of every message and checks every answer against the true best matches, with the plan written down before the main analysis.
- Full citations in the paper, Section II
- 04 / 15

Speaker notes:

About 45 seconds

None of the parts of this idea is brand new, and the paper credits each one. Web crawlers already refresh pages under a budget, choosing the pages whose update would most improve search. Work on searching many text collections showed that old summaries hurt, and proposed ways to decide when to update them. Single vector indexes are already kept up to date as their data changes. And a recent system spread over many servers, Semord, asks extra servers when its routing goes wrong, but its central baseline assumes the routing data is always fresh.

What we add is the combination: a coordinator whose summaries go out of date, a fixed byte budget, and a fair comparison of refreshing, extra searches and pushing at the same number of bytes. We also adapt the crawling idea into a refresh rule that uses sixty-four sample questions, and we test everything in a simulator that checks every answer, with the plan written down before the main analysis.

This slide covers the related-work part of the paper.

## Slide 5 (simulator)

Visible text:

- What we built
- One simulated time step
- At the start, about half of the passages are split into 32 shards by similarity of meaning. Each shard builds its own fast search structure and its 16-point summary. Then 50 time steps follow, each like this:
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
- Scope: everything runs on one computer. Bytes are counted exactly, but network delay, waiting in line and server failures are not modeled.
- How the simulator works
- 05 / 15

Speaker notes:

About 50 seconds

This is the simulator I built, shown as one time step. At the start, about half of the passages are split into thirty-two shards by similarity of meaning, and every shard builds its own fast search structure and its summary.

Then each of the fifty time steps goes the same way. New passages arrive, about a thousand per step, and a placement rule decides which shard stores each one. A slow, exact search over everything finds the true best answers, so we can grade every answer. Shards that changed prepare a fresh summary but do not send it yet. Then the method spends its byte budget, and every message is counted with its size. Questions are answered from two shards plus whatever extra the method paid for. Finally every answer is graded against the truth, and the run stops if a method overspends.

One honest limit: it all runs on one computer. We count bytes exactly, but we do not model network delay, waiting in line, or failures.

This slide covers the architecture part of the rubric.

## Slide 6 (methods)

Visible text:

- Methods compared
- Twelve methods, one budget
- Classical refresh baselines
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
- Reference points, no budget
- Never refresh: keeps the first summaries and spends nothing; the starting point.
- Always fresh: all 32 summaries rebuilt every step.
- Best-shard and all-shard search: how much better routing could get.
- Baselines adapted from Shokouhi et al. (2007) and Ipeirotis et al. (2005)
- 06 / 15

Speaker notes:

About 50 seconds

We compare twelve methods. Four are classical refresh baselines. Rotation lets shards take turns. Largest shard and most-searched come from earlier work on searching many text collections. Most-changed asks each shard for a tiny four-byte score that says how badly its old summary fits its new passages, and refreshes the worst first. Keep those tiny scores in mind; they matter later.

Our own rule is the sample-query probe. The coordinator sends sixty-four sample questions and counts how many new best matches each fresh summary would bring back. That costs bytes too: at the default budget its messages use about a fifth of the allowance, so it can send one fewer summary.

Three methods spend the bytes another way: a planned extra search that looks at all of a step’s questions first, a fair live version that decides question by question, and push. Finally, reference points show the range. Never refreshing is the starting point, and the others show how much better routing could get without a budget.

This slide covers the classical-baseline part of the rubric.

## Slide 7 (data)

Visible text:

- Data and measures
- What arrives, and how we score it
- Blocks are shards. Shaded blocks receive the orange new passages.
- Data: 100,000 web passages and 1,000 real search questions from MS MARCO, a public data set. A small language model turns each into 384 numbers that capture its meaning.
- Placement: by topic (the shard with the closest topic), one shard (all of a step’s passages on one shard), or eight shards (spread over eight).
- Topics: mixed (random order) or new (held back at the start). Budgets of 2, 4 or 8 summaries; 50, 200 or 800 questions per step; five repeats with different random starts, called seeds.
- Recent recall, our main score
- Of a question’s 10 true best matches from the last two steps, the share the system found. Overall recall counts all 10.
- When a difference counts
- We resample questions and seeds many times. If the 95% range of a difference stays on one side of zero, we call it clear.
- Data, workloads and measures
- 07 / 15

Speaker notes:

About 55 seconds

The data are one hundred thousand passages from MS MARCO, a public set of web passages and real search questions. A small language model turns each passage into a list of three hundred eighty-four numbers that capture its meaning. We use a thousand test questions, and keep sixty-four other questions apart for the probe.

The animation shows the three placements we test, meaning where a new passage is stored: on the shard with the closest topic, all on one shard per time step, or spread over eight shards per time step. New passages come either in mixed topic order or from new topics that were held back at the start.

Our main score is recent recall. Take a question’s ten true best matches, keep the ones that arrived in the last two steps, and ask what share the system found. Overall recall is the same, counting all ten. Every setting is repeated five times with different random starts, called seeds. We resample the questions and seeds many times, and we call a difference clear only when its ninety-five percent range stays on one side of zero. And we wrote the plan and the tests down before the main analysis, so we could not pick the tests after seeing the results.

This slide covers the metrics, data and workload parts of the rubric.

## Slide 8 (finding)

Visible text:

- Main finding
- Where new data lands decides.
- It decides which use of the bytes finds new matches best:
- Placed by topic
- Extra search and push beat every refresh rule.
- One active shard
- Fresh summaries win on new topics; push wins on mixed topics.
- Eight active shards
- Push wins on mixed topics. On new topics, no reliable winner until the largest budget, where push wins.
- Default settings: budget 4, 200 questions per step, unless a budget is named
- 08 / 15

Speaker notes:

About 30 seconds

Here is the main finding in one sentence: where new data lands decides how to spend the network budget.

When new passages go to the shard with the closest topic, extra search and push both beat every refresh rule. When one shard takes all of a step’s new passages, fresh summaries win on new topics and push wins on mixed topics. When eight shards share them, push wins on mixed topics; on new topics there is no reliable winner until the largest budget, where push wins. The next three slides show each case.

## Slide 9 (result1)

Visible text:

- Result 1 · new passages placed by topic
- By topic, extra search and push win
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
- Here each new passage goes to the shard with the closest starting topic, so the old summaries already send questions to the right shards.
- +2.29
- points from refreshing every summary every step, on new topics. On mixed topics, +0.55, not a clear difference.
- At the same bytes, every bar is a clear win over the best refresh rule.
- Recent recall · budget 4 · 200 questions per step · five seeds
- 09 / 15

Chart or image description: Bar chart. Gain over the best refresh rule in recent recall points, at budget 4 and 200 questions per step. Mixed topics: planned extra search 2.76, live extra search 2.55, push 1.88. New topics: 1.97, 1.84 and 2.09.

Speaker notes:

About 45 seconds

First case: each new passage goes to the shard whose starting topic is closest. Here the old summaries keep working, because new passages land exactly where the old summaries already send questions.

Even refreshing every summary at every step, which ignores the budget, gains only about two and a quarter points on new topics, and on mixed topics the half-point change is not a clear difference.

So at the same number of bytes, it is better to spend them elsewhere. The chart shows the gain over the best refresh rule. Both kinds of extra search and push win by about two to three points, on both kinds of topics, and every one of these differences is clear.

## Slide 10 (result2)

Visible text:

- Result 2 · one active shard per time step
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
- Refresh leads push by 1.5 points, but only while most-changed’s tiny score messages are free. Charged, they cost one of its two summaries, and the lead disappears.
- Budget 4
- No clear winner: refresh is ahead by 1.0 point, but the 95% range includes zero.
- Budget 8
- Push wins, by 3.6 points.
- Mixed topics: push wins at every budget on new matches. Counting older matches too, live extra search beats push.
- Eight shards take turns being active and share the new passages · five seeds
- 11 / 15

Chart or image description: Bar chart of recent recall in percent on new topics with eight active shards. Most-changed: 28.9, 31.3 and 31.6 at budgets 2, 4 and 8. Push: 27.4, 30.3 and 35.2. Sample-query probe: 26.8, 27.8 and 30.0. A dashed line marks never refresh at 24.9.

Speaker notes:

About 50 seconds

Third case: eight shards share each time step’s new passages. This is the hardest case, and here the size of the budget decides. The dashed line is never refreshing.

On new topics, compare the teal and orange bars. With the smallest budget, refreshing is ahead of push by about one and a half points. But that lead is fragile. The most-changed rule needs each shard to send a tiny four-byte score, and in our main runs those bytes were free. The budget buys whole summaries, so when we charged for those scores, the rule could afford only one summary instead of two, and the lead disappeared. With a budget of four there is no clear winner, and with a budget of eight push wins by about three and a half points.

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
- How well each signal ranks shards by real benefit
- 0.63 · count of new passages
- 0.26 · sample-query probe
- Our plan said the probe had to beat the other rules by at least 5 points. Instead it lost by 3.60 points (95% range −4.93 to −2.26).
- Its own messages use 22% of the budget, so it sends one fewer summary. That accounts for 0.94 points.
- The other 2.66 points remain even when both rules send the same number of summaries, so they come from picking worse shards. Its estimate was zero for every shard in 62 of 250 time steps (5 seeds × 50 steps), leaving it nothing to rank by.
- A plain count of new passages per shard tracked the real benefit better. We did not test that count as a refresh rule.
- Rank agreement: 1 = same order as the true benefit, 0 = unrelated
- 12 / 15

Chart or image description: Stacked bar: of the 3.60-point gap, 2.66 points come from picking worse shards and 0.94 points from sending one fewer summary.
Chart or image description: Two bars of rank agreement with the true benefit: a plain count of new passages per shard, 0.63; the sample-query probe, 0.26.

Speaker notes:

About 55 seconds

Now our own rule, and I want to be clear: it lost. In the eight-shard setting on new topics, with a budget of four, the simple most-changed rule found about thirty-one percent of new matches and our probe about twenty-eight. Our plan said the probe had to win by at least five points; instead it lost by three point six.

Where did the gap come from? A small part, about one point, is the probe’s own messages: they cost about a fifth of the budget, so it sends one fewer summary. The bigger part, about two and two-thirds points, stays even when both rules send the same number of summaries, so it comes from picking worse shards. In sixty-two of two hundred fifty time steps, the probe’s estimate was zero for every shard, so it had nothing to rank by.

The bars at the bottom show how well each signal ranks shards by their real benefit. A plain count of new passages per shard did much better than the probe, although we did not test that count as a refresh rule.

## Slide 13 (checks)

Visible text:

- Checks and road blockers
- What surprised us, and what we fixed
- Surprises in the data
- Trial run. Refresh gained little, because old summaries already pointed to the new passages. We added placements where they fail.
- One-shard tie. Every rule must refresh the one changed shard. Code review caught this before the runs.
- Seeing ahead. Planned extra search sees the whole step. A fair live version showed this was worth at most 0.21 points by topic, 0.30 with eight shards.
- Free messages. Most-changed’s 128 score bytes per step went uncharged. The budget buys whole summaries, so charging them costs one summary; a labeled follow-up did that.
- Chance check. With one shard and mixed topics, never refreshing found 6.27% of new matches: about random guessing, 2 of 32 shards.
- Road blockers
- Memory. Each run needed about 6.5 GB, and 113 runs crashed in the first pass. A runner that restarts only unfinished runs, fewer runs at once and memory monitoring got every run done; later batches still needed some reruns.
- AI drafting. An early draft typed numbers with no source. Now every result number comes from scripts, and separate reviewers check each claim.
- Readability. The full paper was too dense, so we wrote a plain-English version for beginners.
- Sanity checks, anomalies and risks
- 13 / 15

Speaker notes:

About 50 seconds

These are the checks that changed the project along the way. A trial run showed that refreshing barely helped when new passages join their topic’s shard, so we added placements where old summaries really fail. A code review found that with one active shard every rule must make the same choice, before we wasted any runs. The planned extra search could see a whole step’s questions in advance, so we added a fair live version; seeing ahead was worth very little. We noticed that the most-changed rule’s tiny score messages were free, and a clearly labeled follow-up charged them. And a chance-level check matched simple math: two shards out of thirty-two.

The road blockers were practical. Each run needed several gigabytes of memory, and the first big batch crashed, so I built a runner that restarts only the unfinished runs and ran fewer jobs at once; later batches still needed a few reruns. An AI drafting tool once typed numbers with no source, so every result number is now generated by scripts and checked. And the first paper was too dense, so there is now a plain-English version.

This slide covers the sanity-check and risk parts of the rubric.

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
- Experiments done, both papers drafted, code public.
- Before 24 Nov
- Final checks, my own full read, and the choice of version to submit.
- 24 Nov
- Register the abstract at CCGrid 2027.
- 1 Dec
- Submit the paper.
- 2 to 11 Dec
- Course presentation and final code package.
- One risk: the paper is anonymous for review, but the public repository shows my name. I will check the conference rule on public preprints before 24 November.
- Paper status and plan to the deadline
- 14 / 15

Speaker notes:

About 40 seconds

Here is where the project stands. All one thousand one hundred fifty-eight simulation runs are finished, with none missing, and all twenty-four automated tests pass. The code is public, with eight tags marking the steps of the plan, so anyone can check that each part of the plan was committed before the runs it covers. There is a full ten-page paper in conference format and an eight-page plain-English version.

What is left is writing and submission, not experiments. Before the abstract deadline on the twenty-fourth of November, I will finish the final checks, read the whole paper myself, and choose which version to submit. The paper is due on the first of December, and the course presentation follows.

One risk: the paper must be anonymous for review, but the public repository shows my name, so I will check the conference rule on public preprints.

This slide covers the paper-status and plan parts of the rubric.

## Slide 15 (close)

Visible text:

- Thank you
- Questions?
- Where new data lands decides how to spend a fixed network budget. The simple most-changed rule beat our costlier probe. And every byte counts: a few free bytes can change the winner when the budget buys whole summaries.
- Code, results and both papers
- github.com/BryanLim0214/vector-search-refresh-budget
- Every reported result number can be regenerated from the stored runs.
- Panhapiseth Lim · COSC-6315
- Scan for the code

Chart or image description: QR code linking to the project's GitHub repository

Speaker notes:

About 30 seconds

To sum up. Where new data lands decides how a vector database should spend a fixed network budget: on fresh summaries, extra searches, or pushed copies. The simple most-changed rule beat our more expensive probe. And every byte counts, because a few free bytes can change the winner when the budget buys whole summaries.

The code, the results and both versions of the paper are public on GitHub, at the address and code on screen, and every reported result number can be regenerated from the stored runs. Thank you, and I am happy to take questions.

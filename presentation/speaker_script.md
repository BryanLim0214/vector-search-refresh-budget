# Speaker script: Finding New Data in a Vector Database Split Across Servers: Midterm Progress

One section per slide, in order. Read it aloud as written; times are rough.

## 1. Finding new data in a vector database split across servers

*About 30 seconds*

My project asks a practical question about databases that search by meaning. These databases keep their data on many servers, and new data arrives all the time. Keeping the search accurate costs network traffic, and the traffic budget is limited.

So the question is simple. If you can only send so many bytes, what should you spend them on? I will show what I built, how I tested it, what I found, and what is left before the conference deadline.

The picture behind me shows the idea. Each cloud of white dots is one server’s data, and the rings are the short summaries the system keeps of each server. Orange dots are new data arriving. The moving teal dot is a question, sent to the two servers whose summaries look closest to it.

## 2. Summaries go out of date

*About 50 seconds*

First, what is a vector database? It stores passages of text as lists of numbers that capture their meaning. When a question comes in, it finds the stored passages whose meaning is closest.

Ours keeps one hundred thousand passages on thirty-two servers, which we call shards. To save work, a coordinator, which works like a front desk, sends each question to only two shards. It decides which two by looking at a short summary of every shard: sixteen points that mark the main topics the shard holds.

Here is the problem, in the animation. New passages land on shard three. But the coordinator still has shard three’s old summary, so it sends the question to shards one and two and misses the new match. Once shard three sends a fresh summary, the question goes to shard three and finds it. Fresh summaries help, but each one costs network traffic.

## 3. Where should the network bytes go?

*About 45 seconds*

So the system has a fixed allowance of network bytes each time step, and every method that has a budget gets exactly the same allowance. That keeps the comparison fair.

There are three ways to spend it, shown in the three small animations. Refresh: a shard sends the coordinator a fresh summary of what it now holds. Extra search: when the coordinator is unsure which shards hold the answer, the question also goes to a third shard. Push: shards send copies of their newest passages, and the coordinator checks those copies for every question.

The second question is about refreshing itself. If we can only refresh a few shards each step, which ones should go first? We compare five rules, and one of them is our own.

## 4. What others did, and what we add

*About 45 seconds*

None of the parts of this idea is brand new, and the paper credits each one. Web crawlers already refresh pages under a budget, choosing the pages whose update would most improve search. Work on searching many text collections showed that old summaries hurt, and proposed ways to decide when to update them. Single vector indexes are already kept up to date as their data changes. And a recent system spread over many servers, Semord, asks extra servers when its routing goes wrong, but its central baseline assumes the routing data is always fresh.

What we add is the combination: a coordinator whose summaries go out of date, a fixed byte budget, and a fair comparison of refreshing, extra searches and pushing at the same number of bytes. We also adapt the crawling idea into a refresh rule that uses sixty-four sample questions, and we test everything in a simulator that checks every answer, with the plan written down before the main analysis.

This slide covers the related-work part of the paper.

## 5. One simulated time step

*About 50 seconds*

This is the simulator I built, shown as one time step. At the start, about half of the passages are split into thirty-two shards by similarity of meaning, and every shard builds its own fast search structure and its summary.

Then each of the fifty time steps goes the same way. New passages arrive, about a thousand per step, and a placement rule decides which shard stores each one. A slow, exact search over everything finds the true best answers, so we can grade every answer. Shards that changed prepare a fresh summary but do not send it yet. Then the method spends its byte budget, and every message is counted with its size. Questions are answered from two shards plus whatever extra the method paid for. Finally every answer is graded against the truth, and the run stops if a method overspends.

One honest limit: it all runs on one computer. We count bytes exactly, but we do not model network delay, waiting in line, or failures.

This slide covers the architecture part of the rubric.

## 6. Twelve methods, one budget

*About 50 seconds*

We compare twelve methods. Four are classical refresh baselines. Rotation lets shards take turns. Largest shard and most-searched come from earlier work on searching many text collections. Most-changed asks each shard for a tiny four-byte score that says how badly its old summary fits its new passages, and refreshes the worst first. Keep those tiny scores in mind; they matter later.

Our own rule is the sample-query probe. The coordinator sends sixty-four sample questions and counts how many new best matches each fresh summary would bring back. That costs bytes too: at the default budget its messages use about a fifth of the allowance, so it can send one fewer summary.

Three methods spend the bytes another way: a planned extra search that looks at all of a step’s questions first, a fair live version that decides question by question, and push. Finally, reference points show the range. Never refreshing is the starting point, and the others show how much better routing could get without a budget.

This slide covers the classical-baseline part of the rubric.

## 7. What arrives, and how we score it

*About 55 seconds*

The data are one hundred thousand passages from MS MARCO, a public set of web passages and real search questions. A small language model turns each passage into a list of three hundred eighty-four numbers that capture its meaning. We use a thousand test questions, and keep sixty-four other questions apart for the probe.

The animation shows the three placements we test, meaning where a new passage is stored: on the shard with the closest topic, all on one shard per time step, or spread over eight shards per time step. New passages come either in mixed topic order or from new topics that were held back at the start.

Our main score is recent recall. Take a question’s ten true best matches, keep the ones that arrived in the last two steps, and ask what share the system found. Overall recall is the same, counting all ten. Every setting is repeated five times with different random starts, called seeds. We resample the questions and seeds many times, and we call a difference clear only when its ninety-five percent range stays on one side of zero. And we wrote the plan and the tests down before the main analysis, so we could not pick the tests after seeing the results.

This slide covers the metrics, data and workload parts of the rubric.

## 8. Where new data lands decides.

*About 30 seconds*

Here is the main finding in one sentence: where new data lands decides how to spend the network budget.

When new passages go to the shard with the closest topic, extra search and push both beat every refresh rule. When one shard takes all of a step’s new passages, fresh summaries win on new topics and push wins on mixed topics. When eight shards share them, push wins on mixed topics; on new topics there is no reliable winner until the largest budget, where push wins. The next three slides show each case.

## 9. By topic, extra search and push win

*About 45 seconds*

First case: each new passage goes to the shard whose starting topic is closest. Here the old summaries keep working, because new passages land exactly where the old summaries already send questions.

Even refreshing every summary at every step, which ignores the budget, gains only about two and a quarter points on new topics, and on mixed topics the half-point change is not a clear difference.

So at the same number of bytes, it is better to spend them elsewhere. The chart shows the gain over the best refresh rule. Both kinds of extra search and push win by about two to three points, on both kinds of topics, and every one of these differences is clear.

## 10. One active shard: the topic decides

*About 45 seconds*

Second case: all of a time step’s new passages go to a single shard. Now the old summaries fail, and the kind of topic decides what helps.

On the left, new topics. When the new passages share a new topic, one fresh summary describes them well. Refreshing wins by a lot, about twenty-two and a half points over never refreshing, and it beats push at every budget.

On the right, mixed topics. When the new passages mix all topics, sixteen summary points cannot describe them. Refreshing gains less than two points, it slightly lowers recall on older matches, and push wins at every budget.

One more detail: here all five refresh rules tie, because they all refresh the one shard that changed. A code review caught that before we ran anything, and it is why we added the eight-shard setting on the next slide.

## 11. Eight active shards: the budget decides

*About 50 seconds*

Third case: eight shards share each time step’s new passages. This is the hardest case, and here the size of the budget decides. The dashed line is never refreshing.

On new topics, compare the teal and orange bars. With the smallest budget, refreshing is ahead of push by about one and a half points. But that lead is fragile. The most-changed rule needs each shard to send a tiny four-byte score, and in our main runs those bytes were free. The budget buys whole summaries, so when we charged for those scores, the rule could afford only one summary instead of two, and the lead disappeared. With a budget of four there is no clear winner, and with a budget of eight push wins by about three and a half points.

On mixed topics, push wins at every budget for finding new matches. If you count older matches too, live extra search does even better than push.

## 12. Our probe lost to a simple rule

*About 55 seconds*

Now our own rule, and I want to be clear: it lost. In the eight-shard setting on new topics, with a budget of four, the simple most-changed rule found about thirty-one percent of new matches and our probe about twenty-eight. Our plan said the probe had to win by at least five points; instead it lost by three point six.

Where did the gap come from? A small part, about one point, is the probe’s own messages: they cost about a fifth of the budget, so it sends one fewer summary. The bigger part, about two and two-thirds points, stays even when both rules send the same number of summaries, so it comes from picking worse shards. In sixty-two of two hundred fifty time steps, the probe’s estimate was zero for every shard, so it had nothing to rank by.

The bars at the bottom show how well each signal ranks shards by their real benefit. A plain count of new passages per shard did much better than the probe, although we did not test that count as a refresh rule.

## 13. What surprised us, and what we fixed

*About 50 seconds*

These are the checks that changed the project along the way. A trial run showed that refreshing barely helped when new passages join their topic’s shard, so we added placements where old summaries really fail. A code review found that with one active shard every rule must make the same choice, before we wasted any runs. The planned extra search could see a whole step’s questions in advance, so we added a fair live version; seeing ahead was worth very little. We noticed that the most-changed rule’s tiny score messages were free, and a clearly labeled follow-up charged them. And a chance-level check matched simple math: two shards out of thirty-two.

The road blockers were practical. Each run needed several gigabytes of memory, and the first big batch crashed, so I built a runner that restarts only the unfinished runs and ran fewer jobs at once; later batches still needed a few reruns. An AI drafting tool once typed numbers with no source, so every result number is now generated by scripts and checked. And the first paper was too dense, so there is now a plain-English version.

This slide covers the sanity-check and risk parts of the rubric.

## 14. Done, and what is left

*About 40 seconds*

Here is where the project stands. All one thousand one hundred fifty-eight simulation runs are finished, with none missing, and all twenty-four automated tests pass. The code is public, with eight tags marking the steps of the plan, so anyone can check that each part of the plan was committed before the runs it covers. There is a full ten-page paper in conference format and an eight-page plain-English version.

What is left is writing and submission, not experiments. Before the abstract deadline on the twenty-fourth of November, I will finish the final checks, read the whole paper myself, and choose which version to submit. The paper is due on the first of December, and the course presentation follows.

One risk: the paper must be anonymous for review, but the public repository shows my name, so I will check the conference rule on public preprints.

This slide covers the paper-status and plan parts of the rubric.

## 15. Questions?

*About 30 seconds*

To sum up. Where new data lands decides how a vector database should spend a fixed network budget: on fresh summaries, extra searches, or pushed copies. The simple most-changed rule beat our more expensive probe. And every byte counts, because a few free bytes can change the winner when the budget buys whole summaries.

The code, the results and both versions of the paper are public on GitHub, at the address and code on screen, and every reported result number can be regenerated from the stored runs. Thank you, and I am happy to take questions.

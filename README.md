[![Build Status](https://www.travis-ci.org/cobusc/erlang_priority_queue.png?branch=master)](https://www.travis-ci.org/cobusc/erlang_priority_queue)


Erlang Priority Queue
=====================

A priority queue implementation in Erlang based on Ulf Wiger's comments [here](http://erlang.org/pipermail/erlang-questions/2005-May/015431.html).

The priority queue is based on an `ordered_set` `disc_copy` table in mnesia.

Keys are constructed as `{PriorityLevel::non_neg_integer(), erlang:now()}`, which means that entries will be sorted by priority level first, then by insertion time. This avoids possible starvation of entries within a specific priority level.

Priority level `0` is the highest priority, with higher valued levels having lower priority. If this inverted logic does not suit your taste, simple change the code to use `mnesia:dirty_last/1` instead of `mnesia:dirty_first/1` [here](https://github.com/cobusc/erlang_priority_queue/blob/master/src/priority_queue.erl#L157).

Inserts are O(log N). Retrievals are O(1).

Access is serialized via a `gen_server`.


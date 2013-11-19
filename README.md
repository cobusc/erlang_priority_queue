[![Build Status](https://www.travis-ci.org/cobusc/erlang_priority_queue.png?branch=master)](https://www.travis-ci.org/cobusc/erlang_priority_queue)


Erlang Priority Queue
=====================

A priority queue implementation in Erlang based on Ulf Wiger's comments here: http://erlang.org/pipermail/erlang-questions/2005-May/015431.html

The priority queue is based on an _ordered\_set_ _disc\_copy_ table in mnesia.

Keys are constructed as {Priority::non\_neg\_integer(), erlang:now()}, which means that entries will be sorted by priority first, then by insertion time. This avoids possible starvation of entries withing a specific priority level.

Inserts are O(log N). Retrievals are O(1).

Access is serialized via a gen\_server.


-module(priority_queue_tests).

-include_lib("eunit/include/eunit.hrl").

-define(QUEUE_NAME, pqtest).

setup_test() ->
    mnesia:stop(),
    ?assertEqual([], os:cmd("rm -rf Mnesia*")),
    ?assertEqual(ok, mnesia:create_schema([node()])),
    ?assertEqual(ok, mnesia:start()),
    case mnesia:change_table_copy_type(schema, node(), disc_copies) of
                {atomic, ok} -> ok;
                {aborted,{already_exists,schema,nonode@nohost,disc_copies}} -> ok;
                {aborted,{no_exists,schema}} -> ok
    end,
    case mnesia:change_table_copy_type(?QUEUE_NAME, node(), disc_copies) of
        {atomic, ok} -> ok; 
        {aborted,{no_exists,?QUEUE_NAME}} -> ok
    end,
    FormatString = 
    case mnesia:delete_table(?QUEUE_NAME) of
        {atomic, ok} -> 
            "Deleted existing table '~p'~n";
        {aborted,{no_exists,?QUEUE_NAME}} -> 
            "Table '~p' does not exist. OK.~n"
    end,
    error_logger:info_msg(FormatString, [?QUEUE_NAME]),
    ?assertMatch({ok, _}, priority_queue:start_link(?QUEUE_NAME)).

enqueue_test_() ->
    [
    ?_assertEqual({ok, [{table_name,?QUEUE_NAME},{length,0},{enqueued,0},{dequeued,0}]},
                  priority_queue:info(?QUEUE_NAME)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 9, <<"I1_P9">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 9, <<"I2_P9">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 0, <<"I3_P0">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 8, <<"I4_P8">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 1, <<"I5_P1">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 7, <<"I6_P7">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 2, <<"I7_P2">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 6, <<"I8_P6">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 3, <<"I9_P3">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 5, <<"I10_P5">>)),
    ?_assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 4, <<"I11_P4">>)),
    ?_assertEqual({ok, [{table_name,?QUEUE_NAME},{length,11},{enqueued,11},{dequeued,0}]},
                  priority_queue:info(?QUEUE_NAME))
    ].

length_test() ->
    ?assertEqual({ok, 11}, priority_queue:length(?QUEUE_NAME)).

dequeue_test_() ->
    [
    ?_assertEqual({ok, [{table_name,?QUEUE_NAME},{length,11},{enqueued,11},{dequeued,0}]}, 
                  priority_queue:info(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I3_P0">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I5_P1">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I7_P2">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I9_P3">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I11_P4">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I10_P5">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I8_P6">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I6_P7">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I4_P8">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I1_P9">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, <<"I2_P9">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, [{table_name,?QUEUE_NAME},{length,0},{enqueued,11},{dequeued,11}]}, 
                  priority_queue:info(?QUEUE_NAME)),
    ?_assertEqual(empty, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual(empty, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual(empty, priority_queue:dequeue(?QUEUE_NAME)),
    ?_assertEqual({ok, [{table_name,?QUEUE_NAME},{length,0},{enqueued,11},{dequeued,11}]}, 
                  priority_queue:info(?QUEUE_NAME))
    ].

survival_test() ->
    QueuePid = global:whereis_name(?QUEUE_NAME),
    ?assertEqual(ok, priority_queue:reset_counters(?QUEUE_NAME)),
    ?assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 3, <<"I1_P3">>)),
    ?assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 0, <<"I2_P0">>)),
    ?assertEqual(ok, priority_queue:enqueue(?QUEUE_NAME, 1, <<"I3_P1">>)),
    ?assertEqual({ok, [{table_name,?QUEUE_NAME},{length,3},{enqueued,3},{dequeued,0}]},
                      priority_queue:info(?QUEUE_NAME)),
    process_flag(trap_exit, true),
    exit(QueuePid, testing),
    receive
         {'EXIT', QueuePid, testing} -> ok
    end,
    ?assertMatch({ok, _}, priority_queue:start_link(?QUEUE_NAME)),
    ?assertEqual({ok, [{table_name,?QUEUE_NAME},{length,3},{enqueued,0},{dequeued,0}]},
                       priority_queue:info(?QUEUE_NAME)),
    ?assertEqual({ok, <<"I2_P0">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?assertEqual({ok, <<"I3_P1">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?assertEqual({ok, <<"I1_P3">>}, priority_queue:dequeue(?QUEUE_NAME)),
    ?assertEqual({ok, [{table_name,?QUEUE_NAME},{length,0},{enqueued,0},{dequeued,3}]},
                       priority_queue:info(?QUEUE_NAME)).
    
teardown_test() ->
    ?assertEqual(stopped, mnesia:stop()).


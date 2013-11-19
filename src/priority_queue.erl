-module(priority_queue).

-behaviour(gen_server).

%%
%% Based on Ulf Wiger's comments here: http://erlang.org/pipermail/erlang-questions/2005-May/015431.html
%%
%% The priority queue is based on an ordered_set disc_copy table in mnesia.
%%
%% Keys are constructed as {Priority, erlang:now()} which means that entries will
%% be sorted by priority first, then by insertion time. This avoids possible starvation of
%% entries withing a specific priority level.
%%
%% Inserts are O(log N), retrievals are O(1).
%%
%% Access is serialized via a gen_server.
%%

%% API
-export([start_link/1,
         enqueue/3,
         dequeue/1,
         info/1,
         reset_counters/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record(pq_entry,
{
    key :: {non_neg_integer(), erlang:now()}, % The key will be constructed as {priority, erlang:now()}.
    data :: term()
}).

-record(state, 
{
    table_name :: atom(),
    length = 0 :: non_neg_integer(), % The length is initialised from the table and updated by operations.
    enqueued = 0 :: non_neg_integer(),
    dequeued = 0 :: non_neg_integer()
}).

-define(SERVER_REF(Name), {global, Name}).

%%%===================================================================
%%% API
%%%===================================================================

%%
%% @doc Create a key to use with Mnesia.
%%
%% The key construction is important, as it dictates how the
%% entries in the Mnesia table will be sorted and hence retrieved.
%%
%% Note that erlang:now/0 is guaranteed to always return a unique value.
%%
-spec create_key(Priority::non_neg_integer()) -> 
    {non_neg_integer(), erlang:now()}.

create_key(Priority)
when is_integer(Priority), 0 =< Priority ->
    {Priority, erlang:now()}.


%%
%% @doc Enqueue an element into the priorirty queue
%%
-spec enqueue(QueueName::atom(), Priority::non_neg_integer(), Item::term()) -> ok.

enqueue(QueueName, Priority, Item)
when is_atom(QueueName), is_integer(Priority), 0 =< Priority ->
    Entry = #pq_entry{
        key = create_key(Priority),
        data = Item
    },
    gen_server:call(?SERVER_REF(QueueName), {enqueue, Entry}, infinity).

%%
%% @doc Dequeue an element from the priorirty queue
%%
-spec dequeue(QueueName::atom()) -> empty | {ok, term()} | {error, Reason::any()}.

dequeue(QueueName)
when is_atom(QueueName) ->
    gen_server:call(?SERVER_REF(QueueName), dequeue, infinity).


%%
%% @doc Get some useful information regarding the queue 
%%
-spec info(QueueName::atom()) -> {ok, Info::pt_types:proplist(atom(), any())} | {error, Reason::any()}.

info(QueueName) ->
    {ok, State} = gen_server:call(?SERVER_REF(QueueName), state, infinity),
    Values = tl(tuple_to_list(State)),
    Names = record_info(fields, state),
    {ok,lists:zip(Names, Values)}.

%%
%% @doc Reset the 'enqueued' and 'dequeued' counters to 0
%%
-spec reset_counters(QueueName::atom()) -> ok.

reset_counters(QueueName) ->
    gen_server:call(?SERVER_REF(QueueName), reset, infinity).


%%
%% @doc Start a priority queue server registered using the specified name.
%%
-spec start_link(QueueName::atom()) -> {ok, pid()} | ignore | {error, Error::any()}.

start_link(QueueName)
when is_atom(QueueName) ->
    gen_server:start_link(?SERVER_REF(QueueName), ?MODULE, [QueueName], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%
%% @private
%% @doc Initializes the server
%%

init([QueueName]) ->
    {ok, Length} = initialize(QueueName),
    {ok, #state{table_name=QueueName, length=Length}}.

%%
%% @private
%% @doc Handling call messages
%%

handle_call({enqueue, #pq_entry{}=Entry}, _From, State) ->
    TableName = State#state.table_name,
    case catch mnesia:dirty_write(TableName, Entry) of
        ok -> 
            Length = State#state.length,
            Enqueued = State#state.enqueued,
            NewState = State#state{length=Length+1, enqueued=Enqueued+1},
            {reply, ok, NewState};
        Error ->
            {reply, {error, Error}, State}
    end;

handle_call(dequeue, _From, State) ->
    TableName = State#state.table_name,
    {Reply, ReplyState} =
    case catch mnesia:dirty_first(TableName) of
        '$end_of_table' -> 
            {empty, State};
        {P, _} = Key when is_integer(P) ->
            case catch mnesia:dirty_read(TableName, Key) of
                [#pq_entry{}=Entry] ->
                    case catch mnesia:dirty_delete(TableName, Key) of
                        ok ->
                            Length = State#state.length,
                            Dequeued = State#state.dequeued,
                            NewState = State#state{length=Length-1, dequeued=Dequeued+1},
                            {{ok, Entry#pq_entry.data}, NewState};
                        DirtyDeleteError ->
                            {{error, DirtyDeleteError}, State}
                    end;
                DirtyReadError->
                    {{error, DirtyReadError}, State}
            end;
        DirtyFirstError ->
            {{error, DirtyFirstError}, State} 
    end,
    {reply, Reply, ReplyState};

handle_call(state, _From, State) ->
    {reply, {ok, State}, State};

handle_call(reset, _From, State) ->
    NewState = State#state{enqueued=0, dequeued=0},
    {reply, ok, NewState};

handle_call(_Request, _From, State) ->
    Reply = unknown,
    {reply, Reply, State}.

%%
%% @private
%% @doc Handling cast messages
%%
handle_cast(_Msg, State) ->
    {noreply, State}.

%%
%% @private
%% @doc Handling all non call/cast messages
%%
handle_info(_Info, State) ->
    {noreply, State}.

%%
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
terminate(_Reason, _State) ->
    ok.

%%
%% @private
%% @doc Convert process state when code is changed
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%
%% @doc Initialize an Mnesia table used to persist the priority queue...
%%
%% ...returning the number of entries in the table.
%%
-spec initialize(TableName::atom()) -> {ok, Size::non_neg_integer()}.

initialize(TableName) ->
    TableDefinition = [
        {record_name, pq_entry}, % The table contains priority queue entries..
        {attributes, record_info(fields, pq_entry)}, % ...with attributes as in the record.
        {type, ordered_set},  % The table entries are sorted by key and unique..
        {disc_copies, [node()|nodes()]}, % ...and persisted to disk.
        {storage_properties, [
            {ets, []}, 
            {dets, [{auto_save, 5000}]} 
        ]}
    ],
    ok = 
    case mnesia:create_table(TableName,TableDefinition) of
        {atomic, ok} -> ok;
        {aborted,{already_exists,TableName}} -> ok
    end,
    ok = mnesia:wait_for_tables([TableName], infinity),
    Size = mnesia:table_info(TableName, size),
    {ok, Size}.

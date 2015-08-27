-module(antidote_pb_codec).

-include("riak_pb.hrl").
-include("antidote_pb.hrl").

-export([encode/2,
         decode/2,
         decode_response/1]).

-define(TYPE_COUNTER, counter).
-define(TYPE_SET, set).

encode(start_transaction, {Clock, Properties}) ->
    #apbstarttransaction{timestamp=Clock,
                         properties = encode(txn_properties, Properties)};

encode(txn_properties, _) ->
 %%TODO: Add more property paramaeters
 #apbtxnproperties{};

encode(abort_transaction, TxId) ->
    #apbaborttransaction{transaction_descriptor = TxId};

encode(commit_transaction, TxId) ->
    #apbcommittransaction{transaction_descriptor = TxId};

encode(update_objects, {Updates, TxId}) ->
   EncUpdates = lists:map(fun(Update) ->
                                  encode(update_op, Update) end,
                          Updates),
   #apbupdateobjects{ updates = EncUpdates, transaction_descriptor = TxId};

encode(update_op, {Object={_Key, Type, _Bucket}, {Op,Param}}) ->
    EncObject = encode(bound_object, Object),
    case Type of
        riak_dt_pncounter -> EncUp = encode(counter_update, {Op, Param}),                
                         #apbupdateop{boundobject=EncObject, optype = 1, counterop = EncUp}; 
        riak_dt_orset -> SetUp = encode(set_update, {Op, Param}),
                     #apbupdateop{boundobject=EncObject, optype = 2, setop=SetUp}
    end;

encode(bound_object, {Key, Type, Bucket}) ->
    #apbboundobject{key=Key, type=term_to_binary(Type), bucket=Bucket};

encode(counter_update, {increment, Amount}) ->
    #apbcounterupdate{optype = 1, inc = Amount};

encode(counter_update, {decrement, Amount}) ->
    #apbcounterupdate{optype = 2, dec=Amount};

encode(read_objects, {Objects, TxId}) ->
    BoundObjects = lists:map(fun(Object) ->
                                     encode(bound_object, Object) end,
                             Objects),
    #apbreadobjects{boundobjects = BoundObjects, transaction_descriptor = TxId};

encode(start_transaction_response, {error, _Reason}) ->
    #apbstarttransactionresp{success=false};

encode(start_transaction_response, {ok, TxId}) ->
    #apbstarttransactionresp{success=true, transaction_descriptor=term_to_binary(TxId)};

encode(operation_response, {error, _Reason}) ->
    #apboperationresp{success=false};

encode(operation_response, ok) ->
    #apboperationresp{success=true};

encode(commit_response, {error, _Reason}) ->
    #apbcommitresp{success=false};

encode(commit_response, {ok, CommitTime}) ->
    #apbcommitresp{success=true, commit_time= term_to_binary(CommitTime)};

encode(read_objects_response, {error, _Reason}) ->
    #apbreadobjectsresp{success=false};

encode(read_objects_response, {ok, Results}) ->
    EncResults = lists:map(fun(R) ->
                                   encode(read_object_resp, R) end,
                           Results),
    #apbreadobjectsresp{success=true, objects = EncResults};

encode(read_object_resp, {{_Key, riak_dt_pncounter, _Bucket}, Val}) ->
    #apbreadobjectresp{counter=#apbgetcounterresp{value=Val}};

encode(read_object_resp, {{_Key, riak_dt_gcounter, _Bucket}, Val}) ->
    #apbreadobjectresp{counter=#apbgetcounterresp{value=Val}};

encode(read_object_resp, {{_Key, riak_dt_orset, _Bucket}, Val}) ->
    #apbreadobjectresp{set=#apbgetsetresp{value=term_to_binary(Val)}};

encode(_Other, _) ->
    erlang:error("Incorrect operation/Not yet implemented").

decode(txn_properties, _Properties) ->
    {};
decode(bound_object, #apbboundobject{key = Key, type=Type, bucket=Bucket}) ->
    {Key, binary_to_term(Type), Bucket};
decode(update_object, #apbupdateop{boundobject = Object, optype = OpType, counterop = CounterOp, setop = SetOp}) ->
    {Op, OpParam} = case OpType of 
                 1 ->
                     decode(counter_update, CounterOp);
                 2 ->
                     decode(set_update, SetOp)
    end,
    {decode(bound_object, Object), Op, OpParam};
decode(counter_update, #apbcounterupdate{optype = Op, inc = I, dec = D}) ->
    case Op of
        1 -> {increment, I};
        2 -> {decrement, D}
    end;

decode(_Other, _) ->
    erlang:error("Unknown message").
             
decode_response(#apboperationresp{success = true}) ->
    {opresponse, ok};
decode_response(#apboperationresp{success = false, reason = Reason})->
    {error, Reason};
decode_response(#apbstarttransactionresp{success=true,
                                         transaction_descriptor = TxId}) ->
    {start_transaction, TxId};
decode_response(#apbstarttransactionresp{success=false}) ->
    error;
decode_response(#apbcommitresp{success=true, commit_time = TimeStamp}) ->
    {commit_transaction, TimeStamp};
decode_response(#apbcommitresp{success=false}) ->
    error;
decode_response(#apbreadobjectsresp{success=true, objects = Objects}) ->
    Resps = lists:map(fun(O) ->
                              decode_response(O) end,
                      Objects),
    {read_objects, Resps};
decode_response(#apbreadobjectresp{counter = #apbgetcounterresp{value = Val}}) ->
    Val;
decode_response(#apbreadobjectresp{set = #apbgetsetresp{value = Val}}) ->
    erlang:binary_to_term(Val);
decode_response(Other) ->
    erlang:error("Unexpected message: ~p",[Other]).



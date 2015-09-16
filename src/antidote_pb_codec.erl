%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidote_pb_codec).

-include("riak_pb.hrl").
-include("antidote_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

encode(update_op, {Object={_Key, Type, _Bucket}, Op, Param}) ->
    EncObject = encode(bound_object, Object),
    case Type of
        riak_dt_pncounter -> EncUp = encode(counter_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 1, counterop = EncUp};
        riak_dt_orset -> SetUp = encode(set_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 2, setop=SetUp};
        crdt_pncounter -> EncUp = encode(counter_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 1, counterop = EncUp};
        crdt_orset -> SetUp = encode(set_update, {Op, Param}),
                     #apbupdateop{boundobject=EncObject, optype = 2, setop=SetUp};
        riak_dt_gcounter -> EncUp = encode(counter_update, {Op, Param}),
                         #apbupdateop{boundobject=EncObject, optype = 1, counterop = EncUp}
        
    end;

encode(bound_object, {Key, Type, Bucket}) ->
    #apbboundobject{key=Key, type=encode(type,Type), bucket=Bucket};

encode(type, riak_dt_pncounter) -> 0;
encode(type, riak_dt_gcounter) -> 1;
encode(type, riak_dt_orset) -> 2;
encode(type, crdt_pncounter) -> 3;
encode(type, crdt_orset) -> 4;

encode(counter_update, {increment, Amount}) ->
    #apbcounterupdate{optype = 1, inc = Amount};

encode(counter_update, {decrement, Amount}) ->
    #apbcounterupdate{optype = 2, dec=Amount};

encode(read_objects, {Objects, TxId}) ->
    BoundObjects = lists:map(fun(Object) ->
                                     encode(bound_object, Object) end,
                             Objects),
    #apbreadobjects{boundobjects = BoundObjects, transaction_descriptor = TxId};

encode(start_transaction_response, {error, Reason}) ->
    #apbstarttransactionresp{success=false, errorcode = encode(error_code, Reason)};

encode(start_transaction_response, {ok, TxId}) ->
    #apbstarttransactionresp{success=true, transaction_descriptor=term_to_binary(TxId)};

encode(operation_response, {error, Reason}) ->
    #apboperationresp{success=false, errorcode = encode(error_code, Reason)};

encode(operation_response, ok) ->
    #apboperationresp{success=true};

encode(commit_response, {error, Reason}) ->
    #apbcommitresp{success=false, errorcode = encode(error_code, Reason)};

encode(commit_response, {ok, CommitTime}) ->
    #apbcommitresp{success=true, commit_time= term_to_binary(CommitTime)};

encode(read_objects_response, {error, Reason}) ->
    #apbreadobjectsresp{success=false, errorcode = encode(error_code, Reason)};

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

%% Add new error codes
encode(error_code, unknown) -> 0;
encode(error_code, timeout) -> 1;
encode(error_code, _Other) -> 0;

encode(_Other, _) ->
    erlang:error("Incorrect operation/Not yet implemented").

decode(txn_properties, _Properties) ->
    {};
decode(bound_object, #apbboundobject{key = Key, type=Type, bucket=Bucket}) ->
    {Key, decode(type, Type), Bucket};

decode(type, 0) -> riak_dt_pncounter;
decode(type, 1) -> riak_dt_gcounter;
decode(type, 2) -> riak_dt_orset;
decode(type, 3) -> crdt_pncounter;
decode(type, 4) -> crdt_orset;
decode(error_code, 0) -> unknown;
decode(error_code, 1) -> timeout;

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
decode_response(#apboperationresp{success = false, errorcode = Reason})->
    {error, decode(error_code, Reason)};
decode_response(#apbstarttransactionresp{success=true,
                                         transaction_descriptor = TxId}) ->
    {start_transaction, TxId};
decode_response(#apbstarttransactionresp{success=false, errorcode = Reason}) ->
    {error, decode(error_code, Reason)};
decode_response(#apbcommitresp{success=true, commit_time = TimeStamp}) ->
    {commit_transaction, TimeStamp};
decode_response(#apbcommitresp{success=false, errorcode = Reason}) ->
    {error, decode(error_code, Reason)};
decode_response(#apbreadobjectsresp{success=false, errorcode=Reason}) ->
    {error, decode(error_code, Reason)};
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
